package gellyStreaming.gradoop.model;

import gellyStreaming.gradoop.algorithms.TriangleCount;
import gellyStreaming.gradoop.algorithms.TriangleEstimator;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.runtime.dispatcher.SingleJobJobGraphStore;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;

public class SimpleTemporalEdgeStream extends GradoopGraphStream<TemporalGraphHead, TemporalVertex, TemporalEdge> {

    private final StreamExecutionEnvironment context;
    private final DataStream<TemporalEdge> edges;
    private final GradoopIdSet graphIdSet;

    // GraphHead could possibly be used to save global information / state

    public SimpleTemporalEdgeStream(DataStream<TemporalEdge> edges,
                                    StreamExecutionEnvironment context,
                                    GradoopIdSet graphIdSet) {
        this.context = context;
        this.edges = edges;
        this.graphIdSet = graphIdSet;
    }

    @Override
    public StreamExecutionEnvironment getContext() {
        return this.context;
    }

    public GradoopIdSet getGraphIdSet() {
        return this.graphIdSet;
    }

    //TODO What if edges/vertices get removed? Only appropriate for edge addition streams?
    //TODO: Also, is this scalable? MinBy works over 2*E, which might be too much to keep in memory
    /**
     * @return All distinct vertices with a timestamp of their earliest reference as a src or trg of an edge
     */
    @Override
    public DataStream<TemporalVertex> getVertices() {
        return this.edges.flatMap(new VertexFlatMapper())
                .keyBy(0)
                .minBy(1)
                .flatMap(new VertexMapper(this.graphIdSet));
    }

    public static final class VertexFlatMapper implements FlatMapFunction<TemporalEdge, Tuple2<GradoopId, Long>> {
        @Override
        public void flatMap(TemporalEdge temporalEdge, Collector<Tuple2<GradoopId, Long>> collector) {
            collector.collect(Tuple2.of(temporalEdge.getSourceId(), temporalEdge.getValidFrom()));
            collector.collect(Tuple2.of(temporalEdge.getTargetId(), temporalEdge.getValidFrom()));
        }
    }

    public static final class VertexMapper implements FlatMapFunction<Tuple2<GradoopId, Long>, TemporalVertex> {
        private Set<GradoopId> vertices;
        private GradoopIdSet gradoopIds;

        public VertexMapper(GradoopIdSet graphIdSet) {
            this.vertices = new HashSet<>();
            this.gradoopIds = graphIdSet;
        }

        @Override
        public void flatMap(Tuple2<GradoopId, Long> tuple, Collector<TemporalVertex> collector) {
            if (!vertices.contains(tuple.f0)) {
                vertices.add(tuple.f0);
                collector.collect(new TemporalVertex(
                        tuple.f0,
                        null,
                        null,
                        this.gradoopIds,
                        tuple.f1,
                        Long.MAX_VALUE
                ));
            }
        }
    }

    @Override
    public DataStream<TemporalEdge> getEdges() {
        return this.edges;
    }

    //TODO implement if want to use GraphHead for global info / state
    @Override
    public DataStream<TemporalGraphHead> getGraphHead() {
        return null;
    }

    // TODO: implement so that only last gets maintained? And what about if the properties are different?
    // TODO: Also, think about how multiple graphs get combined, like in GRADOOP and a single edge can be part
    // TODO: of multiple (logical) graphs.
    // TODO: Also, does distinct even make sense in a temporal context?
    @Override
    public GradoopGraphStream<TemporalGraphHead, TemporalVertex, TemporalEdge> distinct() {
        DataStream<TemporalEdge> distinctEdges = this.edges
                .keyBy((KeySelector<TemporalEdge, Object>) TemporalEdge::getSourceId)
                .flatMap(
                new FlatMapFunction<TemporalEdge, TemporalEdge>() {
                    Set<GradoopId> neighbours = new HashSet<>();
                    @Override
                    public void flatMap(TemporalEdge temporalEdge, Collector<TemporalEdge> collector) {
                        if(!neighbours.contains(temporalEdge.getTargetId())) {
                            neighbours.add(temporalEdge.getTargetId());
                            collector.collect(temporalEdge);
                        }
                    }
                }
        );
        return new SimpleTemporalEdgeStream(distinctEdges, this.context, new GradoopIdSet());
    }

    @Override
    public DataStream<TemporalVertex> getDegrees() {
        return this.edges
                .flatMap(new DegreeTypeSeparator(true, true))
                .keyBy(0)
                .map(new DegreeMapFunction(true, true));
    }

    @Override
    public DataStream<TemporalVertex> getInDegrees() {
        return this.edges
                .flatMap(new DegreeTypeSeparator(true, false))
                .keyBy(0)
                .map(new DegreeMapFunction(true, false));
    }

    @Override
    public DataStream<TemporalVertex> getOutDegrees() {
        return this.edges
                .flatMap(new DegreeTypeSeparator(false, true))
                .keyBy(0)
                .map(new DegreeMapFunction(false, true));
    }

    private static final class DegreeTypeSeparator
            implements FlatMapFunction<TemporalEdge, Tuple3<GradoopId, Long, Long>> {
        private final boolean collectIn;
        private final boolean collectOut;

        public DegreeTypeSeparator(boolean collectIn, boolean collectOut) {
            this.collectIn = collectIn;
            this.collectOut = collectOut;
        }
        @Override
        public void flatMap(TemporalEdge edge, Collector<Tuple3<GradoopId, Long, Long>> collector) {
            if (collectOut) {
                collector.collect(Tuple3.of(edge.getSourceId(), edge.getValidFrom(), 1L));
            }
            if (collectIn) {
                collector.collect(Tuple3.of(edge.getTargetId(), edge.getValidFrom(), 1L));
            }
        }
    }

    private static final class DegreeMapFunction
            implements MapFunction<Tuple3<GradoopId, Long, Long>, TemporalVertex> {
        private final Map<GradoopId, Tuple2<Long, Long>> degrees;
        private final String typeDegree;
        private final GradoopIdSet graphId;

        public DegreeMapFunction(boolean collectIn, boolean collectOut) {
            String typeDegree1;
            degrees = new HashMap<>();
            typeDegree1 = "";
            if(collectIn && collectOut) {
                typeDegree1 = "degree";
            } else if (collectIn) {
                typeDegree1 = "indegree";
            } else if (collectOut) {
                typeDegree1 = "outdegree";
            }
            this.typeDegree = typeDegree1;
            this.graphId = new GradoopIdSet();
        }

        @Override
        public TemporalVertex map(Tuple3<GradoopId, Long, Long> tuple) {
            GradoopId gradoopId = tuple.f0;
            Long validFrom = tuple.f1;
            Long degree = tuple.f2;
            if (!degrees.containsKey(gradoopId)) {
                degrees.put(gradoopId, Tuple2.of(tuple.f1, 0L));
            }
            Tuple2<Long, Long> values = degrees.get(gradoopId);
            Long newDegree = values.f1 + degree;
            Long newValidFrom = Math.min(values.f0, validFrom);
            degrees.put(gradoopId, Tuple2.of(newValidFrom, newDegree));
            Properties properties = new Properties();
            properties.set(typeDegree, newDegree);
            return new TemporalVertex(
                    gradoopId,
                    null,
                    properties,
                    this.graphId,
                    newValidFrom,
                    Long.MAX_VALUE);
        }

    }

    public static final class EdgeKeySelector implements KeySelector<TemporalEdge, GradoopId> {

        @Override
        public GradoopId getKey(TemporalEdge temporalEdge) throws Exception {
            return temporalEdge.getId();
        }
    }

    // TODO: works when parallelism set to 1, figure out how to make it work in parallel
    // Tried KeyBy but no success
    // Maybe test if counter++, or Hashset is faster, if parallelism(1) needs to be used anyway
    @Override
    public DataStream<Long> numberOfEdges() {
        return this.edges
                //.keyBy(new EdgeKeySelector())
                .map(new EdgeMapper())
                .setParallelism(1);
    }

    public static final class EdgeMapper implements MapFunction<TemporalEdge, Long> {
        Long counter;
        public EdgeMapper() {
            this.counter = 0L;
        }

        @Override
        public Long map(TemporalEdge temporalEdge) {
            counter++;
            return counter;
        }
    }

    // TODO: works when parallelism set to 1, figure out how to make it work in parallel
    // TODO: Tried keyBy, but no success
    // TODO: broadcast works but is it scalable? net.time faster than with parallel(1), but seems double work..
    @Override
    public DataStream<Long> numberOfVertices() {
        return this.edges
                .flatMap((FlatMapFunction<TemporalEdge, Tuple1<GradoopId>>) (temporalEdge, collector) -> {
                    collector.collect(Tuple1.of(temporalEdge.getTargetId()));
                    collector.collect(Tuple1.of(temporalEdge.getSourceId()));
                })
                //.keyBy(0)
                //.broadcast()
                .flatMap(new FlatMapFunction<Tuple1<GradoopId>, Long>() {
                    HashSet<GradoopId> vertices = new HashSet<>();
                    @Override
                    public void flatMap(Tuple1<GradoopId> gradoopId, Collector<Long> collector) {
                        if (!vertices.contains(gradoopId.f0)) {
                            vertices.add(gradoopId.f0);
                            collector.collect((long) vertices.size());
                        }
                    }
                })
                .setParallelism(1)
                ;
    }

    private static final class ReverseEdgeMapper implements MapFunction<TemporalEdge, TemporalEdge> {
        @Override
        public TemporalEdge map(TemporalEdge temporalEdge) {
            return reverseEdge(temporalEdge);
        }
    }

    public static TemporalEdge reverseEdge(TemporalEdge temporalEdge) {
            String label = temporalEdge.getLabel();
            GradoopId newSrc = temporalEdge.getTargetId();
            GradoopId newTrg = temporalEdge.getSourceId();
            Properties properties = temporalEdge.getProperties();
            GradoopIdSet graphIds = temporalEdge.getGraphIds();
            Long validFrom = temporalEdge.getValidFrom();
            Long validTo = temporalEdge.getValidTo();
            return new TemporalEdge(
                    GradoopId.get(),
                    label,
                    newSrc,
                    newTrg,
                    properties,
                    graphIds,
                    validFrom,
                    validTo
            );
    }

    @Override
    public SimpleTemporalEdgeStream undirected() {
        DataStream<TemporalEdge> undirectedEdges = this.edges.flatMap(new FlatMapFunction<TemporalEdge, TemporalEdge>() {
            @Override
            public void flatMap(TemporalEdge temporalEdge, Collector<TemporalEdge> collector) throws Exception {
                collector.collect(temporalEdge);
                collector.collect(reverseEdge(temporalEdge));
            }
        });
        return new SimpleTemporalEdgeStream(undirectedEdges, this.context, new GradoopIdSet());
    }


    @Override
    public GradoopGraphStream<TemporalGraphHead, TemporalVertex, TemporalEdge> reverse() {
        return new SimpleTemporalEdgeStream(this.edges.map(new ReverseEdgeMapper()), this.context, new GradoopIdSet());
    }

    @Override
    public GradoopGraphStream<TemporalGraphHead, TemporalVertex, TemporalEdge> filterEdges(FilterFunction<TemporalEdge> filter) {
        DataStream<TemporalEdge> remainingEdges = this.edges.filter(filter);
        return new SimpleTemporalEdgeStream(remainingEdges, this.context, new GradoopIdSet());
    }

    @Override
    public GradoopGraphStream<TemporalGraphHead, TemporalVertex, TemporalEdge> filterVertices(FilterFunction<TemporalVertex> filter) {
        DataStream<TemporalEdge> remainingEdges = this.edges
                .filter(new VertexFilter(filter));
        return new SimpleTemporalEdgeStream(remainingEdges, this.context, new GradoopIdSet());
    }

    private static final class VertexFilter implements FilterFunction<TemporalEdge> {
        private FilterFunction<TemporalVertex> vertexFilter;

        private VertexFilter(FilterFunction<TemporalVertex> vertexFilter) {
            this.vertexFilter = vertexFilter;
        }

        @Override
        public boolean filter(TemporalEdge temporalEdge) throws Exception {
            boolean src = vertexFilter.filter(
                    new TemporalVertex(
                            temporalEdge.getSourceId(),
                            null,
                            null,
                            temporalEdge.getGraphIds(),
                            temporalEdge.getValidFrom(),
                            temporalEdge.getValidTo()));
            boolean trg = vertexFilter.filter(
                    new TemporalVertex(
                            temporalEdge.getTargetId(),
                            null,
                            null,
                            temporalEdge.getGraphIds(),
                            temporalEdge.getValidFrom(),
                            temporalEdge.getValidTo()));
            return src && trg;
        }
    }

    //TODO: rewrite to return any type?
    @Override
    public GradoopGraphStream<TemporalGraphHead, TemporalVertex, TemporalEdge> mapEdges(
            MapFunction<TemporalEdge, TemporalEdge> mapper) {
        return new SimpleTemporalEdgeStream(this.edges.map(mapper), this.context, new GradoopIdSet());
    }

    public static final class NeighborDoubleKeySelector implements KeySelector<TemporalEdge, Tuple2<GradoopId, GradoopId>> {
        private final String direction;

        NeighborDoubleKeySelector(String direction) {
            this.direction = direction;
        }

        @Override
        public Tuple2<GradoopId, GradoopId> getKey(TemporalEdge temporalEdge) throws Exception {
            if(direction.equals("src")) {
                return Tuple2.of(temporalEdge.getSourceId(), temporalEdge.getTargetId());
            } else {
                return Tuple2.of(temporalEdge.getTargetId(), temporalEdge.getSourceId());
            }
        }
    }
    public static final class NeighborKeySelector implements KeySelector<TemporalEdge, GradoopId> {
        private final String direction;

        NeighborKeySelector(String direction) {
            this.direction = direction;
        }

        @Override
        public GradoopId getKey(TemporalEdge temporalEdge) {
            if(direction.equals("src")) {
                return temporalEdge.getSourceId();
            } else {
                return temporalEdge.getTargetId();
            }
        }
    }


/*
keyed on source or target vertex --> good for adjacency list
 */
    public GradoopSnapshotStream slice(Time size, Time slide, EdgeDirection direction, String strategy)
            throws IllegalArgumentException {

            switch (direction) {
                case IN:
                    return new GradoopSnapshotStream(
                            getEdges()
                                    .keyBy(new NeighborKeySelector("src"))
                                    .window(SlidingEventTimeWindows.of(size, slide))
                            , strategy);

                case OUT:
                    return new GradoopSnapshotStream(
                            getEdges()
                                    .keyBy(new NeighborKeySelector("trg"))
                                    .window(SlidingEventTimeWindows.of(size, slide))
                            , strategy);
                case ALL:
                    return new GradoopSnapshotStream(
                            this.undirected()
                                    .getEdges()
                                    .keyBy(new NeighborKeySelector("src"))
                                    .window(SlidingEventTimeWindows.of(size, slide))
                            , strategy);
                default:
                    throw new IllegalArgumentException("Illegal edge direction");
            }
    }

    static class GraphIdSelector implements KeySelector<TemporalEdge, GradoopIdSet> {
        @Override
        public GradoopIdSet getKey(TemporalEdge temporalEdge) {
            return temporalEdge.getGraphIds();
        }
    }

    static class getPartitionId implements KeySelector<TemporalEdge, Integer> {
        @Override
        public Integer getKey(TemporalEdge temporalEdge) throws Exception {
            return temporalEdge.getPropertyValue("partitionID").getInt();
        }
    }

    private static int makeInt(byte b3, byte b2, byte b1, byte b0) {
        return b3 << 24 | (b2 & 255) << 16 | (b1 & 255) << 8 | b0 & 255;
    }

    public DataStream<Double> getGlobalTriangles(int k, double alpha) {
        int randomSeed = new Random().nextInt();
        return this.edges
                .map(new MapFunction<TemporalEdge, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(TemporalEdge edge) throws Exception {
                        //byte[] bytes = edge.getSourceId().toByteArray();
                        //int src = makeInt(bytes[3], bytes[2],bytes[1],bytes[0]);
                        //byte[] bytes1 = edge.getTargetId().toByteArray();
                        //int trg = makeInt(bytes1[3], bytes1[2],bytes1[1],bytes1[0]);
                        int src = Integer.parseUnsignedInt(edge.getSourceId().toString().replace("0",""), 16);
                        int trg = Integer.parseUnsignedInt(edge.getTargetId().toString().replace("0",""), 16);
                        return Tuple2.of(src, trg);
                    }
                })
                .keyBy(0,1)
                .process(new GlobalTriangle(k, alpha));

    }

    public DataStreamSink<TemporalEdge> print() {
        return this.edges.print();
    }

    private class GlobalTriangle extends ProcessFunction<Tuple2<Integer, Integer>, Double> {

        TriangleEstimator TE;

        GlobalTriangle(int k, double alpha) {
            TE = new TriangleEstimator(k, alpha, new Random().nextInt());
        }
        @Override
        public void processElement(Tuple2<Integer, Integer> integerIntegerTuple2, Context context, Collector<Double> collector) throws Exception {
            TE.processElement(integerIntegerTuple2);
            collector.collect(TE.getGlobalTriangle());
        }
    }

    public TriangleCount TriangleCount(int k, double alpha) {
        return new TriangleCount(k, alpha, this.edges
                .map(new MapFunction<TemporalEdge, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(TemporalEdge edge) throws Exception {
                        //byte[] bytes = edge.getSourceId().toByteArray();
                        //int src = makeInt(bytes[3], bytes[2],bytes[1],bytes[0]);
                        //byte[] bytes1 = edge.getTargetId().toByteArray();
                        //int trg = makeInt(bytes1[3], bytes1[2],bytes1[1],bytes1[0]);
                        int src = Integer.parseUnsignedInt(edge.getSourceId().toString().replace("0",""), 16);
                        int trg = Integer.parseUnsignedInt(edge.getTargetId().toString().replace("0",""), 16);
                        return Tuple2.of(src, trg);
                    }
                })
                .keyBy(0,1));
    }


    public GraphState buildState(String strategy) {
        return new GraphState(
                this.edges.keyBy(new getPartitionId()),
                strategy);
    }

//USING
    public GraphState buildState(QueryState QS,
                                 String strategy,
                                 org.apache.flink.streaming.api.windowing.time.Time windowsize,
                                 org.apache.flink.streaming.api.windowing.time.Time slide,
                                Integer numPartitions) throws InterruptedException, IOException {
        return new GraphState(QS,
                this.edges.keyBy(new getPartitionId()),
                strategy, windowsize, slide, numPartitions);
    }

/*
//Currently using:
    public GraphState buildState(StreamGraph sg, StreamExecutionEnvironment env, String strategy,
                                 org.apache.flink.streaming.api.windowing.time.Time windowsize,
                                 org.apache.flink.streaming.api.windowing.time.Time slide,
                                 Integer numPartitions) throws UnknownHostException, InterruptedException {
        return new GraphState(sg,
                env,
                this.edges.keyBy(new getPartitionId()),
                strategy,
                windowsize,
                slide,
                numPartitions);
    }
 */

    public GraphState buildState(QueryState QS,
                                 String strategy,
                                 Long windowSize,
                                 Long slide,
                                 Integer numPartitions) throws InterruptedException {
        return new GraphState(QS,
                this.edges.keyBy(new getPartitionId()),
                strategy,
                windowSize,
                slide,
                numPartitions);
    }

    public GraphState buildState(QueryState QS,
                                 String strategy,
                                 Integer numPartitions) throws InterruptedException {
        return new GraphState(QS,
                this.edges.keyBy(new getPartitionId()),
                strategy,
                numPartitions);
    }


    public GraphState buildState(String strategy, Long windowsize, Long slide)  {
        return new GraphState(
                this.edges.keyBy(new getPartitionId()),
                strategy, windowsize, slide);
    }

}
