package gellyStreaming.gradoop.model;

import gellyStreaming.gradoop.algorithms.Algorithm;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SimpleTemporalEdgeStream extends GradoopGraphStream<TemporalGraphHead, TemporalVertex, TemporalEdge> {

    private final StreamExecutionEnvironment context;
    private final DataStream<TemporalEdge> edges;
    private final GradoopIdSet graphIdSet;

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
        private final Set<GradoopId> vertices;
        private final GradoopIdSet gradoopIds;

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

    // Could be used to keep global information / graph sketches.
    @Override
    public DataStream<TemporalGraphHead> getGraphHead() {
        return null;
    }

    @Override
    public SimpleTemporalEdgeStream distinct() {
        DataStream<TemporalEdge> distinctEdges = this.edges
                .keyBy((KeySelector<TemporalEdge, Object>) TemporalEdge::getSourceId)
                .flatMap(
                new FlatMapFunction<TemporalEdge, TemporalEdge>() {
                    final Set<GradoopId> neighbours = new HashSet<>();
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

    @Override
    public DataStream<Long> numberOfEdges() {
        return this.edges
                .map(new MapFunction<TemporalEdge, Long>() {
                    final AtomicLong counter = new AtomicLong(0);
                    @Override
                    public Long map(TemporalEdge edge) throws Exception {
                        return counter.incrementAndGet();
                    }
                });
    }


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
        //SimpleTemporalEdgeStream distinct = new SimpleTemporalEdgeStream(undirectedEdges, this.context, new GradoopIdSet()).distinct();
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


    public DataStreamSink<TemporalEdge> print() {
        return this.edges.print();
    }


    public GraphState buildState(QueryState QS,
                                 String strategy,
                                 Long windowSize,
                                 Long slide,
                                 Integer numPartitions,
                                 Boolean lazyPurging,
                                 int batchSize,
                                 Algorithm algorithm) {
        return new GraphState(QS,
                this.edges.keyBy(new getPartitionId()),
                strategy,
                windowSize,
                slide,
                numPartitions,
                lazyPurging,
                batchSize,
                algorithm);
    }


}
