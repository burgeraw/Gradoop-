package gellyStreaming.gradoop.model;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import scala.Int;

import javax.xml.crypto.Data;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Tests {

    public static void testLoadingGraph() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        DataStream<Edge<Long, Long>> edges = env
                .readTextFile("src/main/resources/ml-100k/u.data")
                .map(new MapFunction<String, Edge<Long, Long>>() {
                    @Override
                    public Edge<Long, Long> map(String s) throws Exception {
                        String[] args = s.split("\t");
                        long src = Long.parseLong(args[0]);
                        long trg = Long.parseLong(args[1]) + 1000000;
                        long val = Long.parseLong(args[2]) * 10;
                        return new Edge<>(src, trg, val);
                    }
                });
        GraphStream<Long, NullValue, Long> graph = new SimpleEdgeStream<>(edges, env);
        //graph.getEdges().print();
        //graph.numberOfEdges().print();
        //graph.numberOfVertices().print();
        env.execute();
    }

    public static void testTemporalGraph() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        GradoopIdSet graphId = new GradoopIdSet();

        DataStream<TemporalEdge> edges = env.readTextFile("src/main/resources/ml-100k/u.data")
                .map(new MapFunction<String, TemporalEdge>() {
                    @Override
                    public TemporalEdge map(String s) throws Exception {
                        String[] values = s.split("\t");
                        Map<String, Object> properties = new HashMap<>();
                        properties.put("rating", values[2]);

                        return new TemporalEdge(GradoopId.get(),
                                                "watched",
                                                new GradoopId(0, Integer.parseInt(values[0]), (short)0,0),
                                                new GradoopId(0, Integer.parseInt(values[1]), (short)1,0),
                                                Properties.createFromMap(properties),
                                                graphId,
                                                Long.parseLong(values[3]), // (valid until) starting time
                                                Long.MAX_VALUE             //               ending   time
                        );
                    }
                });

        DataStream<TemporalVertex> vertices = env.readTextFile("src/main/resources/ml-100k/u.data")
                .flatMap(new FlatMapFunction<String, TemporalVertex>() {
                    Map<String, Long> verticesUsers = new HashMap();
                    Map<String, Long> verticesMovies = new HashMap<>();

                    @Override
                    public void flatMap(String s, Collector<TemporalVertex> out) throws Exception {
                        String[] values = s.split("\t");
                        long validFrom = Long.parseLong(values[3]);

                        if (verticesUsers.containsKey(values[0])) {
                            validFrom = Math.min(validFrom, verticesUsers.get(values[0]));
                        }

                        verticesUsers.put(values[0], validFrom);

                        Map<String, Object> properties = new HashMap<>();
                        properties.put("age", (int)((Math.random() * 82) + 18));

                        out.collect(new TemporalVertex(new GradoopId(0,Integer.parseInt(values[0]), (short)0,0),
                                                        "user",
                                                        Properties.createFromMap(properties),
                                                        graphId,
                                                        validFrom,
                                                        Long.MAX_VALUE)
                        );

                        if(verticesMovies.containsKey(values[1])) {
                            validFrom = Math.min(validFrom, verticesUsers.get(values[1]));
                        }
                        properties.clear();
                        properties.put("director", "unknown");
                        properties.put("released", (int)((Math.random()*10)+2010));

                        out.collect(new TemporalVertex(new GradoopId(1, Integer.parseInt(values[1]),(short)0,0),
                                                        "movie",
                                                        Properties.createFromMap(properties),
                                                        graphId,
                                                        validFrom,
                                                        Long.MAX_VALUE)
                        );
                    }
                });

        // Q1
        // TODO: Q1: Intuitively, this does not hold. You are reading the same file that essentially contains edges ONLY. Why???

        // Q2
        // TODO: Q2: Also, if we assume that you read the edge file and a separate vertex file simultaneously, how do you ensure
        // TODO: that the vertices of an edge that you ingested and have given for processing in a streaming fashion, really
        // TODO: exist in the vertex DataStream already as well???. You are just reading the files. Could you elaborate?

        // Therefore...

        // TODO: You should do something cleaner first. The most common format is an EdgeStream. You read the edges and you infer
        // TODO: the vertices implicitly. Implement a TemporalEdgeStream class.

        // Q3
        // TODO: The SimpleEdgeStream class is not good enough already???
        // TODO: You just have make sure you can also add temporal ranges and an ID for each graph (graphID), edge (edgeID) and
        // TODO: vertex (vertexID).

        // Q4
        // TODO You do not maintain state for the graph at all. DataStream is a "flowing context"
        // TODO: Check how/if they store the graph in GRADOOP code and locate the specific classes!
        // TODO: In adjacency list (AL) format? In edge list (EL)? In a compressed form like Compressed Sparsed Row (CSR)
        // TODO: Read this one it is useful: https://arxiv.org/pdf/1912.12740.pdf
        // TODO: Do they use hash tables?
        // TODO: You use DataStream<TemporalVertex> vertices.
        // TODO: But, if you want to access a specific vertex, how are you going to do it efficiently?
        // TODO: Especially if you have a processing window then we should store the part of the graph inside the window at least
        // TODO: And be able to access it fast. Food for thought.

        // TODO: Oh, please write code as neatly as possible for me to read it fast, so that I can help. Similar to how I transformed it. :)

        TemporalGraphStream<GradoopId, String, String> graph = new TemporalGraphStream(env, edges, vertices);
        //graph.printEdges();
        //graph.printVertices();
        //graph.numberOfVertices().print().setParallelism(1);
        //graph.numberOfEdges().printToErr().setParallelism(1);
        //graph.getTempEdges().print();
        env.execute();

    }

    public static void testTemporalGraph2() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        GradoopIdSet graphId = new GradoopIdSet();

        DataStream<TemporalEdge> edges = env.readTextFile("src/main/resources/ml-100k/u.data")
                .map(new MapFunction<String, TemporalEdge>() {
                    @Override
                    public TemporalEdge map(String s) throws Exception {
                        String[] values = s.split("\t");
                        Map<String, Object> properties = new HashMap<>();
                        properties.put("rating", values[2]);

                        return new TemporalEdge(GradoopId.get(),
                                "watched",
                                new GradoopId(0, Integer.parseInt(values[0]), (short)0,0),
                                new GradoopId(0, Integer.parseInt(values[1]), (short)1,0),
                                Properties.createFromMap(properties),
                                graphId,
                                Long.parseLong(values[3]), // (valid until) starting time
                                Long.MAX_VALUE             //               ending   time
                        );
                    }
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<TemporalEdge>() {
                    @Override
                    public long extractAscendingTimestamp(TemporalEdge temporalEdge) {
                        return temporalEdge.getValidFrom();
                    }
                });
        //DataStream<Integer> ratingsPerHour =  edges.timeWindowAll(Time.of(1, TimeUnit.HOURS));
        SimpleTemporalEdgeStream edgestream = new SimpleTemporalEdgeStream(edges, env);
        DataStream<GradoopId> vertices = edgestream.getEdges().flatMap(
                new FlatMapFunction<TemporalEdge, GradoopId>() {
                    @Override
                    public void flatMap(TemporalEdge temporalEdge, Collector<GradoopId> collector) throws Exception {
                        collector.collect(temporalEdge.getSourceId());
                        collector.collect(temporalEdge.getTargetId());
                    }
                });

        //vertices.print();
        final int[] counter = {0};
        DataStream<Integer> numberOfVertices = edgestream.getVertices().map(
                new MapFunction<TemporalVertex, Integer>() {
                    @Override
                    public Integer map(TemporalVertex temporalVertex) throws Exception {
                        return counter[0]++;
                    }
                }
        ).setParallelism(1);
        edgestream.getVertices().print();
        numberOfVertices.print();
        env.execute();
    }

    public static void main(String[] args) throws Exception {
        //testLoadingGraph();
        //testTemporalGraph();
        testTemporalGraph2();
    }
}

/*

public class StreamingGraph {
    private int userID;
    private int itemID;
    private int rating;
    private BigInteger timestamp;

    public static StreamingGraph fromString(String line) {
        if (line == null || line.equals("")) throw new IllegalArgumentException("Invalid input string.");
        String[] elements = line.split("\t");
        StreamingGraph rating =  new StreamingGraph();
        this.userID = Integer.parseInt(elements[0]);
        this.itemID = Integer.parseInt(elements[1]);
        this.rating = Integer.parseInt(elements[2]);
        this.timestamp = new BigInteger(elements[3]);
        return rating;
    }


    public static void main(String[] args) {
        String filePath = "src/main/resources/ml-100k/u.data";
        StreamExecutionEnvironment STREAM_EXECUTION_ENVIRONMENT = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream = STREAM_EXECUTION_ENVIRONMENT.readTextFile(filePath);
        DataStream<StreamingGraph> streamingGraph = dataStream.map(StreamingGraph::fromString);

    }

    //TODO
    public static void testWindowedGraph() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        SimpleEdgeStream<Long, NullValue> edges = new SimpleEdgeStream<>(env.readTextFile("src/main/resources/ml-100k/u.data")
                .map(new MapFunction<String, Edge<Long, Long>>() {
                    @Override
                    public Edge<Long, Long> map(String s) throws Exception {
                        String[] args = s.split("\t");
                        long src = Long.parseLong(args[0]);
                        long trg = Long.parseLong(args[1]) + 1000000;
                        long time = Long.parseLong(args[3]);
                        return new Edge<>(src, trg, time);
                    }
                }), new EdgeValueTimestampExtractor(), env).mapEdges(new RemoveEdgeValue());
        //edges.getEdges().print();
        Time windowTime = Time.of(5, TimeUnit.SECONDS);
        //edges.slice(windowTime, EdgeDirection.ALL).applyOnNeighbors(new GenerateCandidateEdges()).print();
        DataStream<Tuple2<Integer, Long>> triangleCount =
               edges.slice(windowTime, EdgeDirection.ALL)
                        .applyOnNeighbors(new GenerateCandidateEdges())
                        .keyBy(0, 1).timeWindow(windowTime)
                        .apply(new CountTriangles())
                        .timeWindowAll(windowTime).sum(0);
        //System.out.println(triangleCount);
        triangleCount.print();
        env.execute();
    }

        public static final class EdgeValueTimestampExtractor extends AscendingTimestampExtractor<Edge<Long, Long>> {
        @Override
        public long extractAscendingTimestamp(Edge<Long, Long> element) {
            return element.getValue();
        }
    }

    public static final class RemoveEdgeValue implements MapFunction<Edge<Long,Long>, NullValue> {
        @Override
        public NullValue map(Edge<Long, Long> edge) {
            return NullValue.getInstance();
        }
    }

    @SuppressWarnings("serial")
    public static final class GenerateCandidateEdges implements
            EdgesApply<Long, NullValue, Tuple3<Long, Long, Boolean>> {

        @Override
        public void applyOnEdges(Long vertexID,
                                 Iterable<Tuple2<Long, NullValue>> neighbors,
                                 Collector<Tuple3<Long, Long, Boolean>> out) throws Exception {

            Tuple3<Long, Long, Boolean> outT = new Tuple3<>();
            outT.setField(vertexID, 0);
            outT.setField(false, 2); //isCandidate=false

            Set<Long> neighborIdsSet = new HashSet<Long>();
            for (Tuple2<Long, NullValue> t: neighbors) {
                outT.setField(t.f0, 1);
                out.collect(outT);
                neighborIdsSet.add(t.f0);
            }
            Object[] neighborIds = neighborIdsSet.toArray();
            neighborIdsSet.clear();
            outT.setField(true, 2); //isCandidate=true
            for (int i=0; i<neighborIds.length-1; i++) {
                for (int j=i; j<neighborIds.length; j++) {
                    // only emit the candidates
                    // with IDs larger than the vertex ID
                    if (((long)neighborIds[i] > vertexID) && ((long)neighborIds[j] > vertexID)) {
                        outT.setField((long)neighborIds[i], 0);
                        outT.setField((long)neighborIds[j], 1);
                        out.collect(outT);
                    }
                }
            }
        }
    }

    @SuppressWarnings("serial")
    public static final class CountTriangles implements
            WindowFunction<Tuple3<Long, Long, Boolean>, Tuple2<Integer, Long>, Tuple, TimeWindow> {

        @Override
        public void apply(Tuple key, TimeWindow window,
                          Iterable<Tuple3<Long, Long, Boolean>> values,
                          Collector<Tuple2<Integer, Long>> out) throws Exception {
            int candidates = 0;
            int edges = 0;
            for (Tuple3<Long, Long, Boolean> t: values) {
                if (t.f2) { // candidate
                    candidates++;
                }
                else {
                    edges++;
                }
            }
            if (edges > 0) {
                out.collect(new Tuple2<Integer, Long>(candidates, window.maxTimestamp()));
            }
        }
    }

}


*/