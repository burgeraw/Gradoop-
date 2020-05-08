package gellyStreaming.gradoop.model;

import gellyStreaming.gradoop.oldModel.GraphStream;
import gellyStreaming.gradoop.oldModel.SimpleEdgeStream;
import gellyStreaming.gradoop.partitioner.CustomKeySelector;
import gellyStreaming.gradoop.partitioner.DBHPartitioner;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.util.concurrent.TimeUnit.SECONDS;

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

    public static void testGradoopSnapshotStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        GradoopIdSet graphId = new GradoopIdSet();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
/*
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
*/
        //edgestream.numberOfVertices().print();
        DataStream<TemporalEdge> edges2 = getSampleEdgeStream(env);
        SimpleTemporalEdgeStream edgestream = new SimpleTemporalEdgeStream(edges2, env, null);
        //GradoopSnapshotStream snapshotStream = edgestream.slice(Time.of(4, SECONDS), Time.of(2, SECONDS), EdgeDirection.IN, "EL");
        //GradoopSnapshotStream snapshotStream = edgestream.slice(Time.of(4, SECONDS), Time.of(4, SECONDS), EdgeDirection.ALL, "AL");
        GradoopSnapshotStream snapshotStream1 = edgestream.slice2(Time.of(4, SECONDS), Time.of(4, SECONDS), EdgeDirection.IN);
        JobExecutionResult job = env.execute();
        System.out.println(job.getNetRuntime());
    }

    public static void testPartitioner() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int numberOfPartitions = 8;
        env.setParallelism(numberOfPartitions);
        //nv.setMaxParallelism(4);
        DataStream<Edge<Long, String>> edges = getMovieEdges(env);

        CustomKeySelector<Long, String> keySelector1 = new CustomKeySelector<>(0);

        Partitioner<Long> partitioner = new DBHPartitioner<>(keySelector1, numberOfPartitions);
        KeySelector<Edge<Long, String>, Integer> keySelector2 = new KeySelector<Edge<Long, String>, Integer>() {
            @Override
            public Integer getKey(Edge<Long, String> edge) throws Exception {
                return partitioner.partition(edge.f0, numberOfPartitions);
            }};

        KeySelector<Tuple2<Edge<Long, String>,Integer>, Integer> keySelector3 = new KeySelector<Tuple2<Edge<Long, String>, Integer>, Integer>() {
            @Override
            public Integer getKey(Tuple2<Edge<Long, String>, Integer> edgeIntegerTuple2) throws Exception {
                return edgeIntegerTuple2.f1;
            }
        };

        // Original way as used by Gelly-streaming, we require a keyby to use state tho.
        // Gives correct results, also in thread
        DataStream<Edge<Long, String>> partitionedEdges = edges
                .partitionCustom(new DBHPartitioner<>(
                        new CustomKeySelector<Long, String>(0), numberOfPartitions),
                        new CustomKeySelector<Long, String>(0))
                ;
        // Correct results, using map to save partition
        DataStream<Tuple2<Edge<Long,String>,Integer>> partitionedEdges2 =
                edges.map(
                        new MapFunction<Edge<Long, String>, Tuple2<Edge<Long, String>, Integer>>() {
                            @Override
                            public Tuple2<Edge<Long, String>, Integer> map(Edge<Long, String> edge) throws Exception {
                                Long keyEdge = keySelector1.getKey(edge);
                                int machineId = partitioner.partition(keyEdge, numberOfPartitions);
                                return Tuple2.of(edge, machineId);
                            }
                        });

        // Partitioner suddenly loses ability to get Target id = always 0 --> because keySelector1 needs to be called first.
        KeyedStream<Edge<Long, String>, Integer> keyedStream =
                DataStreamUtils.reinterpretAsKeyedStream(edges, keySelector2);
        KeyedStream<Edge<Long, String>, Integer> keyedStream2 =
                DataStreamUtils.reinterpretAsKeyedStream(partitionedEdges, keySelector2);
        // Problem of keygroups. 4 keys outputted, but put into 2 partitions. Also no trg.
        KeyedStream<Edge<Long, String>, Integer> keyedStream3 = partitionedEdges.keyBy(new CustomKeySelector(0)).keyBy(keySelector2);
        // Threads dont get all values with same key
        KeyedStream<Tuple2<Edge<Long, String>, Integer>, Integer> keyedStream4 =
                DataStreamUtils.reinterpretAsKeyedStream(partitionedEdges2, keySelector3);
        // Puts the four keys into 2 threads.
        KeyedStream<Tuple2<Edge<Long, String>, Integer>, Integer> keyedStream5 =
                partitionedEdges2.keyBy(keySelector3);


        //partitionedEdges2.print();
        //partitionedEdges2.writeAsCsv("out", FileSystem.WriteMode.OVERWRITE);
        //keyedStream5.print();
        //keyedStream4.writeAsCsv("out2", FileSystem.WriteMode.OVERWRITE);
        keyedStream5.writeAsCsv("out", FileSystem.WriteMode.OVERWRITE);
        JobExecutionResult result = env.execute();
        System.out.println("Net runtime is: "+result.getNetRuntime());

    }


    public static void main(String[] args) throws Exception {
        //testLoadingGraph();
        //testGradoopSnapshotStream();
        testPartitioner();
    }

    static DataStream<TemporalEdge> getSampleEdgeStream(StreamExecutionEnvironment env) {
        GradoopIdSet graphId = new GradoopIdSet();
        return env.fromElements(
                new TemporalEdge(
                        new GradoopId(0, 1, (short)1, 0),
                        null,
                        new GradoopId(0, 1, (short)0, 0),
                        new GradoopId(0, 2, (short)0, 0),
                        null,
                        graphId,
                        800000000L,
                        Long.MAX_VALUE),
                new TemporalEdge(
                        new GradoopId(0, 2, (short)1, 0),
                        null,
                        new GradoopId(0, 1, (short)0, 0),
                        new GradoopId(0, 3, (short)0, 0),
                        null,
                        graphId,
                        800000000L,
                        Long.MAX_VALUE),
                new TemporalEdge(
                        new GradoopId(0, 3, (short)1, 0),
                        null,
                        new GradoopId(0, 2, (short)0, 0),
                        new GradoopId(0, 3, (short)0, 0),
                        null,
                        graphId,
                        800000000L,
                        Long.MAX_VALUE),
                new TemporalEdge(
                        new GradoopId(0, 4, (short)1, 0),
                        null,
                        new GradoopId(0, 1, (short)0, 0),
                        new GradoopId(0, 4, (short)0, 0),
                        null,
                        graphId,
                        800001000L,
                        Long.MAX_VALUE),
                new TemporalEdge(
                        new GradoopId(0, 5, (short)1, 0),
                        null,
                        new GradoopId(0, 1, (short)0, 0),
                        new GradoopId(0, 5, (short)0, 0),
                        null,
                        graphId,
                        800001000L,
                        Long.MAX_VALUE),
                new TemporalEdge(
                        new GradoopId(0, 6, (short)1, 0),
                        null,
                        new GradoopId(0, 4, (short)0, 0),
                        new GradoopId(0, 5, (short)0, 0),
                        null,
                        graphId,
                        800001000L,
                        Long.MAX_VALUE),
                new TemporalEdge(
                        new GradoopId(0, 7, (short)1, 0),
                        null,
                        new GradoopId(0, 1, (short)0, 0),
                        new GradoopId(0, 6, (short)0, 0),
                        null,
                        graphId,
                        800002000L,
                        Long.MAX_VALUE),
                new TemporalEdge(
                        new GradoopId(0, 8, (short)1, 0),
                        null,
                        new GradoopId(0, 1, (short)0, 0),
                        new GradoopId(0, 7, (short)0, 0),
                        null,
                        graphId,
                        800002000L,
                        Long.MAX_VALUE),
                new TemporalEdge(
                        new GradoopId(0, 9, (short)1, 0),
                        null,
                        new GradoopId(0, 2, (short)0, 0),
                        new GradoopId(0, 7, (short)0, 0),
                        null,
                        graphId,
                        800002000L,
                        Long.MAX_VALUE),
                new TemporalEdge(
                        new GradoopId(0, 10, (short)1, 0),
                        null,
                        new GradoopId(0, 7, (short)0, 0),
                        new GradoopId(0, 8, (short)0, 0),
                        null,
                        graphId,
                        800003000L,
                        Long.MAX_VALUE),
                new TemporalEdge(
                        new GradoopId(0, 11, (short)1, 0),
                        null,
                        new GradoopId(0, 8, (short)0, 0),
                        new GradoopId(0, 1, (short)0, 0),
                        null,
                        graphId,
                        800003000L,
                        Long.MAX_VALUE),
                new TemporalEdge(
                        new GradoopId(0, 12, (short)1, 0),
                        null,
                        new GradoopId(0, 6, (short)0, 0),
                        new GradoopId(0, 5, (short)0, 0),
                        null,
                        graphId,
                        800003000L,
                        Long.MAX_VALUE),
                new TemporalEdge(
                        new GradoopId(0, 13, (short)1, 0),
                        null,
                        new GradoopId(0, 8, (short)0, 0),
                        new GradoopId(0, 1, (short)0, 0),
                        null,
                        graphId,
                        800003000L,
                        Long.MAX_VALUE),
                new TemporalEdge(
                        new GradoopId(0, 14, (short)1, 0),
                        null,
                        new GradoopId(0, 8, (short)0, 0),
                        new GradoopId(0, 1, (short)0, 0),
                        null,
                        graphId,
                        800003000L,
                        Long.MAX_VALUE))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<TemporalEdge>() {
            @Override
            public long extractAscendingTimestamp(TemporalEdge temporalEdge) {
                MonotonyViolationHandler violationHandler = new IgnoringHandler();
                return temporalEdge.getValidFrom();
            }
        });
    }

    static DataStream<TemporalEdge> getMovieEdgesTemp(StreamExecutionEnvironment env) {
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
                })
                /*
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<TemporalEdge>() {

                    @Override
                    public long extractAscendingTimestamp(TemporalEdge temporalEdge) {
                        return temporalEdge.getValidFrom();
                    }
                })
                */
                ;
        return edges;
    }

    public static  DataStream<Edge<Long, String>> getMovieEdges(StreamExecutionEnvironment env) throws IOException {

        return env.readTextFile("src/main/resources/ml-100k/u.data")
                .map(new MapFunction<String, Edge<Long, String>>() {
                    @Override
                    public Edge<Long, String> map(String s) throws Exception {
                        String[] fields = s.split("\t");
                        long src = Long.parseLong(fields[0]);
                        long trg = Long.parseLong(fields[1]);
                        String value = fields[2] + "," + fields[3];
                        return new Edge<>(src, trg, value);
                    }
                });

    }
}