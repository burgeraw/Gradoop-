package gellyStreaming.gradoop.model;


import gellyStreaming.gradoop.partitioner.HashVertexPartitioner;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Tests2 {

    public static void countTriangles() throws Exception {
        int numberOfPartitions = 8;
        Configuration config = new Configuration();
        config.set(DeploymentOptions.ATTACHED, false);
        config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(numberOfPartitions, config);
        env.getConfig().enableSysoutLogging();
        SimpleTemporalEdgeStream tempEdges = getTempEdges(env, numberOfPartitions,
                "src/main/resources/email-Eu-core.txt", "\\s"); //found 218385, should be 105461
        // With par(2), found
                //"src/main/resources/Cit-HepPh.txt", "\\s"); //finding 1282207 should be 1276868
        GraphState gs = tempEdges.buildState(new QueryState(), "triangles",numberOfPartitions);
        JobClient jobClient = env.executeAsync();
        //JobExecutionResult result = env.execute();
        gs.overWriteQS(jobClient.getJobID());
        Logger LOG = LoggerFactory.getLogger(GraphState.class);
        LOG.error("log");

    }

    public static void testVertexPartitioner() throws Exception {
        //String filepath = "src/main/resources/aves-sparrow-social.edges";
        //516 edges: 4:253, 9:180, 1:224, 2:173 = 830 with trying to place trg&src in same partition
        //516 edges: 4:253, 9:160, 1:160, 2:272 = 845 without
        // Should find 8.2k triangles, found: 534, 407, 674, 313 = 1928
        // Not screening duplicate edges found: 531, 251, 372, 198 = 1352
        // Parallelism(1) = 1857
        //String filepath = "src/main/resources/Cit-HepPh.txt";
        //421578 edges: 169022, 169022, 169022, 169021 = 676083 with if size smallest > 0.1* size partition(src)
        //421578 edges: 172816, 172815, 172815, 172815 = 691261 with if size smallest > 0.5* size partition(src)
        //421578 edges: 4:182757, 9:182756, 1:182757, 2: 182757 = 731027 without
        // Should find 1.276.868 triangles, found: 236352, 257327, 265925, 241437 = 1.001.066
        // Not screening duplicate edges:   found: 235560, 255919, 264977, 240350 = 996.806
        // Parallelism(1) = found 1.277.612 triangles
        //String filepath = "src/main/resources/email-Eu-core.txt";
        // 25571 edges: 11228, 10125, 11513, 9974 = 42.840
        // Should find 105.461 traingles, found: 38428, 35771, 53122, 26680 = 153.352
        // Not screening duplicate edges: found: 21362, 19563, 28525, 14317 = 83.767
        // Paralellism(1) = 16706 edges w/o duplicates 121.973 triangles.
        //String filepath = "src/main/resources/ml-100k/u.data"; --> mind that movie & user should be different.
        // 100.000 edges: 43591, 39381, 41307, 49539 = 173.818
        // Shouldnt find any triangles: correct.
        //String filepath = "src/main/resources/aves-sparrow-social2.txt";
        // 12 edges, should have 2 triangles, found 2.
        String filepath = "src/main/resources/as-733/as20000102.txt"; //tab separated 26467 edges
        // 13895 edges w/o duplicates, found 11612 triangles, should be 6584.
        //String filepath = "src/main/resources/GeneratedEdges.csv";
        // 2200 edges, correctly finding 1099 triangles.
        // with par(4): 521, 517, 516, 718 = 2272 edges
        // should find 1100 traingles, found 249, 246, 249, 351 = 1095
        String delimiter = "\\s";
        int numberOfPartitions = 1;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Tuple2<Edge<Long, String>, Integer>> partitionedStream =
                new HashVertexPartitioner<String>().getPartitionedEdges(getEdges(env, filepath, delimiter), numberOfPartitions);
        //partitionedStream.print();
        env.setParallelism(numberOfPartitions);
        GradoopIdSet graphId = new GradoopIdSet();
        SimpleTemporalEdgeStream edges = new SimpleTemporalEdgeStream(partitionedStream.keyBy(1)
                .map(new MapFunction<Tuple2<Edge<Long, String>, Integer>, TemporalEdge>() {
                    @Override
                    public TemporalEdge map(Tuple2<Edge<Long, String>, Integer> edge) throws Exception {
                        Map<String, Object> properties = new HashMap<>();
                        properties.put("partitionID", edge.f1);
                        return new TemporalEdge(
                                GradoopId.get(),
                                "watched",
                                new GradoopId(edge.f0.getSource().intValue(),0 ,(short) 0, 0),
                                new GradoopId(edge.f0.getTarget().intValue(), 0,(short) 0, 0),
                                //new GradoopId(edge.f0.getTarget().intValue(), 0, (short) 1, 0),
                                Properties.createFromMap(properties),
                                graphId,
                                //timestamp, //       (valid) starting time
                                0L,
                                Long.MAX_VALUE);
                    }
                })
                ,env, graphId);
        GraphState gs = edges.buildState("triangles");
        env.execute();
    }

    public static void WindowedVertexCounter() throws IOException, InterruptedException {
        int numberOfPartitions = 2;
        Configuration config = new Configuration();
        config.set(DeploymentOptions.ATTACHED, false);
        config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(numberOfPartitions, config);
        env.getConfig().enableSysoutLogging();
        SimpleTemporalEdgeStream tempEdges = getTempEdges(env, numberOfPartitions,
                "src/main/resources/email-Eu-core.txt", "\\s");
        GraphState gs = tempEdges.buildState(new QueryState(), "vertices",10000L,
                10000L,numberOfPartitions);
        try {
            JobClient jobClient = env.executeAsync();
            gs.overWriteQS(jobClient.getJobID());
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public static void main(String[] args) throws Exception {
        //countTriangles();
        //testVertexPartitioner();
        WindowedVertexCounter();
    }

    public static SimpleTemporalEdgeStream getTempEdges(StreamExecutionEnvironment env, Integer numberOfPartitions, String filepath, String delimiter) throws IOException {
        env.setParallelism(numberOfPartitions);
        DataStream<Tuple2<Edge<Long, String>, Integer>> partitionedStream =
                new PartitionEdges<Long, String>().getPartitionedEdges(getEdges(env, filepath, delimiter), numberOfPartitions);
        GradoopIdSet graphId = new GradoopIdSet();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        DataStream<TemporalEdge> tempEdges = partitionedStream.map(new MapFunction<Tuple2<Edge<Long, String>, Integer>, TemporalEdge>() {
            @Override
            public TemporalEdge map(Tuple2<Edge<Long, String>, Integer> edge) throws Exception {
                Map<String, Object> properties = new HashMap<>();
                //Integer rating = Integer.parseInt(edge.f0.f2.split(",")[0]);
                //Long timestamp = Long.parseLong(edge.f0.f2.split(",")[1]);
                //properties.put("rating", rating);
                properties.put("partitionID", edge.f1);
                return new TemporalEdge(
                        GradoopId.get(),
                        "watched",
                        new GradoopId(edge.f0.getSource().intValue(),0 ,(short) 0, 0),
                        new GradoopId(edge.f0.getTarget().intValue(), 0,(short) 0, 0),
                        //new GradoopId(edge.f0.getTarget().intValue(), 0, (short) 1, 0),
                        Properties.createFromMap(properties),
                        graphId,
                        //timestamp, //       (valid) starting time
                        0L,
                        Long.MAX_VALUE
                );
            }
        })
                //.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<TemporalEdge>() {
                // @Override
                //public long extractAscendingTimestamp(TemporalEdge edge) {
                //return edge.getTxFrom();
                //    }
                //})
                ;
        SourceFunction<TemporalEdge> infinite = new SourceFunction<TemporalEdge>() {
            @Override
            public void run(SourceContext<TemporalEdge> sourceContext) throws Exception {
                /*
                Map<String, Object> properties = new HashMap<>();
                properties.put("partitionID", 1);
                sourceContext.collect(new TemporalEdge(GradoopId.get(),
                        "watched",
                        new GradoopId(0, 640, (short)0,0),
                        new GradoopId(0, 712, (short)0,0),
                        Properties.createFromMap(properties),
                        null, null, null));
                 */
                while(true) {
                    sourceContext.collect(new TemporalEdge(null, null, null, null,
                            null, null, null, null));
                    Thread.sleep(100);
                }
            }
            @Override
            public void cancel() {

            }
        };
        DataStream<TemporalEdge> makeInfinite =  env.addSource(infinite);
        DataStream<TemporalEdge> finalEdges = tempEdges.union(makeInfinite)
                .filter(new FilterFunction<TemporalEdge>() {
                    @Override
                    public boolean filter(TemporalEdge edge) throws Exception {
                        return edge.getId() != null;
                    }
                });

        return new SimpleTemporalEdgeStream(finalEdges, env, graphId);
    }

    public static DataStream<Edge<Long, String>> getEdges(StreamExecutionEnvironment env, String filepath, String delimiter) throws IOException {
        //env.readTextFile(filepath).print();
        return env.readTextFile(filepath).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                //System.out.println(s);
                return !s.startsWith("#");
            }
        })
                .map(new MapFunction<String, Edge<Long, String>>() {
                    @Override
                    public Edge<Long, String> map(String s) throws Exception {
                        //System.out.println(s);
                        //String[] fields = s.split(",");
                        //String[] fields = s.split("\t");
                        String[] fields = s.split(delimiter);
                        long src = Long.parseLong(fields[0]);
                        long trg = Long.parseLong(fields[1]);
                        //String value = fields[2] + "," + fields[3];
                        //return new Edge<>(src, trg, value);
                        return new Edge<>(src, trg, null);
                    }
                });

    }
}
