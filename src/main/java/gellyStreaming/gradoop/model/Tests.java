package gellyStreaming.gradoop.model;


import gellyStreaming.gradoop.algorithms.*;
import gellyStreaming.gradoop.partitioner.*;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.*;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import java.io.*;
import java.util.*;

public class Tests {
    // Read this one it is useful: https://arxiv.org/pdf/1912.12740.pdf

    public static void testPartitioner() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int numberOfPartitions = 8;
        env.setParallelism(numberOfPartitions);
        DataStream<Edge<Long, String>> edges = getMovieEdges(env);

        CustomKeySelector<Long, String> keySelector1 = new CustomKeySelector<>(0);

        Partitioner<Long> partitioner = new DBHPartitioner<>(keySelector1, numberOfPartitions);

        KeySelector<Tuple2<Edge<Long, String>,Integer>, Integer> keySelector3 =
                new KeySelector<Tuple2<Edge<Long, String>, Integer>, Integer>() {
            @Override
            public Integer getKey(Tuple2<Edge<Long, String>, Integer> edgeIntegerTuple2) throws Exception {
                return edgeIntegerTuple2.f1;
            }
        };
/*
        // Original way as used by Gelly-streaming, However, we require a keyby to use state.
        DataStream<Edge<Long, String>> partitionedEdges = edges
                .partitionCustom(new DBHPartitioner<>(
                        new CustomKeySelector<Long, String>(0), numberOfPartitions),
                        new CustomKeySelector<Long, String>(0));
 */
        // Using map to save partition in order to key on the partition.
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

        KeyedStream<Tuple2<Edge<Long, String>, Integer>, Integer> keyedStream =
                partitionedEdges2.keyBy(keySelector3);

        //keyedStream.print();
        keyedStream.writeAsCsv("out", FileSystem.WriteMode.OVERWRITE);
        JobExecutionResult result = env.execute();
    }

    public static void countVertex() throws Exception {
        int numberOfPartitions = 8;
        Configuration config = new Configuration();
        config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
        config.set(DeploymentOptions.ATTACHED, false);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(numberOfPartitions, config);
        env.setParallelism(numberOfPartitions);
        env.getConfig().enableSysoutLogging();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        SimpleTemporalEdgeStream tempEdges = getSimpleTemporalMovieEdgesStream2(env, numberOfPartitions,
                "src/main/resources/as20000102.txt");
                //"src/main/resources/Cit-HepPh.txt");
                //"src/main/resources/aves-sparrow-social.edges");
                //"src/main/resources/ml-100k/ml-100k-sorted.csv");
        //"src/main/resources/aves-sparrow-social2.txt");
        //SimpleTemporalEdgeStream doubleEdges = tempEdges.undirected();
        //GraphState gs = tempEdges.buildState(new QueryState(), "vertices", 1000000000L, 10000L, numberOfPartitions);
        JobClient jobClient = env.executeAsync();
        //gs.overWriteQS(jobClient.getJobID());
    }


    public static void countTriangles2() throws IOException, InterruptedException {
        int numberOfPartitions = 1;
        Configuration config = new Configuration();
        config.set(DeploymentOptions.ATTACHED, false);
        config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(numberOfPartitions, config);
        env.setParallelism(numberOfPartitions);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        SimpleTemporalEdgeStream tempEdges = getSimpleTemporalMovieEdgesStream2(env, numberOfPartitions,
                //"src/main/resources/aves-sparrow-social.edges");
                //"src/main/resources/email-Eu-core.txt");
                "src/main/resources/Cit-HepPh.txt");
                //"src/main/resources/as-733/as20000102.txt");
        QueryState QS = new QueryState();
        //GraphState gs = tempEdges.buildState(new QueryState(), "triangles2", 200000L, 10000L, numberOfPartitions);
        try {
            JobClient jobClient = env.executeAsync();
           // gs.overWriteQS(jobClient.getJobID());
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public static void builtState() throws IOException, InterruptedException {
        int numberOfPartitions = 2;
        Configuration config = new Configuration();
        config.set(DeploymentOptions.ATTACHED, false);
        config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(numberOfPartitions, config);
        env.setParallelism(numberOfPartitions);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        SimpleTemporalEdgeStream tempEdges = getSimpleTemporalMovieEdgesStream2(env, numberOfPartitions,
                //"src/main/resources/aves-sparrow-social.edges");
                //"src/main/resources/email-Eu-core.txt");
        "src/main/resources/Cit-HepPh.txt");
        //"src/main/resources/as-733/as20000102.txt");
        //tempEdges = tempEdges.undirected();
        //GraphState gs = tempEdges.buildState(new QueryState(), "buildSortedEL", 20000L, 5000L, numberOfPartitions);
        try {
            JobClient jobClient = env.executeAsync();
          //  gs.overWriteQS(jobClient.getJobID());
        } catch (Exception e) {
            System.out.println(e);
        }
    }


    public static void testBuildingState() throws IOException, InterruptedException {
        int numberOfPartitions = 2;
        Configuration config = new Configuration();
        config.set(DeploymentOptions.ATTACHED, false);
        config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(numberOfPartitions, config);
        env.setParallelism(numberOfPartitions);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        SimpleTemporalEdgeStream tempEdges = getSimpleTemporalMovieEdgesStream2(env, numberOfPartitions,
                "src/main/resources/Cit-HepPh.txt");
                //"src/main/resources/email-Eu-core.txt");
        tempEdges = tempEdges.undirected();
        QueryState QS = new QueryState();
        GraphState GS1 = tempEdges.buildState(QS, "AL", 500000L, 60000L,
                numberOfPartitions, false, 100000, new TriangleCountingALRetrieveAllState());
        try {
            GS1.getAlgorithmOutput().print();
        } catch (Exception e) {
            e.printStackTrace();
        }
        //GraphState GS2 = tempEdges.buildState(QS, "buildEL", 5000L, 1000L, numberOfPartitions);
        //GraphState GS3 = tempEdges.buildState(QS, "buildSortedEL", 5000L, 1000L, numberOfPartitions);
        try {
            JobClient jobClient = env.executeAsync();
            GS1.overWriteQS(jobClient.getJobID());
            //GS1.countTriangles().print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void makeAL() throws Exception {
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);
        //DataStream<Edge<Long, String>> edgeDataStream = getMovieEdges2(env, "src/main/resources/email-Eu-core.txt");
        //FennelPartitioner fennelPartitioner = new FennelPartitioner();
        //HashMap<Long, HashSet<Long>> state = fennelPartitioner.AL4(edgeDataStream);
        //System.out.println(state.toString());
        HelpState state = new HelpState(true);
        //FileReader fr = new FileReader("src/main/resources/email-Eu-core.txt");
        FileReader fr = new FileReader("src/main/resources/Cit-HepPh.txt");
        BufferedReader br = new BufferedReader(fr);
        String line;
        int counter = 0;
        while((line = br.readLine())!= null) {
            String[] fields = line.split("\\s");
            long src = Long.parseLong(fields[0]);
            long trg = Long.parseLong(fields[1]);
            state.addEdge(src, trg);
            System.out.println("We read "+counter++ +" edges");
        }
        br.close();
        fr.close();

        HashMap<Long, HashSet<Long>> stateFinal = state.returnState();

        File output = new File("resources/AL/Cit-HepPh");
        try (BufferedWriter bf = new BufferedWriter(new FileWriter(output))) {
            for (long src : stateFinal.keySet()) {
                bf.write( src + ":" + Arrays.toString(stateFinal.get(src).toArray()) );
                bf.newLine();
            }
            bf.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void testVertexPartitioner() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FennelPartitioning fennel = new FennelPartitioning();
        DataStream<Tuple2<Edge<Long, String>, Integer>> edges = fennel.getFennelPartitionedEdges(
                env, "resources/AL/email-Eu-core", 4, 1005, 25570
        );
        GradoopIdSet graphId = new GradoopIdSet();
        DataStream<TemporalEdge> tempEdges = edges.map(new MapFunction<Tuple2<Edge<Long, String>, Integer>, TemporalEdge>() {
            @Override
            public TemporalEdge map(Tuple2<Edge<Long, String>, Integer> edge) throws Exception {
                Map<String, Object> properties = new HashMap<>();
                properties.put("partitionID", edge.f1);
                properties.put("sourceVertexId", edge.f0.getSource());
                properties.put("targetVertexId", edge.f0.getTarget());
                return new TemporalEdge(
                        GradoopId.get(),
                        "watched",
                        new GradoopId(0, edge.f0.getSource().intValue(), (short)0, 0),
                        new GradoopId(0, edge.f0.getTarget().intValue(), (short)0, 0),
                        Properties.createFromMap(properties),
                        graphId,
                        0L, //       (valid) starting time
                        Long.MAX_VALUE
                );
            }
        });
        SourceFunction<TemporalEdge> infinite = new SourceFunction<TemporalEdge>() {
            @Override
            public void run(SourceContext<TemporalEdge> sourceContext) throws Exception {
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
        SimpleTemporalEdgeStream edgeStream = new SimpleTemporalEdgeStream(finalEdges, env, graphId);
        //edgeStream.print();
        env.setParallelism(1);

        edgeStream.getEdges().process(new ProcessFunction<TemporalEdge, Object>() {
            StoredVertexPartitionState copy = fennel.getState();
            @Override
            public void processElement(TemporalEdge edge, Context context, Collector<Object> collector) throws Exception {
                Long src = edge.getProperties().get("sourceVertexId").getLong();
                Long trg = edge.getProperties().get("targetVertexId").getLong();
                Iterator<Byte> byteIterator = fennel.getPartitions(src);
                List<Byte> byteList = new LinkedList<>();
                for (Iterator<Byte> it = byteIterator; it.hasNext(); ) {
                    byteList.add(it.next());
                }
                System.out.println("For key: "+src+" we have source vertex partitions: "+ byteList.toString());
                List<Byte> byteList1 = new LinkedList<>();
                byteIterator = fennel.getPartitions(trg);
                for (Iterator<Byte> it = byteIterator; it.hasNext(); ) {
                    byteList1.add(it.next());
                }
                System.out.println("For key: "+trg+" we have target vertex partitions: "+ byteList1.toString());

            }
        });
        env.execute();

    }

    public static void vertexBasedTriangleCounting() throws IOException, InterruptedException {
        int numberOfPartitions = 2;
        Configuration config = new Configuration();
        config.set(DeploymentOptions.ATTACHED, false);
        config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(numberOfPartitions, config);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        SimpleTemporalEdgeStream tempEdges = makeEdgesTemporal(env, numberOfPartitions, "resources/AL/Cit-HepPh",
                34546,421578); //1.276.868 triangles
        //SimpleTemporalEdgeStream tempEdges = makeEdgesTemporal(env, numberOfPartitions, "resources/AL/email-Eu-core",
        //        1005, 50000);//25571); //105.461 triangles, estimation finds 118/119.000
        //SimpleTemporalEdgeStream tempEdges = getSimpleTemporalMovieEdgesStream2(env, numberOfPartitions,
        //"src/main/resources/ER-20");
        //        "src/main/resources/Cit-HepPh.txt");
        //"src/main/resources/email-Eu-core.txt");
        //tempEdges = tempEdges.undirected();
        env.setParallelism(numberOfPartitions);
        QueryState QS = new QueryState();
        GraphState GS = tempEdges.buildState(QS, "AL", 20000L, null,
                numberOfPartitions, true, 100000,
                //new TriangleCountingFennelALRetrieveVertex(fennel, 10000000, true));
                //new TriangleCountingALRetrieveAllState());
                //new TriangleCountingFennelALRetrieveVertex(fennel, 1000000000, false));
                //null);
                //new DistinctVertexCounterFennelAL(fennel));
                //new TotalSizeALState());
                new EstimateTrianglesFennelAL(fennel, 1000, true, 300000L));
        GS.getAlgorithmOutput().print();
        //GS.getDecoupledOutput().print();
        try {
            JobClient jobClient = env.executeAsync();
            GS.overWriteQS(jobClient.getJobID());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void GradoopIdTests() {
        for(int i = 0; i < 1000; i++) {
            //int i = 16777215;
            GradoopId test = new GradoopId(0, i, (short) 0, 0);
            System.out.println(i + " : " + test.toString());
            System.out.println(GradoopIdUtil.getLong(test));
        }
    }

    public static void testDecoupled() throws IOException, InterruptedException {
        int numberOfPartitions = 2;
        Configuration config = new Configuration();
        config.set(DeploymentOptions.ATTACHED, false);
        config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(numberOfPartitions, config);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //SimpleTemporalEdgeStream tempEdges = makeEdgesTemporal(env, numberOfPartitions, "resources/AL/Cit-HepPh",
        //        34546,421578); //1.276.868 triangles
        SimpleTemporalEdgeStream tempEdges = makeEdgesTemporal(env, numberOfPartitions, "resources/AL/email-Eu-core",
                1005, 25571); //105.461 triangles
        //SimpleTemporalEdgeStream tempEdges = getSimpleTemporalMovieEdgesStream2(env, numberOfPartitions,
        //"src/main/resources/ER-20");
        //        "src/main/resources/Cit-HepPh.txt");
        //"src/main/resources/email-Eu-core.txt");
        //tempEdges = tempEdges.undirected();
        env.setParallelism(numberOfPartitions);
        QueryState QS = new QueryState();
        GraphState GS = tempEdges.buildState(QS, "AL", 16000L, 1000L,
                numberOfPartitions, true, 100000,
                //new TriangleCountingFennelALRetrieveVertex(fennel, 10000000, true));
                //new TriangleCountingALRetrieveAllState());
                //new TriangleCountingFennelALRetrieveEdge(
                //        fennel, 1000000000, true));
                null);
        //GS.getAlgorithmOutput().print();
        TriangleCountingFennelALRetrieveEdge alg = new TriangleCountingFennelALRetrieveEdge(
                fennel, 1000000000, false);
        GS.doDecoupledAlg(alg).print();
        //GS.getAlgorithmOutput().print();
        try {
            JobClient jobClient = env.executeAsync();
            GS.overWriteQS(jobClient.getJobID());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    public static void main(String[] args) throws Exception {
        //Runtime rt = Runtime.getRuntime();
        //long usedMB = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;
        //System.out.println("Used MB before: "+ usedMB);
        //testPartitioner();
        //countVertex();
        //countTriangles2();
        //builtState();
        //testBuildingState();
        //makeAL();
        //testVertexPartitioner();
        vertexBasedTriangleCounting();
        //testDecoupled();
        //GradoopIdTests();
        //Thread.sleep(100000);
        //Runtime rt2 = Runtime.getRuntime();
        //long usedMB2 = (rt2.totalMemory() - rt2.freeMemory()) / 1024 / 1024;
        //System.out.println("Used MB after: "+ usedMB2);
    }

    public static FennelPartitioning fennel;

    static SimpleTemporalEdgeStream makeEdgesTemporal(StreamExecutionEnvironment env, Integer numberOfPartitions, String filepath,
                                                      Integer vertexCount, Integer edgeCount) throws IOException {
        fennel = new FennelPartitioning();
        DataStream<Tuple2<Edge<Long, String>, Integer>> edges = fennel.getFennelPartitionedEdges(
                env, filepath, numberOfPartitions, vertexCount, edgeCount
        );
        GradoopIdSet graphId = new GradoopIdSet();
        DataStream<TemporalEdge> tempEdges = edges.map(new MapFunction<Tuple2<Edge<Long, String>, Integer>, TemporalEdge>() {
            @Override
            public TemporalEdge map(Tuple2<Edge<Long, String>, Integer> edge) throws Exception {
                Map<String, Object> properties = new HashMap<>();
                properties.put("partitionID", edge.f1);
                properties.put("sourceVertexId", edge.f0.getSource());
                properties.put("targetVertexId", edge.f0.getTarget());
                return new TemporalEdge(
                        GradoopId.get(),
                        "watched",
                        new GradoopId(0, edge.f0.getSource().intValue(), (short)0, 0),
                        new GradoopId(0, edge.f0.getTarget().intValue(), (short)0, 0),
                        Properties.createFromMap(properties),
                        graphId,
                        0L, //       (valid) starting time
                        Long.MAX_VALUE
                );
            }
        });
        SourceFunction<TemporalEdge> infinite = new SourceFunction<TemporalEdge>() {
            @Override
            public void run(SourceContext<TemporalEdge> sourceContext) throws Exception {
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

    static SimpleTemporalEdgeStream getSimpleTemporalMovieEdgesStream(StreamExecutionEnvironment env, Integer numberOfPartitions, String filepath) throws IOException {
        env.setParallelism(numberOfPartitions);
        DataStream<Tuple2<Edge<Long, String>, Integer>> partitionedStream =
                new PartitionEdges<Long, String>().getPartitionedEdges(getMovieEdges2(env, filepath), numberOfPartitions);
        GradoopIdSet graphId = new GradoopIdSet();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<TemporalEdge> tempEdges = partitionedStream.map(new MapFunction<Tuple2<Edge<Long, String>, Integer>, TemporalEdge>() {
            @Override
            public TemporalEdge map(Tuple2<Edge<Long, String>, Integer> edge) throws Exception {
                Map<String, Object> properties = new HashMap<>();
                Integer rating = Integer.parseInt(edge.f0.f2.split(",")[0]);
                Long timestamp = Long.parseLong(edge.f0.f2.split(",")[1]);
                properties.put("rating", rating);
                properties.put("partitionID", edge.f1);
                return new TemporalEdge(
                        GradoopId.get(),
                        "watched",
                        new GradoopId(0, edge.f0.getSource().intValue(), (short)0, 0),
                        new GradoopId(0, edge.f0.getTarget().intValue(), (short)1, 0),
                        Properties.createFromMap(properties),
                        graphId,
                        timestamp, //       (valid) starting time
                        Long.MAX_VALUE
                );
            }
        })
                /*
                .assignTimestampsAndWatermarks(
                        new AssignerWithPunctuatedWatermarks<TemporalEdge>() {
                            @Nullable
                            @Override
                            public Watermark checkAndGetNextWatermark(TemporalEdge edge, long l) {
                                return null;
                            }

                            @Override
                            public long extractTimestamp(TemporalEdge edge, long l) {
                                return edge.getValidFrom();
                            }
                        });

                 */
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<TemporalEdge>() {
            @Override
            public long extractAscendingTimestamp(TemporalEdge temporalEdge) {
                return temporalEdge.getValidFrom();
            }
        });

        return new SimpleTemporalEdgeStream(tempEdges, env, graphId);
    }

    static SimpleTemporalEdgeStream getSimpleTemporalMovieEdgesStream2(StreamExecutionEnvironment env, Integer numberOfPartitions, String filepath) throws IOException {
        env.setParallelism(numberOfPartitions);
        DataStream<Tuple2<Edge<Long, String>, Integer>> partitionedStream =
                new PartitionEdges<Long, String>().getPartitionedEdges(getMovieEdges2(env, filepath), numberOfPartitions);
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

    static DataStream<TemporalEdge> getMovieEdgesTemp(StreamExecutionEnvironment env, String filepath) {
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
                                Long.parseLong(values[3]), //       (valid) starting time
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
    public static DataStream<Edge<Long, String>> getMovieEdges2(StreamExecutionEnvironment env, String filepath) throws IOException {
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
                        //String[] fields = s.split(",");
                        String[] fields = s.split("\\s");
                        long src = Long.parseLong(fields[0]);
                        long trg = Long.parseLong(fields[1]);
                        //String value = fields[2] + "," + fields[3];
                        //return new Edge<>(src, trg, value);
                        return new Edge<>(src, trg, null);
                    }
                });

    }

}