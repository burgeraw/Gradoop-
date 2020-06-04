package gellyStreaming.gradoop.model;


import akka.stream.impl.fusing.MapAsync;
import akka.stream.impl.io.InputStreamSinkStage;
import gellyStreaming.gradoop.partitioner.CustomKeySelector;
import gellyStreaming.gradoop.partitioner.DBHPartitioner;
import org.apache.commons.logging.LogConfigurationException;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.client.program.MiniClusterClient;
import org.apache.flink.configuration.*;
import org.apache.flink.core.execution.DetachedJobExecutionResult;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.SingleJobJobGraphStore;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionTracker;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionTrackerImpl;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.resourcemanager.TaskExecutorRegistration;
import org.apache.flink.runtime.resourcemanager.registration.JobManagerRegistration;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.RestClientConfiguration;
import org.apache.flink.runtime.rest.RestServerEndpoint;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationResult;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.JobManagerMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.JobManagerMetricsMessageParameters;
import org.apache.flink.runtime.taskexecutor.JobManagerConnection;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.runtime.taskexecutor.TaskExecutorResourceUtils;
import org.apache.flink.runtime.taskexecutor.TaskExecutorToResourceManagerConnection;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.runtime.webmonitor.WebMonitorEndpoint;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.TimestampAssigner;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ConfigurationException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Environment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.jboss.netty.logging.Log4JLoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.TimeUnit.*;

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
        //GraphStream<Long, NullValue, Long> graph = new SimpleEdgeStream<>(edges, env);
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
        JobExecutionResult job = env.execute();
        System.out.println(job.getNetRuntime());
    }

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



    static void incrementalState() throws Exception {
        int numberOfPartitions = 4;
        Configuration config = new Configuration();
        config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
        //config.setString(JobManagerOptions.ADDRESS, "localhost");
        //config.setInteger(RestOptions.RETRY_MAX_ATTEMPTS, 10);
        //config.setLong(RestOptions.RETRY_DELAY, 1000);
        //config.setInteger(RestOptions.PORT, 0);
        //config.setString(RestOptions.ADDRESS,"localhost");
        //config.setBoolean(DeploymentOptions.ATTACHED, false);


        //ExecutorService ex = WebMonitorEndpoint.createExecutorService(config.getInteger(RestOptions.SERVER_NUM_THREADS),
        //        config.getInteger(RestOptions.SERVER_THREAD_PRIORITY),"name");
        //RestClient restClient = new RestClient(RestClientConfiguration.fromConfiguration(config),ex);
        //RestServerEndpointConfiguration restServerEndpointConfiguration = RestServerEndpointConfiguration.fromConfiguration(config);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(numberOfPartitions, config);

        env.setParallelism(numberOfPartitions);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        SimpleTemporalEdgeStream tempEdges = getSimpleTemporalMovieEdgesStream2(env, numberOfPartitions,
                "src/main/resources/ml-100k/ml-100-sorted.csv");

        // 8 hours is enough to get the entire database in one (eventtime) window, to check if all edges get added.
        // If you check the output files you see that the 4 partitions add up to 100000, which is the size
        // of the edgefile used. You can also see the partitioner is running correctly since all edges in
        // each partition have the same partitionId in their properties.
        SimpleTemporalEdgeStream doubleEdges = tempEdges.undirected();
        QueryState QS = new QueryState();

        GraphState gs = tempEdges.buildState(QS,"EL-proc",
                org.apache.flink.streaming.api.windowing.time.Time.of(4, MILLISECONDS),
                org.apache.flink.streaming.api.windowing.time.Time.of(2, MILLISECONDS),
                numberOfPartitions);
        //sg.getJobGraph().setJobID(jobId);
        JobClient jobClient = env.executeAsync();
        gs.overWriteQS(jobClient.getJobID());
        System.out.println(jobClient.getJobExecutionResult(ClassLoader.getPlatformClassLoader()).get().getNetRuntime(MILLISECONDS) + " milliseconds");
        System.out.println("JobID end: "+jobClient.getJobID());

        //System.out.println("jobid end: "+results.getJobID());
        //System.out.println("The job took "+results.getNetRuntime(MILLISECONDS)+ " millisec");

    }


    public static void testState() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int numberOfPartitions = 4;
        env.setParallelism(numberOfPartitions);
        DataStream<Tuple2<Edge<Long, String>, Integer>> partitionedStream =
                new PartitionEdges<Long, String>().getPartitionedEdges(getMovieEdges(env), numberOfPartitions);
        GradoopIdSet graphId = new GradoopIdSet();
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
        }).assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<TemporalEdge>() {
            @Nullable
            @Override
            public Watermark checkAndGetNextWatermark(TemporalEdge temporalEdge, long l) {
                return null;
            }

            @Override
            public long extractTimestamp(TemporalEdge temporalEdge, long l) {
                return temporalEdge.getValidFrom();
            }
        });
        SimpleTemporalEdgeStream edgestream = new SimpleTemporalEdgeStream(tempEdges, env, graphId);
        //edgestream.buildState("EL"); //6944, 6730, 7225
        edgestream.buildState("EL2"); //4906, 4806, 5126
        JobExecutionResult result = env.execute();
        System.out.println(result.getNetRuntime(MILLISECONDS)+" milliseconds for job.");
    }

    public static void queryableState() throws Exception {
        int numberOfPartitions = 4;
        Configuration config = new Configuration();
        config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
        String tmHostname = TaskManagerLocation.getHostName(InetAddress.getLocalHost());
        int proxyPort = 9069;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(numberOfPartitions, config);
        env.setParallelism(numberOfPartitions);
        DataStream<Tuple2<Edge<Long, String>, Integer>> partitionedStream =
                new PartitionEdges<Long, String>().getPartitionedEdges(getMovieEdges2(env, "src/main/resources/ml-100k/ml-100-sorted.csv"), numberOfPartitions);
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
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<TemporalEdge>() {
            @Override
            public long extractAscendingTimestamp(TemporalEdge temporalEdge) {
                return temporalEdge.getValidFrom();
            }
        });
        SimpleTemporalEdgeStream edgestream = new SimpleTemporalEdgeStream(tempEdges, env, graphId);
        //edgestream.buildState("EL", Time.of(8, HOURS), Time.of(8, HOURS));
        StreamGraph graph = env.getStreamGraph();
        System.out.println("JobId: "+graph.getJobGraph().getJobID());
        env.execute(graph);
    }

    public static void queryableState2() throws Exception {
        int numberOfPartitions = 4;
        Configuration config = new Configuration();
        config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(numberOfPartitions, config);
        env.getConfig().disableSysoutLogging();
        //log4j Log4JLoggerFactory.getDefaultFactory();
        //log4j.logger.org.apache.flink=WARN;

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        SimpleTemporalEdgeStream edges = getSimpleTemporalMovieEdgesStream2(env, numberOfPartitions,
                "src/main/resources/ml-100k/ml-100k-sorted.csv");


        GraphState state = edges.buildState(new QueryState(), "TTL", 2000L, 1000L, numberOfPartitions);

        JobClient jobClient = env.executeAsync();
        state.overWriteQS(jobClient.getJobID());
        System.out.println(jobClient.getJobExecutionResult(ClassLoader.getPlatformClassLoader()).get().getNetRuntime(MILLISECONDS) + " milliseconds");
        System.out.println("JobID end: "+jobClient.getJobID());
    }

    public static void restApi() throws ConfigurationException {
        Configuration config = new Configuration();
        config.setString(JobManagerOptions.ADDRESS, "localhost");
        config.setInteger(RestOptions.RETRY_MAX_ATTEMPTS, 10);
        config.setLong(RestOptions.RETRY_DELAY, 0);
        config.setInteger(RestOptions.PORT, 0);

        RestServerEndpointConfiguration restServerEndpointConfiguration = RestServerEndpointConfiguration.fromConfiguration(config);

        //DispatcherGateway gateway =
        //GatewayRetriever<DispatcherGateway> retriever = () -> CompletableFuture.completedFuture(gateway);

        //RestClient restClient = new RestClient(RestClientConfiguration.fromConfiguration(config), );
        //RestServerEndpoint.
        //jobmanager.web.ssl.enabled
        ExecutorService ex = WebMonitorEndpoint.createExecutorService(config.getInteger(RestOptions.SERVER_NUM_THREADS),
                config.getInteger(RestOptions.SERVER_THREAD_PRIORITY),"name");
        RestClient restClient = new RestClient(RestClientConfiguration.fromConfiguration(config),ex);

        //restClient.sendRequest("localhost", 0, ?);

    }

    public static void triangleEstimator() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SimpleTemporalEdgeStream edges = getSimpleTemporalMovieEdgesStream2(env, 1,
                "src/main/resources/aves-sparrow-social.edges");
        env.setParallelism(1);
        //edges.getGlobalTriangles(1000000, 0.1).writeAsText("out", FileSystem.WriteMode.OVERWRITE);
        edges.TriangleCount(1000000, 0.1).getGlobalTriangles().print();
        env.execute();
    }

    public static void queryableStateAndVertexCounting() throws Exception {
        int numberOfPartitions = 2;
        Configuration config = new Configuration();
        config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(numberOfPartitions, config);
        env.setParallelism(numberOfPartitions);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        SimpleTemporalEdgeStream tempEdges = getSimpleTemporalMovieEdgesStream2(env, numberOfPartitions,
                //"src/main/resources/Cit-HepPh.txt");
                //"src/main/resources/aves-sparrow-social.edges");
                "src/main/resources/ml-100k/ml-100k-sorted.csv");
                //"src/main/resources/aves-sparrow-social2.txt");
        //SimpleTemporalEdgeStream doubleEdges = tempEdges.undirected();
        tempEdges.print();
        QueryState QS = new QueryState();

        //GraphState gs = tempEdges.buildState(QS,"EL-proc",
        //        org.apache.flink.streaming.api.windowing.time.Time.of(1000, MILLISECONDS),
        //        org.apache.flink.streaming.api.windowing.time.Time.of(1000, MILLISECONDS),
        //        numberOfPartitions);
        GraphState gs = tempEdges.buildState(QS, "TTL", 1000000000L, 10000L, numberOfPartitions);
        JobClient jobClient = env.executeAsync();
        gs.overWriteQS(jobClient.getJobID());
        // 2636 on par(1)
        // 2,741, 2634 on par(2)
        // 2661, 2668 on par(4)
        // Finds 1283359 instead of 3358499 on Cit-HepPh.
        //
        //env.execute();
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
        GraphState gs = tempEdges.buildState(new QueryState(), "vertices", 1000000000L, 10000L, numberOfPartitions);
        JobClient jobClient = env.executeAsync();
        gs.overWriteQS(jobClient.getJobID());
    }



    public static void countTriangle() throws Exception {
        int numberOfPartitions = 1;
        Configuration config = new Configuration();
        config.set(DeploymentOptions.ATTACHED, false);

        config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(numberOfPartitions, config);
        env.setParallelism(numberOfPartitions);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //SimpleTemporalEdgeStream tempEdges = getSimpleTemporalMovieEdgesStream2(env, numberOfPartitions,
        //        "src/main/resources/as-733/all days/as20000102.txt");

        /*
                        "src/main/resources/as-733/as20000102.txt"); // finds 41872, 35154

                //"src/main/resources/Cit-HepPh.txt");
                //"src/main/resources/aves-sparrow-social.edges"); //finds 2636
                //"src/main/resources/ml-100k/ml-100k-sorted.csv");
        //"src/main/resources/aves-sparrow-social2.txt");

         */
        //SimpleTemporalEdgeStream doubleEdges = tempEdges.undirected();
        SimpleTemporalEdgeStream tempEdges = getSimpleTemporalMovieEdgesStream2(env, numberOfPartitions,
                "src/main/resources/aves-sparrow-social.edges");
        GraphState gs = tempEdges.buildState(new QueryState(), "triangles",numberOfPartitions);
        JobClient jobClient = env.executeAsync();
        //JobExecutionResult result = env.execute();
        gs.overWriteQS(jobClient.getJobID());
        //gs.overWriteQS(result.getJobID());
    }



    public static void main(String[] args) throws Exception {
        //Runtime rt = Runtime.getRuntime();
        //long usedMB = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;
        //System.out.println("Used MB before: "+ usedMB);
        //testLoadingGraph();
        //testGradoopSnapshotStream();
        //testPartitioner();
        //incrementalState();
        //testState();
        //queryableState();
        //queryableState2();
        //restApi();
        //triangleEstimator();
        //queryableStateAndVertexCounting();
        //countVertex();
        countTriangle();
        //Thread.sleep(100000);
        //Runtime rt2 = Runtime.getRuntime();
        //long usedMB2 = (rt2.totalMemory() - rt2.freeMemory()) / 1024 / 1024;
        //System.out.println("Used MB after: "+ usedMB2);
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