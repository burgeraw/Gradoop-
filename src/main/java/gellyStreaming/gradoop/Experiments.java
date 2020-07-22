package gellyStreaming.gradoop;

import gellyStreaming.gradoop.algorithms.Algorithm;
import gellyStreaming.gradoop.algorithms.TriangleCountingALRetrieveAllState;
import gellyStreaming.gradoop.algorithms.TriangleCountingFennelALRetrieveEdge;
import gellyStreaming.gradoop.algorithms.TriangleCountingFennelALRetrieveVertex;
import gellyStreaming.gradoop.model.GraphState;
import gellyStreaming.gradoop.model.QueryState;
import gellyStreaming.gradoop.model.SimpleTemporalEdgeStream;
import gellyStreaming.gradoop.partitioner.FennelPartitioning;
import gellyStreaming.gradoop.util.makeSimpleTemporalEdgeStream;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.*;
import java.net.UnknownHostException;

public class Experiments {

    // Can be set to a value so that the program exits, and the infinite stream stops.
    public static long valueToReach = -1;
    public static int algValueToReach = -1;
    public static transient makeSimpleTemporalEdgeStream stream = new makeSimpleTemporalEdgeStream();

    public makeSimpleTemporalEdgeStream getStream() {
        return stream;
    }

    public static void Experiment1a(String filepath,
                                    String numberOfEdges,
                                    String runNumber,
                                    String datastructure,
                                    String edgeOrVertexPartitioner,
                                    String numberOfVertices) throws UnknownHostException {
        File log = new File("Results/Experiment1a_edges" + numberOfEdges +"_"+datastructure+"_"+edgeOrVertexPartitioner+"partitioned_run" + runNumber+".txt");
        int numberOfPartitions = 4;
        PrintStream logStream = null;
        try {
            logStream = new PrintStream(log);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        System.setOut(logStream);
        System.out.println("Started job at \t" + System.currentTimeMillis());
        //For local execution
        //Configuration config = new Configuration();
        //config.set(DeploymentOptions.ATTACHED, false);
        //config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(numberOfPartitions, config);

        // For cluster execution
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(numberOfPartitions);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        SimpleTemporalEdgeStream edgeStream;
        stream = new makeSimpleTemporalEdgeStream();
        valueToReach = Long.parseLong(numberOfEdges);
        if (edgeOrVertexPartitioner.equals("vertex")) {
            edgeStream = stream.getVertexPartitionedStream(
                    env, numberOfPartitions, filepath,
                    Integer.parseInt(numberOfVertices), Integer.parseInt(numberOfEdges), false);
        } else {
            edgeStream = stream.getEdgePartitionedStream(
                    env, numberOfPartitions, filepath, false);
        }
        env.setParallelism(numberOfPartitions);
        QueryState QS = null;
        QS = new QueryState();
        // By setting slide to null, we only get output once the full state is loaded.
        GraphState GS = edgeStream.buildState(QS, datastructure, 16000L, null,
                numberOfPartitions, true, 1000,
                null);
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void Experiment1b(String parallelism,
                                    String runNumber,
                                    String filepath,
                                    String datastructure,
                                    String edgeOrVertexPartitioner,
                                    String numberOfVertices,
                                    String numberOfEdges) throws UnknownHostException {
        File log = new File("Results/Experiment1b_parallelism" + parallelism+"_"+datastructure+"_"+edgeOrVertexPartitioner + "partitioned_run" + runNumber+".txt");
        int numberOfPartitions = Integer.parseInt(parallelism);
        PrintStream logStream = null;
        try {
            logStream = new PrintStream(log);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        System.setOut(logStream);
        System.out.println("Started job at \t" + System.currentTimeMillis());
        // For local execution.
        //Configuration config = new Configuration();
        //config.set(DeploymentOptions.ATTACHED, false);
        //config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(numberOfPartitions, config);
        // For cluster execution.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(numberOfPartitions);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        makeSimpleTemporalEdgeStream stream = new makeSimpleTemporalEdgeStream();
        SimpleTemporalEdgeStream edgeStream;
        valueToReach = Long.parseLong(numberOfEdges);
        if (edgeOrVertexPartitioner.equals("vertex")) {
            edgeStream = stream.getVertexPartitionedStream(env, numberOfPartitions, filepath,
                    Integer.parseInt(numberOfVertices), Integer.parseInt(numberOfEdges), false);
        } else {
            edgeStream = stream.getEdgePartitionedStream(
                    env, numberOfPartitions, filepath, false);
        }
        env.setParallelism(numberOfPartitions);
        QueryState QS = null;
        QS = new QueryState();
        GraphState GS = edgeStream.buildState(QS, datastructure, 16000L, null,
                numberOfPartitions, true, 1000,
                null);
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void Experiment2(String batchSizePerPU,
                                   String runNumber,
                                   String filepath,
                                   String datastructure,
                                   String activeOrLazyPurging,
                                   String numberOfEdges) throws UnknownHostException {
        File log = new File("Results/Experiment2_batchSize" + batchSizePerPU+"_"+datastructure+"_"+activeOrLazyPurging + "purging_run" + runNumber+".txt");
        int numberOfPartitions = 4; // local
        //int numberOfPartitions = 100; //cluster
        PrintStream logStream = null;
        try {
            logStream = new PrintStream(log);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        System.setOut(logStream);
        System.out.println("Started job at: \t" + System.currentTimeMillis());
        // For local execution.
        //Configuration config = new Configuration();
        //config.set(DeploymentOptions.ATTACHED, false);
        //config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(numberOfPartitions, config);
        // For cluster execution.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(numberOfPartitions);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        valueToReach = Long.parseLong(numberOfEdges);
        SimpleTemporalEdgeStream edgeStream = new makeSimpleTemporalEdgeStream().getEdgePartitionedStream(
                    env, numberOfPartitions, filepath, false);
        env.setParallelism(numberOfPartitions);
        QueryState QS = null;
        QS = new QueryState();
        // Change window/slide to fit dataset. System exits when all data has been loaded in state, so not
        // all data also gets deleted.
        GraphState GS = edgeStream.buildState(QS, datastructure, 300L, 50L,
                numberOfPartitions, (activeOrLazyPurging.equals("lazy")), Integer.parseInt(batchSizePerPU),
                null);
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void Experiment3a(String algorithmGranularity,
                                    String edgeOrVertexPartitioner,
                                    String withCaching,
                                    String QSbatchSize,
                                    String filepath,
                                    String runNumber,
                                    String numberOfVertices,
                                    String numberOfEdges,
                                    String fullyDecoupled) throws InstantiationException, UnknownHostException {
        File log = new File("Results/Experiment3a_granularity" + algorithmGranularity+"_"+edgeOrVertexPartitioner+
                "_caching"+withCaching+ "_run" + runNumber+"_fullyDecoupled"+fullyDecoupled+".txt");
        int numberOfPartitions = 4; // local
        //int numberOfPartitions = 100; //cluster
        PrintStream logStream = null;
        try {
            logStream = new PrintStream(log);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        //System.setOut(logStream);
        //System.out.println("Started job at: \t" + System.currentTimeMillis());

        // For local execution.
        //Configuration config = new Configuration();
        //config.set(DeploymentOptions.ATTACHED, false);
        //config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(numberOfPartitions, config);

        // For cluster execution.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(numberOfPartitions);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //valueToReach = Long.parseLong(numberOfEdges);
        algValueToReach = numberOfPartitions;
        SimpleTemporalEdgeStream edgeStream;
        env.setParallelism(numberOfPartitions);
        Algorithm alg;
        makeSimpleTemporalEdgeStream stream = new makeSimpleTemporalEdgeStream();
        if(edgeOrVertexPartitioner.equals("edge")) {
            edgeStream = stream.getEdgePartitionedStream(
                    env, numberOfPartitions, filepath, true);
            alg = new TriangleCountingALRetrieveAllState();
            edgeStream = edgeStream.undirected();
        } else if(edgeOrVertexPartitioner.equals("vertex")) {
            edgeStream = stream.getVertexPartitionedStream(
                    env, numberOfPartitions, filepath, Integer.parseInt(numberOfVertices),
                    Integer.parseInt(numberOfEdges), true);
            FennelPartitioning fennel  = stream.getFennel();
            switch (algorithmGranularity) {
                case "state":
                    alg = new TriangleCountingALRetrieveAllState();
                    break;
                case "edge":
                    alg = new TriangleCountingFennelALRetrieveEdge(fennel,
                            Integer.parseInt(QSbatchSize), (withCaching.equals("true")));
                    break;
                case "vertex":
                    alg = new TriangleCountingFennelALRetrieveVertex(fennel,
                            Integer.parseInt(QSbatchSize), (withCaching.equals("true")));
                    break;
                default:
                    throw new InstantiationException("Give either 'edge' or 'vertex' or 'state' as input for algorithmGranularity. ");
            }
        } else {
            throw new InstantiationException("Give either 'edge' or 'vertex' as input for vertexPartitioner. ");
        }
        final QueryState QS = new QueryState();
        GraphState GS;
        if(fullyDecoupled.equals("true")) {
            GS = edgeStream.buildState(QS, "AL", 30000L, null,
                    numberOfPartitions, true, 1000,
                    null);
            GS.doDecoupledAlg(alg).print();
        } else {
            GS = edgeStream.buildState(QS, "AL", 30000L, null,
                    numberOfPartitions, true, 1000,
                    alg);
            GS.getAlgorithmOutput().print();
        }
        try {
            JobClient jobClient = env.executeAsync();
            JobID jobID = jobClient.getJobID();
            //GS.overWriteQS(jobID);
            FileWriter fr = new FileWriter("/share/hadoop/annemarie/tempJobId");
            BufferedWriter bf = new BufferedWriter(fr);
            bf.write(jobID.toHexString());
            bf.close();
            fr.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws InstantiationException, UnknownHostException {
        if (args.length == 0) {
            //Experiment1a("src/main/resources/email-Eu-core.txt", "25571", "1",
            //        "sortedEL", "edge", null);
            //Experiment1b("8", "1", "src/main/resources/email-Eu-core.txt",
             //       "EL", "edge", null, "25571");
            //Experiment1b("8","1","resources/AL/email-Eu-core","AL",
            //        "vertex", "1005", "32770");
            //Experiment2("100", "1", "src/main/resources/email-Eu-core.txt",
            //        "AL", "lazy", "25571");
            //Experiment2("100", "1", "src/main/resources/email-Eu-core.txt",
            //        "AL", "active", "25571");

            Experiment3a("edge", "edge", "true", "100000",
                    "src/main/resources/email-Eu-core.txt", "1", "1005", "25571",
                        "false");

        } else {
            switch (args[0]) {
                case "1a":
                    String filepath = args[1];
                    String numberOfEdges = args[2];
                    String runNumber = args[3];
                    String datastructure = args[4];
                    String edgeOrVertexPartitioner = args[5];
                    String numberOfVertices;
                    if (edgeOrVertexPartitioner.equals("vertex")) {
                        numberOfVertices = args[6];
                    } else {
                        numberOfVertices = null;
                    }
                    Experiment1a(filepath, numberOfEdges, runNumber, datastructure, edgeOrVertexPartitioner, numberOfVertices);
                    break;
                case "1b":
                    String parallelism = args[1];
                    runNumber = args[2];
                    String datasetFilepath = args[3];
                    datastructure = args[4];
                    edgeOrVertexPartitioner = args[5];
                    if (edgeOrVertexPartitioner.equals("vertex")) {
                        numberOfVertices = args[7];
                        numberOfEdges = args[6];
                    } else {
                        numberOfVertices = null;
                        numberOfEdges = args[6];
                    }
                    Experiment1b(parallelism, runNumber, datasetFilepath, datastructure, edgeOrVertexPartitioner, numberOfVertices, numberOfEdges);
                    break;
                case "2":
                    String batchSizePerPU = args[1];
                    runNumber = args[2];
                    filepath = args[3];
                    datastructure = args[4];
                    String activeOrLazyPurging = args[5];
                    numberOfEdges = args[6];
                    Experiment2(batchSizePerPU, runNumber, filepath, datastructure, activeOrLazyPurging, numberOfEdges);
                    break;
                case "3a":
                    String algorithmGranularity = args[1];
                    edgeOrVertexPartitioner = args[2];
                    String withCaching = args[3];
                    String QSbatchSize = args[4];
                    filepath = args[5];
                    runNumber = args[6];
                    numberOfVertices = args[7];
                    numberOfEdges = args[8];
                    String fullyDecoupled = args[9];
                    try {
                        Experiment3a(algorithmGranularity, edgeOrVertexPartitioner, withCaching, QSbatchSize,
                                filepath, runNumber, numberOfVertices, numberOfEdges, fullyDecoupled);
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                    }
            }
        }
    }
}
