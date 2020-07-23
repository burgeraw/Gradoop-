package gellyStreaming.gradoop;

import gellyStreaming.gradoop.algorithms.*;
import gellyStreaming.gradoop.model.GraphState;
import gellyStreaming.gradoop.model.QueryState;
import gellyStreaming.gradoop.model.SimpleTemporalEdgeStream;
import gellyStreaming.gradoop.util.makeSimpleTemporalEdgeStream;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.*;
import java.net.UnknownHostException;

public class Experiments {

    private static void Experiment1(String filepath,
                                     String numberOfEdges,
                                     String runNumber,
                                     String datastructure,
                                     String edgeOrVertexPartitioner,
                                     String numberOfVertices,
                                     String parallelism) throws UnknownHostException {
        System.out.println("Experiment1: edges " + numberOfEdges + ", " + datastructure + ", " + edgeOrVertexPartitioner
                + " partitioned, run " + runNumber+ ", parallelism "+parallelism);
        int numberOfPartitions = Integer.parseInt(parallelism);
        System.out.println("Started job at \t" + System.currentTimeMillis());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(numberOfPartitions);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        SimpleTemporalEdgeStream edgeStream;
        if (edgeOrVertexPartitioner.equals("vertex")) {
            edgeStream = makeSimpleTemporalEdgeStream.getVertexPartitionedStream(
                    env, numberOfPartitions, filepath,
                    Integer.parseInt(numberOfVertices), Integer.parseInt(numberOfEdges), false);
        } else {
            edgeStream = makeSimpleTemporalEdgeStream.getEdgePartitionedStream(
                    env, numberOfPartitions, filepath, false);

        }
        env.setParallelism(numberOfPartitions);
        QueryState QS = new QueryState();

        // By setting slide to null, we only get output once the full state is loaded. More precisely, output gets
        // triggered if the last batch timestamp is more than 10 seconds ago.
        GraphState GS = edgeStream.buildState(QS, datastructure, 16000L, null,
                numberOfPartitions, true, 1000,
                null);
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void Experiment2(String batchSizePerPU,
                                    String runNumber,
                                    String filepath,
                                    String datastructure,
                                    String activeOrLazyPurging,
                                    String parallelism,
                                    String windowSize,
                                    String slideSize) throws UnknownHostException {
        System.out.println("Experiment2: batchSize " + batchSizePerPU + ", " + datastructure + ", " + activeOrLazyPurging
                + " purging, run" + runNumber + ", parallelism "+parallelism+", windowsize "+windowSize+", slide "+slideSize);
        int numberOfPartitions = Integer.parseInt(parallelism);
        System.out.println("Started job at: \t" + System.currentTimeMillis());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(numberOfPartitions);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        SimpleTemporalEdgeStream edgeStream = makeSimpleTemporalEdgeStream.getEdgePartitionedStream(
                env, numberOfPartitions, filepath, false);
        env.setParallelism(numberOfPartitions);
        QueryState QS = new QueryState();
        // Change window/slide to fit dataset. System exits/closes when all data has been loaded in state, so not
        // all data also gets deleted.
        GraphState GS = edgeStream.buildState(QS, datastructure, Long.parseLong(windowSize), Long.parseLong(slideSize),
                numberOfPartitions, (activeOrLazyPurging.equals("lazy")), Integer.parseInt(batchSizePerPU),
                null);
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void Experiment3(String algorithmGranularity,
                                    String edgeOrVertexPartitioner,
                                    String withCaching,
                                    String QSbatchSize,
                                    String filepath,
                                    String runNumber,
                                    String numberOfVertices,
                                    String numberOfEdges,
                                    String fullyDecoupled,
                                    String parallelism) throws UnknownHostException {
        System.out.println("Experiment3: granularity " + algorithmGranularity + ", " + edgeOrVertexPartitioner +
                "partitioned, caching " + withCaching + ", QS batchsize "+QSbatchSize+", run " + runNumber +
                ", fullyDecoupled " + fullyDecoupled);
        int numberOfPartitions = Integer.parseInt(parallelism);
        System.out.println("Started job at: \t" + System.currentTimeMillis());

        // For local execution. (QS tmHostname also needs to be changed to "localhost").
        //Configuration config = new Configuration();
        //config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(numberOfPartitions, config);

        // For cluster execution. (QS tmHostname = TaskManagerLocation.getHostName(InetAddress.getLocalHost()))
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(numberOfPartitions);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        SimpleTemporalEdgeStream edgeStream = null;
        Algorithm alg = null;
        if (edgeOrVertexPartitioner.equals("edge")) {
            edgeStream = makeSimpleTemporalEdgeStream.getEdgePartitionedStream(
                    env, numberOfPartitions, filepath, true);
            edgeStream = edgeStream.undirected();
            alg = new TriangleCountingALRetrieveAllState();
        } else if (edgeOrVertexPartitioner.equals("vertex")) {
            edgeStream = makeSimpleTemporalEdgeStream.getVertexPartitionedStream(
                    env, numberOfPartitions, filepath, Integer.parseInt(numberOfVertices),
                    Integer.parseInt(numberOfEdges), true);
            switch (algorithmGranularity) {
                case "state":
                    alg = new TriangleCountingALRetrieveAllState();
                    break;
                case "edge":
                    alg = new TriangleCountingALRetrieveEdge(
                            Integer.parseInt(QSbatchSize), (withCaching.equals("true")));
                    break;
                case "vertex":
                    alg = new TriangleCountingALRetrieveVertex(
                            Integer.parseInt(QSbatchSize), (withCaching.equals("true")));
                    break;
                }
        }

        QueryState QS = new QueryState();
        GraphState GS;
        assert edgeStream != null;
        if (fullyDecoupled.equals("true")) {
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
            FileWriter fr = new FileWriter("/share/hadoop/annemarie/tempJobId");
            BufferedWriter bf = new BufferedWriter(fr);
            bf.write(jobID.toHexString());
            bf.close();
            fr.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void Experiment5(String withCaching,
                                    String QSbatchSize,
                                    String filepath,
                                    String runNumber,
                                    String numberOfVertices,
                                    String numberOfEdges,
                                    String fullyDecoupled,
                                    String parallelism,
                                    String timeToRun,
                                    String repeats,
                                    String windowSize,
                                    String slide,
                                    String withQS) throws UnknownHostException {
        System.out.println("Experiment 5: caching " + withCaching + ", QS batchsize "+QSbatchSize+", run " + runNumber +
                ", fullyDecoupled " + fullyDecoupled + ", output each " + timeToRun + ", repeat output times "+ repeats +
                " , windowSize "+windowSize+" , slide "+slide+", withQS "+withQS);
        int numberOfPartitions = Integer.parseInt(parallelism);
        System.out.println("Started job at: \t" + System.currentTimeMillis());

        // For local execution. (QS tmHostname also needs to be changed to "localhost").
        //Configuration config = new Configuration();
        //config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(numberOfPartitions, config);

        // For cluster execution. (QS tmHostname = TaskManagerLocation.getHostName(InetAddress.getLocalHost()))
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(numberOfPartitions);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        SimpleTemporalEdgeStream edgeStream = makeSimpleTemporalEdgeStream.getVertexPartitionedStream(
                env, numberOfPartitions, filepath, Integer.parseInt(numberOfVertices), Integer.parseInt(numberOfEdges), true);
        Algorithm alg = new EstimateTrianglesAL(Integer.parseInt(QSbatchSize), withCaching.equals("true"),
                Long.parseLong(timeToRun), Integer.parseInt(repeats), withQS.equals("true"));

        QueryState QS = new QueryState();
        GraphState GS;
        Long slideL;
        if(slide.equals("null")) {
            slideL = null;
        } else {
            slideL = Long.parseLong(slide);
        }
        if (fullyDecoupled.equals("true")) {
            GS = edgeStream.buildState(QS, "AL", Long.parseLong(windowSize), slideL,
                    numberOfPartitions, true, 1000,
                    null);
            GS.doDecoupledAlg(alg).print();
        } else {
            GS = edgeStream.buildState(QS, "AL", Long.parseLong(windowSize), slideL,
                    numberOfPartitions, true, 1000,
                    alg);
            GS.getAlgorithmOutput().print();
        }
        try {
            JobClient jobClient = env.executeAsync();
            JobID jobID = jobClient.getJobID();
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
            //Experiment1("src/main/resources/email-Eu-core.txt", "25571", "1",
              //      "sortedEL", "edge", null, "8");
            //Experiment2("100", "1", "/home/annemarie/Documents/gitkraken/Gradoop++/src/main/resources/email-Eu-core.txt",
              //      "AL", "active", "4", "1000", "1000");
            //Experiment3("edge", "vertex", "true", "100000",
              //      "/home/annemarie/Documents/gitkraken/Gradoop++/resources/AL/email-Eu-core", "1",
                //    "1005", "32770", "false", "4");
            Experiment5("true", "10000", "/home/annemarie/Documents/gitkraken/Gradoop++/resources/AL/Cit-HepPh",
                    "1", "34546", "421578", "false", "2", "50000",
                    "1", "200000", "null", "false");

        } else {
            switch (args[0]) {

                // Test total time necessary to load all data into state.
                case "1":
                    String filepath = args[1];
                    String numberOfEdges = args[2];
                    String runNumber = args[3];
                    String datastructure = args[4];
                    String edgeOrVertexPartitioner = args[5];
                    String numberOfVertices = args[6];
                    String parallelism = args[7];
                    Experiment1(filepath, numberOfEdges, runNumber, datastructure, edgeOrVertexPartitioner, numberOfVertices, parallelism);
                    break;

                // Test behaviour when loading elements in state while also deleting them after window, either with lazy or active
                // batch removal, and varying batch sizes, window/slide sizes
                case "2":
                    String batchSizePerPU = args[1];
                    runNumber = args[2];
                    filepath = args[3];
                    datastructure = args[4];
                    String activeOrLazyPurging = args[5];
                    parallelism = args[6];
                    String windowSize = args[7];
                    String slideSize = args[8];
                    Experiment2(batchSizePerPU, runNumber, filepath, datastructure, activeOrLazyPurging, parallelism, windowSize, slideSize);
                    break;

                // Test behaviour of exact triangle counting algorithms on full state. First load all elements in AL, then
                // use a certain granularity algorithm (state, vertex, edge). Very QSbatchsize, which is grouping together
                // remote partition requests before using QS. Also can be run fully decoupled. Mind that this creates an
                // infinite stream and requires manually job cancelling once all partitions (in all taskmanagers) have
                // printed their results.
                case "3":
                    String algorithmGranularity = args[1];
                    edgeOrVertexPartitioner = args[2];
                    String withCaching = args[3];
                    String QSbatchSize = args[4];
                    filepath = args[5];
                    runNumber = args[6];
                    numberOfVertices = args[7];
                    numberOfEdges = args[8];
                    String fullyDecoupled = args[9];
                    parallelism = args[10];
                    Experiment3(algorithmGranularity, edgeOrVertexPartitioner, withCaching, QSbatchSize,
                                filepath, runNumber, numberOfVertices, numberOfEdges, fullyDecoupled, parallelism);
                    break;

                // Experiment 4: use 3, but focus on reported the memory usage. Get these from web interface of flink

                // Test behaviour of estimated triangle counting. First on all data, to prove how it converges to
                // the true triangle count, then its behaviour when used as intended each slide.
                case "5" :
                    withCaching = args[1];
                    QSbatchSize = args[2];
                    filepath = args[3];
                    runNumber = args[4];
                    numberOfVertices = args[5];
                    numberOfEdges = args[6];
                    fullyDecoupled = args[7];
                    parallelism = args[8];
                    String timeToRun = args[9];
                    String repeats = args[10];
                    windowSize = args[11];
                    slideSize = args[12];
                    String withQS = args[13];
                    Experiment5(withCaching, QSbatchSize, filepath, runNumber, numberOfVertices, numberOfEdges, fullyDecoupled,
                            parallelism, timeToRun, repeats, windowSize, slideSize, withQS);
                    break;
            }
        }
    }
}
