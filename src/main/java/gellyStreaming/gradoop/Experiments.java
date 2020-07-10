package gellyStreaming.gradoop;

import gellyStreaming.gradoop.model.GraphState;
import gellyStreaming.gradoop.model.QueryState;
import gellyStreaming.gradoop.model.SimpleTemporalEdgeStream;
import gellyStreaming.gradoop.util.makeSimpleTemporalEdgeStream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

public class Experiments {

    public static long valueToReach = -1;

    public static void Experiment1a(String filepath,
                                    String numberOfEdges,
                                    String runNumber,
                                    String datastructure,
                                    String edgeOrVertexPartitioner,
                                    String numberOfVertices) {
        File log = new File("Results/Experiment1a_edges" + numberOfEdges +"_"+datastructure+"_"+edgeOrVertexPartitioner+"partitioned_run" + runNumber+".txt");
        int numberOfPartitions = 25;
        PrintStream logStream = null;
        try {
            logStream = new PrintStream(log);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        System.setOut(logStream);
        System.out.println("Started job at \t" + System.currentTimeMillis());
        Configuration config = new Configuration();
        config.set(DeploymentOptions.ATTACHED, false);
        config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(numberOfPartitions, config);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        SimpleTemporalEdgeStream edgeStream;
        valueToReach = Long.parseLong(numberOfEdges);
        if (edgeOrVertexPartitioner.equals("vertex")) {
            edgeStream = makeSimpleTemporalEdgeStream.getVertexPartitionedStream(env, numberOfPartitions, filepath,
                    Integer.parseInt(numberOfVertices), Integer.parseInt(numberOfEdges), false);
        } else {
            edgeStream = makeSimpleTemporalEdgeStream.getEdgePartitionedStream(
                    env, numberOfPartitions, filepath, false);
        }
        env.setParallelism(numberOfPartitions);
        QueryState QS = new QueryState();
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
                                    String numberOfEdges) {
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
        Configuration config = new Configuration();
        config.set(DeploymentOptions.ATTACHED, false);
        config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(numberOfPartitions, config);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        SimpleTemporalEdgeStream edgeStream;
        valueToReach = Long.parseLong(numberOfEdges);
        if (edgeOrVertexPartitioner.equals("vertex")) {
            edgeStream = makeSimpleTemporalEdgeStream.getVertexPartitionedStream(env, numberOfPartitions, filepath,
                    Integer.parseInt(numberOfVertices), Integer.parseInt(numberOfEdges), false);
        } else {
            edgeStream = makeSimpleTemporalEdgeStream.getEdgePartitionedStream(
                    env, numberOfPartitions, filepath, false);
        }
        env.setParallelism(numberOfPartitions);
        QueryState QS = new QueryState();
        GraphState GS = edgeStream.buildState(QS, datastructure, 16000L, null,
                numberOfPartitions, true, 1000,
                null);
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void Experiment2() {

    }

    public static void main(String[] args) {
        switch (args[0]) {
            case "1a":
                String filepath = args[1];
                String numberOfEdges = args[2];
                String runNumber = args[3];
                String datastructure = args[4];
                String edgeOrVertexPartitioner = args[5];
                String numberOfVertices;
                if(edgeOrVertexPartitioner.equals("vertex")) {
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
            case "2" :


        }
    }
}
