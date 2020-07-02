package gellyStreaming.gradoop.model;

import gellyStreaming.gradoop.algorithms.CheckNumberOfElementsAL;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static gellyStreaming.gradoop.model.Tests.makeEdgesTemporal;

public class Experiments {

    public static logWriter myLogWriter;


    public static void Experiment1b() throws IOException, InterruptedException, ExecutionException {
        myLogWriter = new logWriter("out/log");
        //for(int i : new int[]{1, 2, 4, 8, 16}) {
            //cont = false
            //System.out.println("Now testing for paralellism "+n);
            int numberOfPartitions = 16;
            Configuration config = new Configuration();
            config.set(DeploymentOptions.ATTACHED, false);
            config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
            StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(numberOfPartitions, config);
            env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
            myLogWriter.appendLine("Job started at "+System.currentTimeMillis());
            SimpleTemporalEdgeStream tempEdges = makeEdgesTemporal(env, numberOfPartitions, "resources/AL/Cit-HepPh",
                    34546,421578); //1.276.868 triangles
            //SimpleTemporalEdgeStream tempEdges = getSimpleTemporalMovieEdgesStream2(env, numberOfPartitions,
            //"src/main/resources/ER-20"); //10M edges
            env.setParallelism(numberOfPartitions);
            QueryState QS = new QueryState();
            GraphState GS = tempEdges.buildState(QS, "AL", 160L, 100L,
                    numberOfPartitions, false, 1000,
                    //new TriangleCountingFennelALRetrieveVertex(fennel, 10000000, true));
                    //new TriangleCountingALRetrieveAllState());
                    //new TriangleCountingFennelALRetrieveEdge(
                    //        fennel, 1000000000, true));
                    //new DistinctVertexCounterFennelAL(fennel));
                    new CheckNumberOfElementsAL(421578));
            try {
                JobClient jobClient = env.executeAsync();
                GS.overWriteQS(jobClient.getJobID());
            } catch (Exception e) {
                e.printStackTrace();
            }
        //}
    }

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        Experiment1b();
    }
}
