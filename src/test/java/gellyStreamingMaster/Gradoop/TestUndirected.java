package gellyStreamingMaster.Gradoop;

import gellyStreaming.gradoop.model.SimpleTemporalEdgeStream;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.junit.Test;

public class TestUndirected extends AbstractTestBase {

    @Test
    public void testProgram() throws Exception {
        /*
         * Test undirected() with the sample graph
         */
        final String resultPath = getTempDirPath("result");
        final String expectedResult = "1,2,12\n" +
                "2,1,12\n" +
                "1,3,13\n" +
                "3,1,13\n" +
                "2,3,23\n" +
                "3,2,23\n" +
                "3,4,34\n" +
                "4,3,34\n" +
                "3,5,35\n" +
                "5,3,35\n" +
                "4,5,45\n" +
                "5,4,45\n" +
                "5,1,51\n" +
                "1,5,51\n";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        GradoopIdSet graphId = new GradoopIdSet();
        SimpleTemporalEdgeStream graph = new SimpleTemporalEdgeStream(GraphTemporalStreamTestUtils.getLongLongEdgeDataStream(env, graphId), env, graphId);
        graph.undirected().getEdges()
                .map((MapFunction<TemporalEdge, String>) temporalEdge -> ""+temporalEdge.getSourceId().toString().substring(13,14)+","
                        +temporalEdge.getTargetId().toString().substring(13,14)+","
                        +temporalEdge.getPropertyValue("value").toString())
                .writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
        env.execute();
        compareResultsByLinesInMemory(expectedResult, resultPath);
    }
}
