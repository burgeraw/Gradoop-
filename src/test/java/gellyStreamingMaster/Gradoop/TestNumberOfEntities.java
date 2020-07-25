package gellyStreamingMaster.Gradoop;

import gellyStreaming.gradoop.model.SimpleTemporalEdgeStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.junit.Test;

public class TestNumberOfEntities extends AbstractTestBase {

    @Test
    public void testNumberOfVertices() throws Exception {
        /*
         * Test numberOfVertices() with the sample graph
         */
        final String resultPath = getTempDirPath("result");
        final String expectedResult = "1\n" +
                "2\n" +
                "3\n" +
                "4\n" +
                "5\n";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final GradoopIdSet graphId = new GradoopIdSet();
        SimpleTemporalEdgeStream graph = new SimpleTemporalEdgeStream(GraphTemporalStreamTestUtils.getLongLongEdgeDataStream(env, graphId), env, graphId);
        graph.numberOfVertices()
                .writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
        env.execute();
        compareResultsByLinesInMemory(expectedResult, resultPath);
    }

    @Test
    public void testNumberOfEdges() throws Exception {
        /*
         * Test numberOfEdges() with the sample graph
         */
        final String resultPath = getTempDirPath("result");
        final String expectedResult = "1\n" +
                "2\n" +
                "3\n" +
                "4\n" +
                "5\n" +
                "6\n" +
                "7\n";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final GradoopIdSet graphId = new GradoopIdSet();
        SimpleTemporalEdgeStream graph = new SimpleTemporalEdgeStream(GraphTemporalStreamTestUtils.getLongLongEdgeDataStream(env, graphId), env, graphId);
        graph.numberOfEdges().writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
        env.execute();
        compareResultsByLinesInMemory(expectedResult, resultPath);
    }
}
