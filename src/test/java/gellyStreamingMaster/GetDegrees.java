package gellyStreamingMaster;

import gellyStreaming.gradoop.oldModel.GraphStream;
import gellyStreaming.gradoop.oldModel.SimpleEdgeStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.NullValue;
import org.junit.Test;

public class GetDegrees extends AbstractTestBase{

    @Test
    public void testGetDegrees() throws Exception {
        /*
         * Test getDegrees() with the sample graph
         */
        final String resultPath = getTempDirPath("result");
        final String expectedResult = "1,1\n" +
                "1,2\n" +
                "1,3\n" +
                "2,1\n" +
                "2,2\n" +
                "3,1\n" +
                "3,2\n" +
                "3,3\n" +
                "3,4\n" +
                "4,1\n" +
                "4,2\n" +
                "5,1\n" +
                "5,2\n" +
                "5,3\n";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        GraphStream<Long, NullValue, Long> graph = new SimpleEdgeStream<>(GraphStreamTestUtils.getLongLongEdgeDataStream(env), env);
        graph.getDegrees().writeAsCsv(resultPath, FileSystem.WriteMode.OVERWRITE);
        env.execute();

        compareResultsByLinesInMemory(expectedResult, resultPath);
    }

    @Test
    public void testGetInDegrees() throws Exception {
        /*
         * Test getInDegrees() with the sample graph
         */
        final String resultPath = getTempDirPath("result");
        final String expectedResult = "1,1\n" +
                "2,1\n" +
                "3,1\n" +
                "3,2\n" +
                "4,1\n" +
                "5,1\n" +
                "5,2\n";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        GraphStream<Long, NullValue, Long> graph = new SimpleEdgeStream<>(GraphStreamTestUtils.getLongLongEdgeDataStream(env), env);

        graph.getInDegrees().writeAsCsv(resultPath, FileSystem.WriteMode.OVERWRITE);
        env.execute();

        compareResultsByLinesInMemory(expectedResult, resultPath);
    }

    @Test
    public void testGetOutDegrees() throws Exception {
        /*
         * Test getOutDegrees() with the sample graph
         */
        final String resultPath = getTempDirPath("result");
        final String expectedResult = "1,1\n" +
                "1,2\n" +
                "2,1\n" +
                "3,1\n" +
                "3,2\n" +
                "4,1\n" +
                "5,1\n";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        GraphStream<Long, NullValue, Long> graph = new SimpleEdgeStream<>(GraphStreamTestUtils.getLongLongEdgeDataStream(env), env);

        graph.getOutDegrees().writeAsCsv(resultPath, FileSystem.WriteMode.OVERWRITE);
        env.execute();

        compareResultsByLinesInMemory(expectedResult, resultPath);
    }
}