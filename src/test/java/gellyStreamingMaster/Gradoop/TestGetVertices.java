package gellyStreamingMaster.Gradoop;

import gellyStreaming.gradoop.model.SimpleTemporalEdgeStream;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.junit.Test;

public class TestGetVertices extends AbstractTestBase {

    @Test
    public void test() throws Exception {
        /*
         * Test getVertices() with the sample graph
         */
        final String resultPath = getTempDirPath("result");
        final String expectedResult = "1,(null)\n" +
                "2,(null)\n" +
                "3,(null)\n" +
                "4,(null)\n" +
                "5,(null)\n";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final GradoopIdSet graphId = new GradoopIdSet();
        SimpleTemporalEdgeStream graph = new SimpleTemporalEdgeStream(GraphTemporalStreamTestUtils.getLongLongEdgeDataStream(env, graphId), env, graphId);
        graph.getVertices()
                .map((MapFunction<TemporalVertex, String>) temporalVertex -> ""+temporalVertex.getId().toString().substring(13,14)
                        +",("+temporalVertex.getProperties()+")")
                .writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

        env.execute();
        compareResultsByLinesInMemory(expectedResult, resultPath);
    }
}
