package gellyStreamingMaster.Gradoop;


import gellyStreaming.gradoop.model.SimpleTemporalEdgeStream;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.junit.Test;

import java.util.List;

public class TestDistinct extends AbstractTestBase {

    @Test
    public void test() throws Exception {

        final String expectedResult = "1,2,12\n" +
                "1,3,13\n" +
                "2,3,23\n" +
                "3,4,34\n" +
                "3,5,35\n" +
                "4,5,45\n" +
                "5,1,51\n";

        final String resultPath = getTempDirPath("result");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final GradoopIdSet graphId = new GradoopIdSet();
        List<TemporalEdge> edges = GraphTemporalStreamTestUtils.getLongLongEdges(graphId);
        edges.addAll(GraphTemporalStreamTestUtils.getLongLongEdges(graphId));

        SimpleTemporalEdgeStream graph =
                new SimpleTemporalEdgeStream(env.fromCollection(edges), env, graphId);

        graph.distinct().getEdges()
                .map((MapFunction<TemporalEdge, String>) temporalEdge -> ""+temporalEdge.getSourceId().toString().substring(13,14)+","
                        +temporalEdge.getTargetId().toString().substring(13,14)+","
                        +temporalEdge.getPropertyValue("value").toString())
                .writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

        env.execute();

        compareResultsByLinesInMemory(expectedResult, resultPath);
    }
}
