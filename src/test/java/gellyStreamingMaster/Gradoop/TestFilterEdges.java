package gellyStreamingMaster.Gradoop;


import gellyStreaming.gradoop.model.SimpleTemporalEdgeStream;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.junit.Test;

public class TestFilterEdges extends AbstractTestBase {

    @Test
    public void testWithSimpleFilter() throws Exception {
        /*
         * Test filterEdges() with a simple filter
         */
        final String resultPath = getTempDirPath("result");
        final String expectedResult = "2,3,23\n" +
                "3,4,34\n" +
                "3,5,35\n" +
                "4,5,45\n" +
                "5,1,51\n";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final GradoopIdSet graphId = new GradoopIdSet();
        SimpleTemporalEdgeStream graph = new SimpleTemporalEdgeStream(GraphTemporalStreamTestUtils.getLongLongEdgeDataStream(env, graphId), env, graphId);
        graph.filterEdges(new LowEdgeValueFilter())
                .getEdges()
                .map((MapFunction<TemporalEdge, String>) temporalEdge -> ""+temporalEdge.getSourceId().toString().substring(13,14)+","
                        +temporalEdge.getTargetId().toString().substring(13,14)+","
                        +temporalEdge.getPropertyValue("value").toString())
                .writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
        env.execute();

        compareResultsByLinesInMemory(expectedResult, resultPath);
    }

    @Test
    public void testWithEmptyFilter() throws Exception {
        /*
         * Test filterEdges() with a filter that constantly returns true
         */
        final String resultPath = getTempDirPath("result");
        final String expectedResult = "1,2,12\n" +
                "1,3,13\n" +
                "2,3,23\n" +
                "3,4,34\n" +
                "3,5,35\n" +
                "4,5,45\n" +
                "5,1,51\n";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final GradoopIdSet graphId = new GradoopIdSet();

        SimpleTemporalEdgeStream graph = new SimpleTemporalEdgeStream(GraphTemporalStreamTestUtils.getLongLongEdgeDataStream(env, graphId), env, graphId);
        graph.filterEdges(new EmptyFilter())
                .getEdges()
                .map((MapFunction<TemporalEdge, String>) temporalEdge -> ""+temporalEdge.getSourceId().toString().substring(13,14)+","
                        +temporalEdge.getTargetId().toString().substring(13,14)+","
                        +temporalEdge.getPropertyValue("value").toString())
                .writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
        env.execute();

        compareResultsByLinesInMemory(expectedResult, resultPath);
    }

    @Test
    public void testWithDiscardFilter() throws Exception {
        /*
         * Test filterEdges() with a filter that constantly returns false
         */
        final String resultPath = getTempDirPath("result");
        final String expectedResult = "";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final GradoopIdSet graphId = new GradoopIdSet();

        SimpleTemporalEdgeStream graph = new SimpleTemporalEdgeStream(GraphTemporalStreamTestUtils.getLongLongEdgeDataStream(env, graphId), env, graphId);
        graph.filterEdges(new DiscardFilter())
                .getEdges()
                .map((MapFunction<TemporalEdge, String>) temporalEdge -> ""+temporalEdge.getSourceId().toString().substring(13,14)+","
                        +temporalEdge.getTargetId().toString().substring(13,14)+","
                        +temporalEdge.getPropertyValue("value").toString())
                .writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
        env.execute();

        compareResultsByLinesInMemory(expectedResult, resultPath);
    }

    private static final class LowEdgeValueFilter implements FilterFunction<TemporalEdge> {

        @Override
        public boolean filter(TemporalEdge edge) throws Exception {
            return edge.getPropertyValue("value").getLong() > 20L;
        }
    }

    private static final class EmptyFilter implements FilterFunction<TemporalEdge> {

        @Override
        public boolean filter(TemporalEdge edge) throws Exception {
            return true;
        }
    }

    private static final class DiscardFilter implements FilterFunction<TemporalEdge> {

        @Override
        public boolean filter(TemporalEdge edge) throws Exception {
            return false;
        }
    }
}
