package gellyStreamingMaster.Gradoop;

import gellyStreaming.gradoop.model.SimpleTemporalEdgeStream;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.junit.Test;

public class TestFilterVertices extends AbstractTestBase {

    @Test
    public void testWithSimpleFilter() throws Exception {
        /*
         * Test filterVertices() with a simple filter
         */
        final String resultPath = getTempDirPath("result");
        final String expectedResult = "2,3,23\n" +
                "3,4,34\n" +
                "3,5,35\n" +
                "4,5,45\n";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final GradoopIdSet graphId = new GradoopIdSet();
        SimpleTemporalEdgeStream graph = new SimpleTemporalEdgeStream(GraphTemporalStreamTestUtils.getLongLongEdgeDataStream(env, graphId), env, graphId);
        graph.filterVertices(new LowVertexKeyFilter())
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
         * Test filterVertices() with a filter that constantly returns true
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
        GradoopIdSet graphId = new GradoopIdSet();
        SimpleTemporalEdgeStream graph = new SimpleTemporalEdgeStream(GraphTemporalStreamTestUtils.getLongLongEdgeDataStream(env, graphId), env, graphId);
        graph.filterVertices(new EmptyFilter())
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
         * Test filterVertices() with a filter that constantly returns false
         */
        final String resultPath = getTempDirPath("result");
        final String expectedResult = "";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final GradoopIdSet graphId = new GradoopIdSet();
        SimpleTemporalEdgeStream graph = new SimpleTemporalEdgeStream(GraphTemporalStreamTestUtils.getLongLongEdgeDataStream(env, graphId), env, graphId);
        graph.filterVertices(new DiscardFilter())
                .getEdges()
                .map((MapFunction<TemporalEdge, String>) temporalEdge -> ""+temporalEdge.getSourceId().toString().substring(13,14)+","
                        +temporalEdge.getTargetId().toString().substring(13,14)+","
                        +temporalEdge.getPropertyValue("value").toString())
                .writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
        env.execute();

        compareResultsByLinesInMemory(expectedResult, resultPath);
    }

    private static final class LowVertexKeyFilter implements FilterFunction<TemporalVertex> {

        @Override
        public boolean filter(TemporalVertex vertex) throws Exception {
            return Integer.parseInt(vertex.getId().toString().substring(13,14)) > 1;
        }
    }

    private static final class EmptyFilter implements FilterFunction<TemporalVertex> {

        @Override
        public boolean filter(TemporalVertex vertex) throws Exception {
            return true;
        }
    }

    private static final class DiscardFilter implements FilterFunction<TemporalVertex> {

        @Override
        public boolean filter(TemporalVertex vertex) throws Exception {
            return false;
        }
    }
}
