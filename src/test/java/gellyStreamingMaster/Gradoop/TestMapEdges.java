package gellyStreamingMaster.Gradoop;

import gellyStreaming.gradoop.model.SimpleTemporalEdgeStream;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.junit.Test;

public class TestMapEdges extends AbstractTestBase {

    @Test
    public void testWithSameType() throws Exception {
        /*
         * Test mapEdges() keeping the same edge types
         */
        final String resultPath = getTempDirPath("result");
        final String expectedResult = "1,2,13\n" +
                "1,3,14\n" +
                "2,3,24\n" +
                "3,4,35\n" +
                "3,5,36\n" +
                "4,5,46\n" +
                "5,1,52\n";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final GradoopIdSet graphId = new GradoopIdSet();
        SimpleTemporalEdgeStream graph = new SimpleTemporalEdgeStream(GraphTemporalStreamTestUtils.getLongLongEdgeDataStream(env, graphId), env, graphId);
        graph.mapEdges(new AddOneMapper())
                .getEdges()
                .map((MapFunction<TemporalEdge, String>) temporalEdge -> ""+temporalEdge.getSourceId().toString().substring(13,14)+","
                        +temporalEdge.getTargetId().toString().substring(13,14)+","
                        +temporalEdge.getPropertyValue("value").toString())
                .writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
        env.execute();
        compareResultsByLinesInMemory(expectedResult, resultPath);
    }

    @Test
    public void testWithTupleType() throws Exception {
        /*
         * Test mapEdges() converting the edge value type to tuple
         */
        final String resultPath = getTempDirPath("result");
        final String expectedResult = "1,2,(12,13)\n" +
                "1,3,(13,14)\n" +
                "2,3,(23,24)\n" +
                "3,4,(34,35)\n" +
                "3,5,(35,36)\n" +
                "4,5,(45,46)\n" +
                "5,1,(51,52)\n";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final GradoopIdSet graphId = new GradoopIdSet();
        SimpleTemporalEdgeStream graph = new SimpleTemporalEdgeStream(GraphTemporalStreamTestUtils.getLongLongEdgeDataStream(env, graphId), env, graphId);
        graph.mapEdges(new ToTuple2Mapper())
                .getEdges()
                .map((MapFunction<TemporalEdge, String>) temporalEdge -> ""+temporalEdge.getSourceId().toString().substring(13,14)+","
                        +temporalEdge.getTargetId().toString().substring(13,14)+","
                        +temporalEdge.getPropertyValue("value").toString())
                .writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
        env.execute();
        compareResultsByLinesInMemory(expectedResult, resultPath);
    }

    @Test
    public void testChainedMaps() throws Exception {
        /*
         * Test mapEdges() where two maps are chained together
         */
        final String resultPath = getTempDirPath("result");
        final String expectedResult = "1,2,(13,14)\n" +
                "1,3,(14,15)\n" +
                "2,3,(24,25)\n" +
                "3,4,(35,36)\n" +
                "3,5,(36,37)\n" +
                "4,5,(46,47)\n" +
                "5,1,(52,53)\n";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final GradoopIdSet graphId = new GradoopIdSet();
        SimpleTemporalEdgeStream graph = new SimpleTemporalEdgeStream(GraphTemporalStreamTestUtils.getLongLongEdgeDataStream(env, graphId), env, graphId);
        graph.mapEdges(new AddOneMapper())
                .mapEdges(new ToTuple2Mapper())
                .getEdges()
                .map((MapFunction<TemporalEdge, String>) temporalEdge -> ""+temporalEdge.getSourceId().toString().substring(13,14)+","
                        +temporalEdge.getTargetId().toString().substring(13,14)+","
                        +temporalEdge.getPropertyValue("value").toString())
                .writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
        env.execute();
        compareResultsByLinesInMemory(expectedResult, resultPath);
    }

    private static final class AddOneMapper implements MapFunction<TemporalEdge, TemporalEdge> {
        @Override
        public TemporalEdge map(TemporalEdge edge) throws Exception {
            edge.setProperty("value", edge.getPropertyValue("value").getLong() + 1L);
            return edge;
        }
    }

    private static final class ToTuple2Mapper implements MapFunction<TemporalEdge, TemporalEdge> {
        @Override
        public TemporalEdge map(TemporalEdge edge) throws Exception {
            edge.setProperty("value", "("+edge.getPropertyValue("value").toString()+","+(edge.getPropertyValue("value").getLong()+1L)+")");
            return edge;
        }
    }
}
