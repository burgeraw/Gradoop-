package gellyStreamingMaster.Gradoop;

import gellyStreaming.gradoop.model.GradoopGraphStream;
import gellyStreaming.gradoop.model.SimpleTemporalEdgeStream;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.temporal.model.impl.operators.tostring.TemporalVertexToDataString;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.junit.Test;
import org.apache.flink.test.util.AbstractTestBase;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

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
        GradoopIdSet graphId = new GradoopIdSet();
        SimpleTemporalEdgeStream graph = new SimpleTemporalEdgeStream(GraphTemporalStreamTestUtils.getLongLongEdgeDataStream(env,graphId), env, graphId);
        DataStream<Tuple2<String, String>> degrees = graph.getDegrees().map(
                new MapFunction<TemporalVertex, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(TemporalVertex temporalVertex) throws Exception {
                        String id = temporalVertex.getId().toString();
                        String idShort = id.substring(13,14);
                        return Tuple2.of(idShort, temporalVertex.getPropertyValue("degree").toString());
                    }
                });
        degrees.writeAsCsv(resultPath, FileSystem.WriteMode.OVERWRITE);
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
        GradoopIdSet graphId = new GradoopIdSet();
        SimpleTemporalEdgeStream graph = new SimpleTemporalEdgeStream(GraphTemporalStreamTestUtils.getLongLongEdgeDataStream(env,graphId), env, graphId);
        DataStream<Tuple2<String, String>> degrees = graph.getInDegrees().map(
                new MapFunction<TemporalVertex, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(TemporalVertex temporalVertex) throws Exception {
                        String id = temporalVertex.getId().toString();
                        String idShort = id.substring(13,14);
                        return Tuple2.of(idShort, temporalVertex.getPropertyValue("indegree").toString());
                    }
                });
        degrees.writeAsCsv(resultPath, FileSystem.WriteMode.OVERWRITE);
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
        GradoopIdSet graphId = new GradoopIdSet();
        SimpleTemporalEdgeStream graph = new SimpleTemporalEdgeStream(GraphTemporalStreamTestUtils.getLongLongEdgeDataStream(env,graphId), env, graphId);
        DataStream<Tuple2<String, String>> degrees = graph.getOutDegrees().map(
                new MapFunction<TemporalVertex, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(TemporalVertex temporalVertex) throws Exception {
                        String id = temporalVertex.getId().toString();
                        String idShort = id.substring(13,14);
                        return Tuple2.of(idShort, temporalVertex.getPropertyValue("outdegree").toString());
                    }
                });
        degrees.writeAsCsv(resultPath, FileSystem.WriteMode.OVERWRITE);
        env.execute();

        compareResultsByLinesInMemory(expectedResult, resultPath);
    }
}