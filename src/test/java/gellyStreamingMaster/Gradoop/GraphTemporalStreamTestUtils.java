package gellyStreamingMaster.Gradoop;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class GraphTemporalStreamTestUtils {

    public static DataStream<TemporalVertex> getLongLongVertexDataStream(StreamExecutionEnvironment env) {
        return env.fromCollection(getLongLongVertices());
    }

    public static final DataStream<TemporalEdge> getLongLongEdgeDataStream(StreamExecutionEnvironment env, GradoopIdSet graphId) {
        return env.fromCollection(getLongLongEdges(graphId));
    }

    /**
     * @return a List of sample Vertex data.
     */
    private static final List<TemporalVertex> getLongLongVertices() {
        List<TemporalVertex> vertices = new ArrayList<>();
        Properties prop = new Properties();
        prop.set("value", 10);
        GradoopIdSet graphId = new GradoopIdSet();
        vertices.add(new TemporalVertex(
                new GradoopId(),
                null,
                prop,
                graphId,
                875806672L,
                9223372036854775807L));
        prop.set("value", 20);
        vertices.add(new TemporalVertex(
                new GradoopId(),
                null,
                prop,
                graphId,
                875806673L,
                9223372036854775807L));
        prop.set("value", 30);
        vertices.add(new TemporalVertex(
                new GradoopId(),
                null,
                prop,
                graphId,
                875806674L,
                9223372036854775807L));
        prop.set("value", 40);
        vertices.add(new TemporalVertex(
                new GradoopId(),
                null,
                prop,
                graphId,
                875806675L,
                9223372036854775807L));
        prop.set("value", 50);
        vertices.add(new TemporalVertex(
                new GradoopId(),
                null,
                prop,
                graphId,
                875806676L,
                9223372036854775807L));
        return vertices;
    }

    /**
     * @return a List of sample Edge data.
     *         edges.add(new Edge<>(1L, 2L, 12L));
     *         edges.add(new Edge<>(1L, 3L, 13L));
     *         edges.add(new Edge<>(2L, 3L, 23L));
     *         edges.add(new Edge<>(3L, 4L, 34L));
     *         edges.add(new Edge<>(3L, 5L, 35L));
     *         edges.add(new Edge<>(4L, 5L, 45L));
     *         edges.add(new Edge<>(5L, 1L, 51L));
     */
    public static final List<TemporalEdge> getLongLongEdges(GradoopIdSet graphIds) {
        List<TemporalEdge> edges = new ArrayList<>();
        Properties prop = new Properties();
        prop.set("value", 12L);
        edges.add(new TemporalEdge(
                new GradoopId(0,1,(short)0,0),
                null,
                new GradoopId(1,1,(short)0,0),
                new GradoopId(1,2,(short)0,0),
                prop,
                graphIds,
                800000000L,
                9223372036854775807L));
        prop.set("value", 13L);
        edges.add(new TemporalEdge(
                new GradoopId(0,1,(short)0,0),
                null,
                new GradoopId(1,1,(short)0,0),
                new GradoopId(1,3,(short)0,0),
                prop,
                graphIds,
                800000002L,
                9223372036854775807L));
        prop.set("value", 23L);
        edges.add(new TemporalEdge(
                new GradoopId(0,1,(short)0,0),
                null,
                new GradoopId(1,2,(short)0,0),
                new GradoopId(1,3,(short)0,0),
                prop,
                graphIds,
                800000004L,
                9223372036854775807L));
        prop.set("value", 34L);
        edges.add(new TemporalEdge(
                new GradoopId(0,1,(short)0,0),
                null,
                new GradoopId(1,3,(short)0,0),
                new GradoopId(1,4,(short)0,0),
                prop,
                graphIds,
                800000006L,
                9223372036854775807L));
        prop.set("value", 35L);
        edges.add(new TemporalEdge(
                new GradoopId(0,1,(short)0,0),
                null,
                new GradoopId(1,3,(short)0,0),
                new GradoopId(1,5,(short)0,0),
                prop,
                graphIds,
                800000008L,
                9223372036854775807L));
        prop.set("value", 45L);
        edges.add(new TemporalEdge(
                new GradoopId(0,1,(short)0,0),
                null,
                new GradoopId(1,4,(short)0,0),
                new GradoopId(1,5,(short)0,0),
                prop,
                graphIds,
                800000010L,
                9223372036854775807L));
        prop.set("value", 51L);
        edges.add(new TemporalEdge(
                new GradoopId(0,1,(short)0,0),
                null,
                new GradoopId(1,5,(short)0,0),
                new GradoopId(1,1,(short)0,0),
                prop,
                graphIds,
                800000012L,
                9223372036854775807L));
        return edges;
    }
}
