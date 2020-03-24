package gellyStreamingMaster;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class GraphStreamTestUtils {

    public static final DataStream<Vertex<Long, Long>> getLongLongVertexDataStream(StreamExecutionEnvironment env) {
        return env.fromCollection(getLongLongVertices());
    }

    public static final DataStream<Edge<Long, Long>> getLongLongEdgeDataStream(StreamExecutionEnvironment env) {
        return env.fromCollection(getLongLongEdges());
    }

    /**
     * @return a List of sample Vertex data.
     */
    private static final List<Vertex<Long, Long>> getLongLongVertices() {
        List<Vertex<Long, Long>> vertices = new ArrayList<>();
        vertices.add(new Vertex<>(1L, 1L));
        vertices.add(new Vertex<>(2L, 2L));
        vertices.add(new Vertex<>(3L, 3L));
        vertices.add(new Vertex<>(4L, 4L));
        vertices.add(new Vertex<>(5L, 5L));

        return vertices;
    }

    /**
     * @return a List of sample Edge data.
     */
    public static final List<Edge<Long, Long>> getLongLongEdges() {
        List<Edge<Long, Long>> edges = new ArrayList<>();
        edges.add(new Edge<>(1L, 2L, 12L));
        edges.add(new Edge<>(1L, 3L, 13L));
        edges.add(new Edge<>(2L, 3L, 23L));
        edges.add(new Edge<>(3L, 4L, 34L));
        edges.add(new Edge<>(3L, 5L, 35L));
        edges.add(new Edge<>(4L, 5L, 45L));
        edges.add(new Edge<>(5L, 1L, 51L));

        return edges;
    }
}
