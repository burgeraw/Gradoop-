package gellyStreaming.gradoop.model;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * The super-class of all graph stream types.
 *
 * @param <K> the vertex ID type
 * @param <VV> the vertex value type
 * @param <EV> the edge value type
 */
public abstract class GraphStream<K, VV, EV> {

    /**
     * @return the Flink streaming execution environment.
     */
    public abstract StreamExecutionEnvironment getContext();

    /**
     * @return the vertex DataStream.
     */
    public abstract DataStream<Vertex<K, VV>> getVertices();

    /**
     * @return the edge DataStream.
     */
    public abstract DataStream<Edge<K, EV>> getEdges();
}