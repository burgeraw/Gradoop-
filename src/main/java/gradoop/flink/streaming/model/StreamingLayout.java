package gradoop.flink.streaming.model;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.Vertex;

public interface StreamingLayout<
        V extends Vertex,
        E extends Edge> {
    DataStream<V> getVertices();
    DataStream<V> getVerticesByLabel(String label);
    DataStream<E> getEdges();
    DataStream<E> getEdgesByLabel(String label);
}
