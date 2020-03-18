package gradoop.flink.streaming.model;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;

public interface StreamingLogicalGraphLayout<
        G extends GraphHead,
        V extends Vertex,
        E extends Edge> extends  StreamingLayout<V, E>{

    boolean isGVELayout();

    boolean isIndexedGVELayout();

    DataStream<G> getGraphHead();
}
