package gradoop.flink.streaming.model;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.flink.util.GradoopFlinkConfig;

public interface StreamingBaseGraph<
        G extends GraphHead,
        V extends Vertex,
        E extends Edge,
        LG extends StreamingBaseGraph<G, V, E, LG, GC>,
        GC extends StreamingBaseGraphCollection<G, V, E, LG, GC>
        >
        extends StreamingLogicalGraphLayout<G, V, E>,
        StreamingBaseGraphOperators<G, V, E, LG, GC>
{
    GradoopFlinkConfig getConfig();

    StreamingBaseGraphFactory<G, V, E , LG, GC> getFactory();

    StreamingBaseGraphCollectionFactory<G, V, E, LG, GC> getCollectionFactory();

    default DataStream<Boolean> isEmpty() {
        return getVertices()
                .map()
    }
}
