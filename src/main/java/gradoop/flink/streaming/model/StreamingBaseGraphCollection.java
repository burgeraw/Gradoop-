package gradoop.flink.streaming.model;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

public interface StreamingBaseGraphCollection <
        G extends GraphHead,
        V extends Vertex,
        E extends Edge,
        LG extends StreamingBaseGraph<G, V, E, LG, GC>,
        GC extends StreamingBaseGraphCollection<G, V, E, LG, GC>>
        extends StreamingGraphCollectionLayout<G, V, E>, StreamingBaseGraphCollectionOperators<G, V, E, LG, GC> {
    /**
     * Returns the Gradoop Flink configuration.
     *
     * @return the Gradoop Flink configuration
     */
    GradoopFlinkConfig getConfig();

    /**
     * Get the factory that is responsible for creating an instance of {@link GC}.
     *
     * @return a factory that can be used to create a {@link GC} instance
     */
    StreamingBaseGraphCollectionFactory<G, V, E, LG, GC> getFactory();

    /**
     * Get the factory that is responsible for creating an instance of {@link LG}.
     *
     * @return a factory that can be used to create a {@link LG} instance
     */
    StreamingBaseGraphFactory<G, V, E, LG, GC> getGraphFactory();

    //----------------------------------------------------------------------------
    // Base Graph / Graph Head Getters
    //----------------------------------------------------------------------------

    @Override
    default LG getGraph(final GradoopId graphID) {
        // filter vertices and edges based on given graph id
        DataStream<G> graphHead = getGraphHeads()
                .filter(new BySameId<>(graphID));
        DataStream<V> vertices = getVertices()
                .filter(new InGraph<>(graphID));
        DataStream<E> edges = getEdges()
                .filter(new InGraph<>(graphID));

        return getGraphFactory().fromDataSets(graphHead, vertices, edges);
    }

    @Override
    default GC getGraphs(final GradoopIdSet identifiers) {
        DataSet<G> newGraphHeads = this.getGraphHeads()
                .filter((FilterFunction<G>) graphHead -> identifiers.contains(graphHead.getId()));

        // build new vertex set
        DataSet<V> vertices = getVertices()
                .filter(new InAnyGraph<>(identifiers));

        // build new edge set
        DataSet<E> edges = getEdges()
                .filter(new InAnyGraph<>(identifiers));

        return getFactory().fromDataSets(newGraphHeads, vertices, edges);
    }

    //----------------------------------------------------------------------------
    // Utility methods
    //----------------------------------------------------------------------------

    @Override
    default DataStream<Boolean> isEmpty() {
        return getGraphHeads()
                .map(new True<>())
                .distinct()
                .union(getConfig().getExecutionEnvironment().fromElements(false))
                .reduce(new Or())
                .map(new Not());
    }
}