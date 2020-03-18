package gellyStreaming.model;

import java.io.Serializable;

import org.apache.flink.api.common.functions.Function;

/**
 * Interface to be implemented by the function applied to a vertex neighborhood
 * in the {@link SnapshotStream#foldNeighbors(Object, EdgesFold)} method.
 *
 * @param <K> the vertex ID type
 * @param <EV> the edge value type
 * @param <T> the accumulator type
 */
public interface EdgesFold<K, EV, T> extends Function, Serializable {

    /**
     * Combines two edge values into one value of the same type.
     * The foldEdges function is consecutively applied to all edges of a neighborhood,
     * until only a single value remains.
     *
     * @param accum the initial value and accumulator
     * @param vertexID the vertex ID
     * @param neighborID the neighbor's ID
     * @param edgeValue the edge value
     * @return The data stream that is the result of applying the foldEdges function to the graph window.
     * @throws Exception
     */
    T foldEdges(T accum, K vertexID, K neighborID, EV edgeValue) throws Exception;
}