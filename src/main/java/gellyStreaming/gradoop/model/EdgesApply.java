package gellyStreaming.gradoop.model;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * Interface to be implemented by the function applied to a vertex neighborhood
 * in the {@link SnapshotStream#applyOnNeighbors(EdgesApply)} method.
 *
 * @param <K> the vertex ID type
 * @param <EV> the edge value type
 * @param <T> the accumulator type
 */
public interface EdgesApply<K, EV, T> extends Function, Serializable {

    /**
     * Computes a custom function on the neighborhood of a vertex.
     * The vertex can output zero, one or more result values.
     *
     * @param vertexID the vertex ID
     * @param neighbors the neighbors of this vertex. The first field of the tuple contains
     * the neighbor ID and the second field contains the edge value.
     * @param out the collector to emit the result
     * @throws Exception
     */
    void applyOnEdges(K vertexID, Iterable<Tuple2<K, EV>> neighbors, Collector<T> out) throws Exception;
}