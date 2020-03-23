package gellyStreaming.gradoop.model;

import java.io.Serializable;
import org.apache.flink.api.common.functions.Function;

/**
 * Interface to be implemented by the function applied to a vertex neighborhood
 * in the {@link SnapshotStream#reduceOnEdges(EdgesReduce)} method.
 *
 * @param <EV> the edge value type
 */
public interface EdgesReduce<EV> extends Function, Serializable {

    /**
     * Combines two edge values into one value of the same type.
     * The reduceEdges function is consecutively applied to all pairs of edges of a neighborhood,
     * until only a single value remains.
     *
     * @param firstEdgeValue the value of the first edge
     * @param secondEdgeValue the value of the second edge
     * @return The data stream that is the result of applying the reduceEdges function to the graph window.
     * @throws Exception
     */
    EV reduceEdges(EV firstEdgeValue, EV secondEdgeValue) throws Exception;
}
