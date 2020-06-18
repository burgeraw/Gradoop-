package gellyStreaming.gradoop.model;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.State;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import java.util.HashMap;

public interface Algorithm<T, S> {
    T doAlgorithm(S localState, QueryState QS, Integer localKey, Integer[] allKeys, long from, long maxValidTo) throws Exception;
}



