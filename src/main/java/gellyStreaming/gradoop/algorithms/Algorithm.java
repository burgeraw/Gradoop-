package gellyStreaming.gradoop.algorithms;

import gellyStreaming.gradoop.model.QueryState;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.State;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import scala.Int;

import java.io.Serializable;
import java.util.HashMap;

public interface Algorithm<T, S> extends Serializable {
    T doAlgorithm(S localState, QueryState QS, Integer localKey, Integer[] allKeys, long from, long maxValidTo) throws InterruptedException;
}



