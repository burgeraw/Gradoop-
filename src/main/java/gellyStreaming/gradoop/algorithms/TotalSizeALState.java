package gellyStreaming.gradoop.algorithms;

import gellyStreaming.gradoop.model.QueryState;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.MapState;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class TotalSizeALState implements Algorithm<String, MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>>> {

    @Override
    public String doAlgorithm(MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>> localState, QueryState QS, Integer localKey, Integer[] allKeys, long from, long maxValidTo) {
        AtomicInteger counter = new AtomicInteger(0);
        try {
            for(Long timestamp: localState.keys()) {
                for(GradoopId id : localState.get(timestamp).keySet()) {
                    counter.getAndAdd(localState.get(timestamp).get(id).keySet().size());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return "In partition "+localKey+" the local state has "+counter.get()+" edges.";
    }

    @Override
    public String doAlgorithm(MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>> localState, JobID jobID, Integer localKey, Integer[] allKeys, long from, long maxValidTo) throws InterruptedException {
        return null;
    }
}
