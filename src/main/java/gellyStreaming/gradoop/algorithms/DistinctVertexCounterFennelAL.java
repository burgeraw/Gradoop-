package gellyStreaming.gradoop.algorithms;

import gellyStreaming.gradoop.model.QueryState;
import gellyStreaming.gradoop.partitioner.FennelPartitioning;
import org.apache.flink.api.common.state.MapState;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import java.util.HashMap;
import java.util.HashSet;

public class DistinctVertexCounterFennelAL implements Algorithm<String, MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>>> {

    // Granularity of retrieval: only getting a boolean if the edge exists from remote partition.
    private final FennelPartitioning fennel;

    public DistinctVertexCounterFennelAL(FennelPartitioning fennel) {
        this.fennel = fennel;
        if (fennel == null) {
            throw new InstantiationError("Fennel vertex partitioning hasn't been instantiated.");
        }
    }

    @Override
    public String doAlgorithm(MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>> localState,
                              QueryState QS, Integer localKey, Integer[] allKeys, long from, long maxValidTo)
            throws Exception {
        if (!QS.isInitilized()) {
            throw new Exception("We don't have Queryable State initialized.");
        }
        HashSet<GradoopId> allVertices = new HashSet<>();
        for(Long timestamp : localState.keys()) {
            allVertices.addAll(localState.get(timestamp).keySet());
            //for(GradoopId id : localState.get(timestamp).keySet()) {
            //    allVertices.addAll(localState.get(timestamp).get(id).keySet());
            //    allVertices.add(id);
            //}
        }
        return "In partition "+localKey+" we found "+allVertices.size()+" distinct source vertices.";
    }
}
