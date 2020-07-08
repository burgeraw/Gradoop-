package gellyStreaming.gradoop.algorithms;

import gellyStreaming.gradoop.util.GradoopIdUtil;
import gellyStreaming.gradoop.model.QueryState;
import org.apache.flink.api.common.state.MapState;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


// Working triangle counting alg for both edge & fennel partitioner, which duplicates entire state.
public class TriangleCountingALRetrieveAllState implements Algorithm<String, MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>>> {

    @Override
    public String doAlgorithm(MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>> localState,
                               QueryState QS, Integer localKey, Integer[] allKeys, long from, long maxValidTo) throws Exception {
        if (!QS.isInitilized()) {
            throw new Exception("We don't have Queryable State initialized.");
        }
        HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>> localAdjacencyList = new HashMap<>();
        if(localState == null) {
            localState = QS.getALState(localKey);
        }
        for(long timestamp: localState.keys()) {
            if(timestamp <= maxValidTo && timestamp>= from) {
                for (GradoopId src : localState.get(timestamp).keySet()) {
                    //if(GradoopIdUtil.getModulo(src, localKey, allKeys)) {
                    if (!localAdjacencyList.containsKey(src)) {
                        localAdjacencyList.put(src, new HashMap<>());
                    }
                    localAdjacencyList.get(src).putAll(localState.get(timestamp).get(src));
                    //}
                }
            }
        }
        for(int key : allKeys) {
            if(key != localKey) {
                int tries = 0;
                int maxTries = 10;
                while(tries < maxTries) {
                    try {
                        MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>> tempState =
                                QS.getALState(key);
                        for (Long timestamp : tempState.keys()) {
                            if (timestamp <= maxValidTo && timestamp >= from) {
                                for(GradoopId src : tempState.get(timestamp).keySet()) {
                                    if (!localAdjacencyList.containsKey(src)) {
                                        localAdjacencyList.put(src, new HashMap<>());
                                    }
                                    localAdjacencyList.get(src).putAll(tempState.get(timestamp).get(src));
                                }
                            }
                        }
                        tries = maxTries;
                    } catch (Exception e) {
                        tries++;
                        Thread.sleep(10);
                        if (tries >= maxTries) {
                            throw new Exception("We tried to get state " + maxTries + " times, but failed. "+e);
                        }
                    }
                }
            }
        }
        AtomicInteger triangleCount = new AtomicInteger(0);
        for (GradoopId srcId : localAdjacencyList.keySet()) {
            if(GradoopIdUtil.getModulo(srcId, localKey, allKeys)) {
                Set<GradoopId> neighboursSet = localAdjacencyList.get(srcId).keySet();
                GradoopId[] neighbours = neighboursSet.toArray(GradoopId[]::new);
                for (int i = 0; i < neighbours.length; i++) {
                    GradoopId neighbour1 = neighbours[i];
                    if (neighbour1.compareTo(srcId) > 0) {
                        for (int j = 0; j < neighbours.length; j++) {
                            GradoopId neighbour2 = neighbours[j];
                            if (i != j && neighbour2.compareTo(neighbour1) > 0) {
                                AtomicBoolean triangle = new AtomicBoolean(false);
                                if (localAdjacencyList.containsKey(neighbour1)) {
                                    if (localAdjacencyList.get(neighbour1).containsKey(neighbour2)) {
                                        triangle.set(true);
                                    }
                                }
                                if (!triangle.get() && localAdjacencyList.containsKey(neighbour2)) {
                                    if (localAdjacencyList.get(neighbour2).containsKey(neighbour1)) {
                                        triangle.set(true);
                                    }
                                }
                                if (triangle.get()) {
                                    triangleCount.getAndIncrement();
                                }
                            }
                        }
                    }
                }
            }
        }
        return "In partition "+localKey+" we found "+triangleCount.get()+" triangles ";
    }
}