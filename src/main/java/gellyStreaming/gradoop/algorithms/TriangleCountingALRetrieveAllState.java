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
import java.util.concurrent.atomic.AtomicLong;


// Working triangle counting alg for both edge & fennel partitioner, which retrieves entire remote states.
public class TriangleCountingALRetrieveAllState implements Algorithm<String, MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>>> {

    @Override
    public String doAlgorithm(MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>> localState,
                               QueryState QS, Integer localKey, Integer[] allKeys, long from, long maxValidTo) throws Exception {
        if (!QS.isInitilized()) {
            throw new Exception("We don't have Queryable State initialized.");
        }
        AtomicLong QStimer = new AtomicLong(0);
        HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>> localAdjacencyList = new HashMap<>();
        // First retrieve local state.
        if(localState == null) {
            int tries = 0;
            while(tries < 10) {
                try {
                    long start = System.currentTimeMillis();
                    localState = QS.getALState(localKey);
                    long stop = System.currentTimeMillis();
                    QStimer.getAndAdd((stop - start));
                    break;
                } catch(Exception e) {
                    tries++;
                    if(tries>= 10) {
                        System.out.println("Error retrieving state. " + e);
                    }
                }
            }

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
        // Get rest of states.
        for(int key : allKeys) {
            if(key != localKey) {
                int tries = 0;
                int maxTries = 10;
                while(tries < maxTries) {
                    try {
                        long start = System.currentTimeMillis();
                        MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>> tempState =
                                QS.getALState(key);
                        long stop = System.currentTimeMillis();
                        QStimer.getAndAdd((stop-start));
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
        // Iterate over vertex IDs and all pairs of its neighbours (which forms a wedge) and check if these
        // two neighbours are connected with an egde, which makes it a triangle.
        // Only count the triangles if the srcId < neighbour1 < neighbour2 to avoid duplicates.
        // Only start with vertices what, when used Modulo, are allocated to this partition.
        // This so each partition only check a non-overlapping part of the potential triangles.
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
        System.out.println("Time spend on QS in partition \t"+localKey+"\t:\t"+QStimer.get());
        return "In partition "+localKey+" we found "+triangleCount.get()+" triangles ";
    }
}