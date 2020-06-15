package gellyStreaming.gradoop.model;

import org.apache.flink.api.common.state.MapState;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TriangleCountingAlg2 implements Algorithm<Integer, MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>>> {

    @Override
    public Integer doAlgorithm(MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>> localState,
                               QueryState QS, Integer localKey, Integer[] allKeys) {
        if (!QS.isInitilized()) {
            throw new ExceptionInInitializerError("We don't have Queryable State initialized.");
        }
        AtomicInteger triangleCounter = new AtomicInteger(0);
        if (localKey != null) {
            HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>> localState2 = new HashMap<>();
            try {
                for (long timestamp : localState.keys()) {
                    HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>> tempstate = localState.get(timestamp);
                    for(GradoopId srcid : tempstate.keySet()) {
                        if(!localState2.containsKey(srcid)) {
                            localState2.put(srcid, new HashMap<>());
                        }
                        localState2.get(srcid).putAll(tempstate.get(srcid));
                    }
                }
            } catch (Exception e) {
                System.out.println(e);
            }

            for (GradoopId srcId : localState2.keySet()) {
                    GradoopId[] neighbours = localState2.get(srcId).keySet().toArray(GradoopId[]::new);
                    for (int i = 0; i < neighbours.length; i++) {
                        GradoopId neighbour1 = neighbours[i];
                        if (neighbour1.compareTo(srcId) > 0) {
                            for (int j = 0; j < neighbours.length; j++) {
                                GradoopId neighbour2 = neighbours[j];
                                if (i != j && neighbour2.compareTo(neighbour1) > 0) {
                                    AtomicBoolean triangle = new AtomicBoolean(false);
                                    if(localState2.containsKey(neighbour1)) {
                                        if (localState2.get(neighbour1).containsKey(neighbour2)) {
                                            triangle.set(true);
                                        }
                                    }
                                    if(!triangle.get() && localState2.containsKey(neighbour2)) {
                                        if (localState2.get(neighbour2).containsKey(neighbour1)) {
                                            triangle.set(true);
                                        }
                                    }
                                    if(triangle.get()) {
                                        triangleCounter.getAndIncrement();
                                    }
                                }
                            }
                        }
                    }
                }
            }
        System.out.println("We found " + triangleCounter.get() + " triangles.");
        return triangleCounter.get();
    }
}
