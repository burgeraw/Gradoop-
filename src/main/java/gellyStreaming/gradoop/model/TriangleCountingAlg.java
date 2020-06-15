package gellyStreaming.gradoop.model;

import org.apache.flink.api.common.state.MapState;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TriangleCountingAlg implements Algorithm<Integer, MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>>>{
    @Override
    public Integer doAlgorithm(MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>> localState,
                               QueryState QS, Integer localKey, Integer[] allKeys) throws InstantiationException {
        if (!QS.isInitilized()) {
            throw new ExceptionInInitializerError("We don't have Queryable State initialized.");
        }
        AtomicInteger triangleCounter = new AtomicInteger(0);
        if (localKey != null) {

            //MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>> localState2 =
            //        (MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>>)localState;

            MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>>[] remoteStates =
                    new MapState[allKeys.length - 1];
            int tries = 0;
            int maxTries = 10;
            int counter = 0;
            for (int key : allKeys) {
                tries = 0;
                while (tries < maxTries && key != localKey) {
                    try {
                        MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>> tempState =
                                QS.getALState(key);
                        remoteStates[counter] = tempState;
                        tries = maxTries;
                        counter++;
                    } catch (Exception e) {
                        tries++;
                        if (tries >= maxTries) {
                            throw new InstantiationException("We tried to get state " + maxTries + " times, but failed. ");
                        }
                    }
                }
            }
            System.out.println("We got all states & now start counting triangles.");
            //System.out.println("Size remote states combined: "+combinedRemoteALs.entrySet().size());
            try {
                HashSet<GradoopId> allVertexIds = new HashSet<>();
                for (long timestamp : localState.keys()) {
                    allVertexIds.addAll(localState.get(timestamp).keySet());
                }
            /*
            for(MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>> state : remoteStates) {
                for(long timestamp : state.keys()) {
                    allVertexIds.addAll(state.get(timestamp).keySet());
                }
            }

             */
                System.out.println("We found " + allVertexIds.size() + " srcVertexIds");
                for (GradoopId srcId : allVertexIds) {
                    Set<GradoopId> neighboursSet = new HashSet<>();
                    for (long timestamp : localState.keys()) {
                        try {
                            neighboursSet.addAll(localState.get(timestamp).get(srcId).keySet());
                        } catch (NullPointerException ignored) {}
                    }
                    //  localAdjacencyList.get(srcId).keySet();
                    //if(combinedRemoteALs.containsKey(srcId)) {
                    //    neighboursSet.addAll(combinedRemoteALs.get(srcId).keySet());
                    // }
                    GradoopId[] neighbours = neighboursSet.toArray(GradoopId[]::new);
                    for (int i = 0; i < neighbours.length; i++) {
                        GradoopId neighbour1 = neighbours[i];
                        if (neighbour1.compareTo(srcId) > 0) {
                            for (int j = 0; j < neighbours.length; j++) {
                                GradoopId neighbour2 = neighbours[j];
                                if (i != j && neighbour2.compareTo(neighbour1) > 0) {
                                    AtomicBoolean triangle = new AtomicBoolean(false);
                                    for (long timestamp : localState.keys()) {
                                        if (localState.get(timestamp).containsKey(neighbour1)) {
                                            if (localState.get(timestamp).get(neighbour1).containsKey(neighbour2)) {
                                                triangle.set(true);
                                                break;
                                            }
                                        }
                                        if (localState.get(timestamp).containsKey(neighbour2)) {
                                            if (localState.get(timestamp).get(neighbour2).containsKey(neighbour1)) {
                                                triangle.set(true);
                                                break;
                                            }
                                        }
                                    }
                                    //if (localAdjacencyList.containsKey(neighbour1)) {
                                    //    triangle.set(localAdjacencyList.get(neighbour1).containsKey(neighbour2));
                                    //}
                                    //if (!triangle.get() && localAdjacencyList.containsKey(neighbour2)) {
                                    //    triangle.set(localAdjacencyList.get(neighbour2).containsKey(neighbour1));
                                    //}
                                    //if (!triangle && combinedRemoteALs.containsKey(neighbour1)) {
                                    //    triangle = combinedRemoteALs.get(neighbour1).containsKey(neighbour2);
                                    //}
                                    //if (!triangle && combinedRemoteALs.containsKey(neighbour2)) {
                                    //    triangle = combinedRemoteALs.get(neighbour2).containsKey(neighbour1);
                                    //}
                                    if (triangle.get()) {
                                        triangleCounter.getAndIncrement();
                                    }
                                }
                            }
                        }
                    }
                }
                System.out.println("We found " + triangleCounter.get() + " triangles.");
            } catch (Exception en) {
                System.out.println(en);
            }
        }
        return triangleCounter.get();
    }
}
