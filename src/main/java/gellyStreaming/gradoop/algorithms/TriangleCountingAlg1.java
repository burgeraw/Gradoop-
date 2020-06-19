package gellyStreaming.gradoop.algorithms;

import gellyStreaming.gradoop.model.Algorithm;
import gellyStreaming.gradoop.model.GradoopIdUtil;
import gellyStreaming.gradoop.model.QueryState;
import org.apache.flink.api.common.state.MapState;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TriangleCountingAlg1 implements Algorithm<String, MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>>> {

    @Override
    public String doAlgorithm(MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>> localState,
                              QueryState QS, Integer localKey, Integer[] allKeys, long from, long maxValidTo) throws Exception {
        if (!QS.isInitilized()) {
            throw new Exception("We don't have Queryable State initialized.");
        }

        HashSet<GradoopId> relevantVertexIds = new HashSet<>();

        for(int key : allKeys) {
            if(key != localKey) {
                try {
                    HashSet<GradoopId> allIds = QS.getALVertexListFromTo(key, from, maxValidTo);
                    for(GradoopId id : allIds) {
                        if(GradoopIdUtil.getModulo(id, localKey, allKeys)) {
                            relevantVertexIds.add(id);
                        }
                    }
                } catch (Exception e) {
                    System.out.println(e);
                }
            }
        }

        HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>> localAdjacencyList = new HashMap<>();
        for(long timestamp: localState.keys()) {
            for(GradoopId src : localState.get(timestamp).keySet()) {
                if(GradoopIdUtil.getModulo(src, localKey, allKeys)) {
                    relevantVertexIds.add(src);
                    if (!localAdjacencyList.containsKey(src)) {
                        localAdjacencyList.put(src, new HashMap<>());
                    }
                    localAdjacencyList.get(src).putAll(localState.get(timestamp).get(src));
                }
            }
        }
        for(int key : allKeys) {
            if(key != localKey) {
                int tries = 0;
                int maxTries = 10;
                while(tries < maxTries) {
                    try {
                        GradoopId[] vertices = relevantVertexIds.toArray(GradoopId[]::new);
                        HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>> tempState =
                                QS.getALVerticesFromTo(key, vertices, from, maxValidTo);
                        for(GradoopId src : tempState.keySet()) {
                            if (!localAdjacencyList.containsKey(src)) {
                                localAdjacencyList.put(src, new HashMap<>());
                            }
                            localAdjacencyList.get(src).putAll(tempState.get(src));
                        }
                        tries = maxTries;
                    } catch (Exception e) {
                        tries++;
                        if (tries >= maxTries) {
                            throw new Exception("We tried to get state " + maxTries + " times, but failed. ");
                        }
                    }
                }
            }
        }
        AtomicInteger triangleCount = new AtomicInteger(0);
        for (GradoopId srcId : localAdjacencyList.keySet()) {
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
                                // Not necessary since it should've been saved in both directions, but
                                // can be considered as a making sure check.
                                if (!triangle.get() && localAdjacencyList.containsKey(neighbour2)) {
                                    if (localAdjacencyList.get(neighbour2).containsKey(neighbour1)) {
                                        triangle.set(true);
                                    }
                                }
                                int counter = 0;
                                while(!triangle.get() && counter < allKeys.length) {
                                    if(!allKeys[counter].equals(localKey)) {
                                        if(QS.ALcontainsEdgeFromTo(allKeys[counter], neighbour1, neighbour2, from, maxValidTo)) {
                                            triangle.set(true);
                                        }
                                    }
                                    counter++;
                                }

                                if (triangle.get()) {
                                    triangleCount.getAndIncrement();
                                }
                            }
                        }
                    }
                }
        }
        return "In partition "+localKey+" we found "+triangleCount.get()+" triangles ";
    }
}