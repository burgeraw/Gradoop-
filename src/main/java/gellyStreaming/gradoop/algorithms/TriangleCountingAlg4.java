package gellyStreaming.gradoop.algorithms;

import gellyStreaming.gradoop.model.Algorithm;
import gellyStreaming.gradoop.model.GradoopIdUtil;
import gellyStreaming.gradoop.model.QueryState;
import gellyStreaming.gradoop.partitioner.FennelPartitioning;
import org.apache.flink.api.common.state.MapState;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TriangleCountingAlg4 implements Algorithm<String, MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>>> {

    private final FennelPartitioning fennel;
    private final HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>> cache = new HashMap<>();

    public TriangleCountingAlg4(FennelPartitioning fennel) {
        if(fennel == null) {
            throw new InstantiationError("Fennel vertex partitioning hasn't been instantiated.");
        }
        this.fennel = fennel;
    }

    @Override
    public String doAlgorithm(MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>> localState,
                              QueryState QS, Integer localKey, Integer[] allKeys, long from, long maxValidTo) throws Exception {
        if (!QS.isInitilized()) {
            throw new Exception("We don't have Queryable State initialized.");
        }
        HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>> localAdjacencyList = new HashMap<>();
        for(long timestamp: localState.keys()) {
            if(timestamp > from && timestamp < maxValidTo) {
                for (GradoopId src : localState.get(timestamp).keySet()) {
                    if (!localAdjacencyList.containsKey(src)) {
                        localAdjacencyList.put(src, new HashMap<>());
                    }
                    localAdjacencyList.get(src).putAll(localState.get(timestamp).get(src));
                }
            }
        }
        /*
        for(int key : allKeys) {
            if(key != localKey) {
                int tries = 0;
                int maxTries = 10;
                while(tries < maxTries) {
                    try {
                        MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>> tempState =
                                QS.getALState(key);
                        for (Long timestamp : tempState.keys()) {
                            if (timestamp <= maxValidTo) {
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
                        if (tries >= maxTries) {
                            throw new Exception("We tried to get state " + maxTries + " times, but failed. ");
                        }
                    }
                }
            }
        }
         */
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
                                if (!triangle.get() && localAdjacencyList.containsKey(neighbour2)) {
                                    if (localAdjacencyList.get(neighbour2).containsKey(neighbour1)) {
                                        triangle.set(true);
                                    }
                                }
                                if(!triangle.get() && (cache.containsKey(neighbour1))) {
                                    triangle.set(cache.get(neighbour1).containsKey(neighbour2));
                                }
                                if(!triangle.get() && cache.containsKey(neighbour2)) {
                                    triangle.set(cache.get(neighbour2).containsKey(neighbour1));
                                }
                                if(!triangle.get() && !localAdjacencyList.containsKey(neighbour1) &&
                                        !localAdjacencyList.containsKey(neighbour2) && !cache.containsKey(neighbour1)
                                        && !cache.containsKey(neighbour2)) {
                                    Iterator<Byte> byteIterator = fennel.getPartitions(GradoopIdUtil.getLong(neighbour1));
                                    List<Byte> byteList = new LinkedList<>();
                                    while (byteIterator.hasNext()) {
                                        byteList.add(byteIterator.next());
                                    }
                                    GradoopId[] list = new GradoopId[]{neighbour1};
                                    for(Byte partition : byteList) {
                                        //System.out.print("checking for partition byte: "+partition+" which translates to key: "
                                        //+allKeys[partition]);
                                        int tries = 0;
                                        while(tries < 10) {
                                           try {
                                               HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>> temp = QS.getALVerticesFromTo(
                                                       allKeys[partition], list, from, maxValidTo);
                                               if(!cache.containsKey(neighbour1)) {
                                                   cache.put(neighbour1, new HashMap<>());
                                               }
                                               cache.get(neighbour1).putAll(temp.get(neighbour1));
                                               if (temp.get(neighbour1).containsKey(neighbour2)) {
                                                   triangle.set(true);
                                               }
                                               tries = 10;
                                           } catch (Exception e) {
                                               tries++;
                                               if(tries == 10) {
                                                   System.out.print("ERROR, tried 10 times & failed using QS.");
                                               }
                                           }
                                        }
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
        String output = "In partition "+localKey+" we found "+triangleCount.get()+" triangles ";
        System.out.println(output);
        return output;
    }
}
