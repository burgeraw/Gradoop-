package gellyStreaming.gradoop.algorithms;

import gellyStreaming.gradoop.model.Algorithm;
import gellyStreaming.gradoop.model.GradoopIdUtil;
import gellyStreaming.gradoop.model.QueryState;
import gellyStreaming.gradoop.partitioner.FennelPartitioning;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TriangleCountingAlg2 implements Algorithm<String, MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>>> {

    // Granularity of retrieval: getting the vertexID & all its neighbours from remote part.
    private final FennelPartitioning fennel;
    private final HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>> cache = new HashMap<>();
    private final HashMap<Integer, HashMap<GradoopId, HashSet<GradoopId>>> QSqueue = new HashMap<>();
    private final AtomicInteger QSqueueSize = new AtomicInteger(0);
    private final int QSbatchsize;

    public TriangleCountingAlg2(FennelPartitioning fennel, int QSbatchsize) {
        this.fennel = fennel;
        this.QSbatchsize = QSbatchsize;
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
                            if (triangle.get()) {
                                triangleCount.getAndIncrement();
                            }
                            if(!triangle.get() && !localAdjacencyList.containsKey(neighbour1) &&
                                    !localAdjacencyList.containsKey(neighbour2) && !cache.containsKey(neighbour1)
                                    && !cache.containsKey(neighbour2)) {
                                Iterator<Byte> byteIterator = fennel.getPartitions(GradoopIdUtil.getLong(neighbour1));
                                List<Byte> byteList = new LinkedList<>();
                                while (byteIterator.hasNext()) {
                                    byteList.add(byteIterator.next());
                                }
                                for(Byte partition : byteList) {
                                    int key = allKeys[partition];
                                    if(!QSqueue.containsKey(key)) {
                                        QSqueue.put(key, new HashMap<>());
                                    }
                                    if(!QSqueue.get(key).containsKey(neighbour1)
                                            && !QSqueue.get(key).containsKey(neighbour2)) {
                                        QSqueue.get(key).put(neighbour1, new HashSet<>());
                                        QSqueue.get(key).get(neighbour1).add(neighbour2);
                                    } else if (QSqueue.get(key).containsKey(neighbour1)){
                                        QSqueue.get(key).get(neighbour1).add(neighbour2);
                                    } else if (QSqueue.get(key).containsKey(neighbour2)) {
                                        QSqueue.get(key).get(neighbour2).add(neighbour1);
                                    } else {
                                        System.out.println("mistake");
                                    }
                                    QSqueueSize.getAndIncrement();

                                    if(QSqueueSize.get()==QSbatchsize) {
                                        //AtomicInteger newTriangles = requestAndCheckQSqueue(QS, from, maxValidTo);
                                        //triangleCount.getAndAdd(newTriangles.get());
                                        for(int partitionKey : QSqueue.keySet()) {
                                            GradoopId[] list = QSqueue.get(partitionKey).keySet().toArray(GradoopId[]::new);
                                            System.out.println(Arrays.toString(list));
                                            int tries = 0;
                                            while (tries < 10) {
                                                try {
                                                    HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>> temp = QS.getALVerticesFromTo(
                                                            partitionKey, list, from, maxValidTo);
                                                    for(GradoopId gradoopId : temp.keySet()) {
                                                        if(!cache.containsKey(gradoopId)) {
                                                            cache.put(gradoopId, new HashMap<>());
                                                        }
                                                        cache.get(gradoopId).putAll(temp.get(gradoopId));
                                                        for(GradoopId gradoopId1 : QSqueue.get(partitionKey).get(gradoopId)) {
                                                            if(cache.get(gradoopId).containsKey(gradoopId1)) {
                                                                triangleCount.getAndIncrement();
                                                                System.out.println("found a triangle");
                                                            }
                                                        }
                                                    }
                                                    break;
                                                } catch (Exception e) {
                                                    tries++;
                                                    if (tries == 10) {
                                                        System.out.print("ERROR, tried 10 times & failed using QS.");
                                                    }
                                                }
                                            }
                                        }
                                        QSqueue.clear();
                                        QSqueueSize.set(0);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        if(QSqueueSize.get()>0) {
            //AtomicInteger newTriangles = requestAndCheckQSqueue(QS, from, maxValidTo);
            //triangleCount.getAndAdd(newTriangles.get());
            for(int partitionKey : QSqueue.keySet()) {
                GradoopId[] list = QSqueue.get(partitionKey).keySet().toArray(GradoopId[]::new);
                System.out.println(Arrays.toString(list));
                int tries = 0;
                while (tries < 10) {
                    try {
                        HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>> temp = QS.getALVerticesFromTo(
                                partitionKey, list, from, maxValidTo);
                        for(GradoopId gradoopId : temp.keySet()) {
                            if(!cache.containsKey(gradoopId)) {
                                cache.put(gradoopId, new HashMap<>());
                            }
                            cache.get(gradoopId).putAll(temp.get(gradoopId));
                            for(GradoopId gradoopId1 : QSqueue.get(partitionKey).get(gradoopId)) {
                                if(cache.get(gradoopId).containsKey(gradoopId1)) {
                                    triangleCount.getAndIncrement();
                                    System.out.println("found a triangle");
                                }
                            }
                        }
                        break;
                    } catch (Exception e) {
                        tries++;
                        if (tries == 10) {
                            System.out.print("ERROR, tried 10 times & failed using QS.");
                        }
                    }
                }
            }
            QSqueue.clear();
            QSqueueSize.set(0);
        }
        String output = "In partition "+localKey+" we found "+triangleCount.get()+" triangles ";
        System.out.println(output);
        return output;
    }

    public AtomicInteger requestAndCheckQSqueue(QueryState QS, long from, long maxValidTo) {
        AtomicInteger triangleCount = new AtomicInteger();
        for(int partitionKey : QSqueue.keySet()) {
            GradoopId[] list = QSqueue.get(partitionKey).keySet().toArray(GradoopId[]::new);
            System.out.println(Arrays.toString(list));
            int tries = 0;
            while (tries < 10) {
                try {
                    HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>> temp = QS.getALVerticesFromTo(
                            partitionKey, list, from, maxValidTo);
                    for(GradoopId gradoopId : temp.keySet()) {
                        if(!cache.containsKey(gradoopId)) {
                            cache.put(gradoopId, new HashMap<>());
                        }
                        cache.get(gradoopId).putAll(temp.get(gradoopId));
                        for(GradoopId gradoopId1 : QSqueue.get(partitionKey).get(gradoopId)) {
                            if(cache.get(gradoopId).containsKey(gradoopId1)) {
                                triangleCount.getAndIncrement();
                                System.out.println("found a triangle");
                            }
                        }
                    }
                    break;
                } catch (Exception e) {
                    tries++;
                    if (tries == 10) {
                        System.out.print("ERROR, tried 10 times & failed using QS.");
                    }
                }
            }
        }
        QSqueue.clear();
        QSqueueSize.set(0);
        return triangleCount;
    }
}
