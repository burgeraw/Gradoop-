package gellyStreaming.gradoop.algorithms;

import gellyStreaming.gradoop.model.GradoopIdUtil;
import gellyStreaming.gradoop.model.QueryState;
import gellyStreaming.gradoop.partitioner.FennelPartitioning;
import org.apache.flink.api.common.state.MapState;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TriangleCountingFennelALRetrieveVertex implements Algorithm<String, MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>>> {

    // Granularity of retrieval: getting the vertexID & all its neighbours from remote partition.
    private final FennelPartitioning fennel;
    private final int QSbatchsize;
    private final boolean caching;

    public TriangleCountingFennelALRetrieveVertex(FennelPartitioning fennel, int QSbatchsize, boolean caching) {
        this.fennel = fennel;
        if(fennel==null) {
            throw new InstantiationError("Fennel vertex partitioning hasn't been instantiated.");
        }
        this.QSbatchsize = QSbatchsize;
        this.caching = caching;
    }

    @Override
    public String doAlgorithm(MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>> localState,
                              QueryState QS, Integer localKey, Integer[] allKeys, long from, long maxValidTo)
            throws Exception {
            if (!QS.isInitilized()) {
                throw new Exception("We don't have Queryable State initialized.");
            }

            ConcurrentHashMap<Integer, ConcurrentHashMap<GradoopId, LinkedList<GradoopId>>> QSqueue = new ConcurrentHashMap<>();
            ConcurrentHashMap<GradoopId, HashMap<GradoopId, TemporalEdge>> cache = new ConcurrentHashMap<>();
            AtomicInteger QSqueueSize = new AtomicInteger(0);

            HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>> localAdjacencyList = new HashMap<>();

            int tries1 = 0;
            while(localState==null && tries1 < 10) {
                try {
                    localState = QS.getALState(localKey);
                } catch (Exception e) {
                    tries1++;
                    if(tries1==10) {
                        System.out.println("Error retrieving state. " + e);
                    }
                }
            }

        assert localState != null;
        for(long timestamp: localState.keys()) {
            if(timestamp >= from && timestamp <= maxValidTo) {
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
                                    if(cache.get(neighbour1).containsKey(neighbour2)) {
                                        triangle.set(true);
                                    }
                                }
                                if(!triangle.get() && cache.containsKey(neighbour2)) {
                                    if(cache.get(neighbour2).containsKey(neighbour1)) {
                                        triangle.set(true);
                                    }
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
                                        //System.out.print("checking for partition byte: "+partition+" which translates to key: "
                                        //+allKeys[partition]);
                                        int partitionKey = allKeys[partition];
                                        if(!QSqueue.containsKey(partitionKey)) {
                                            QSqueue.put(partitionKey, new ConcurrentHashMap<>());
                                        }
                                        if(!QSqueue.get(partitionKey).containsKey(neighbour1)) {
                                            QSqueue.get(partitionKey).put(neighbour1, new LinkedList<>());
                                        }
                                        QSqueue.get(partitionKey).get(neighbour1).add(neighbour2);
                                        QSqueueSize.getAndIncrement();

                                        if(QSqueueSize.get() > QSbatchsize) {
                                            for (int partitionToQuery : QSqueue.keySet()) {
                                                GradoopId[] list = QSqueue.get(partitionToQuery).keySet().toArray(GradoopId[]::new);
                                                int tries = 0;
                                                while (tries < 10) {
                                                    try {
                                                        HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>> temp = QS.getALVerticesFromTo(
                                                                partitionToQuery, list, from, maxValidTo);
                                                        for(GradoopId beenQueried : list) {
                                                            if (caching) {
                                                                if (!cache.containsKey(beenQueried)) {
                                                                    cache.put(beenQueried, new HashMap<>());
                                                                }
                                                                if(temp.get(beenQueried)==null) {
                                                                    System.out.println("big error");
                                                                }
                                                                cache.get(beenQueried).putAll(temp.get(beenQueried));
                                                            }
                                                                for (GradoopId potentialTriangle : QSqueue.get(partitionToQuery).get(beenQueried)) {
                                                                    if (temp.get(beenQueried).containsKey(potentialTriangle)) {
                                                                        triangleCount.getAndIncrement();
                                                                    }
                                                                }
                                                        }
                                                        break;
                                                    } catch (NullPointerException ignored) {}
                                                    catch (Exception e) {
                                                        tries++;
                                                        Thread.sleep(10);
                                                        if (tries == 10) {
                                                            System.out.println("ERROR, tried 10 times & failed using QS. "+e);
                                                        }
                                                    }
                                                }
                                            }
                                            QSqueueSize.set(0);
                                            QSqueue = new ConcurrentHashMap<>();
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            // Retrieved info doesn't need to be put in cache, because we won't use it again after.
            if(QSqueueSize.get()!= 0) {
                for (int partitionToQuery : QSqueue.keySet()) {
                    GradoopId[] list = QSqueue.get(partitionToQuery).keySet().toArray(GradoopId[]::new);
                    int tries = 0;
                    while (tries < 10) {
                        try {
                            HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>> temp = QS.getALVerticesFromTo(
                                    partitionToQuery, list, from, maxValidTo);
                            for(GradoopId beenQueried : list) {
                                for (GradoopId potentialTriangle : QSqueue.get(partitionToQuery).get(beenQueried)) {
                                    if (temp.get(beenQueried).containsKey(potentialTriangle)) {
                                        triangleCount.getAndIncrement();
                                    }
                                }
                            }
                            break;
                        } catch (NullPointerException ignored) {}
                        catch (Exception e) {
                            Thread.sleep(10);
                            tries++;
                            if (tries == 10) {
                                System.out.println("ERROR, tried 10 times & failed using QS. "+e);
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
