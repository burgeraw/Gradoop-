package gellyStreaming.gradoop.algorithms;

import gellyStreaming.gradoop.util.GradoopIdUtil;
import gellyStreaming.gradoop.model.QueryState;
import gellyStreaming.gradoop.partitioner.FennelPartitioning;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TriangleCountingALRetrieveEdge implements Algorithm<String, MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>>>, Serializable {

    // Granularity of retrieval: only getting a boolean if the edge exists from remote partition.
    private final int QSbatchsize;
    private final boolean caching;

    public TriangleCountingALRetrieveEdge(int QSbatchsize, boolean caching) {
        this.QSbatchsize = QSbatchsize;
        this.caching = caching;
    }


    @Override
    public String doAlgorithm(MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>> localState,
                              QueryState QS, Integer localKey, Integer[] allKeys, long from, long maxValidTo) throws InterruptedException {
        if (!QS.isInitilized()) {
            System.out.println("No QS");
        }
        // Queue for checking remote partitions if an edge if present. Format: Tuple2<
        // List of source vertices , List of target vertices >
        Tuple2<LinkedList<GradoopId>, LinkedList<GradoopId>> QSqueue = Tuple2.of(new LinkedList<>(), new LinkedList<>());
        HashMap<GradoopId, HashSet<GradoopId>> cache = new HashMap<>();
        AtomicInteger QSqueueSize = new AtomicInteger(0);
        AtomicLong QStimer = new AtomicLong(0);

        HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>> localAdjacencyList = new HashMap<>();
        int tries1 = 0;
        // If fully decoupled, we first need to retrieve the 'local'state.
        while (localState == null && tries1 < 10) {
            try {
                long start = System.currentTimeMillis();
                localState = QS.getALState(localKey);
                long stop = System.currentTimeMillis();
                QStimer.getAndAdd((stop - start));
                break;
            } catch (ConcurrentModificationException e) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) { }
                tries1++;
                if (tries1 >= 10) {
                    System.out.println("Error retrieving state. " + e);
                }
            } catch (Exception e) {
                tries1++;
                if (tries1 >= 10) {
                    System.out.println("Error retrieving state. " + e);
                }
            }
        }
        // Take relevant timestamps & put them in a single hashmap
        try {
            for (long timestamp : localState.keys()) {
                if (timestamp >= from && timestamp <= maxValidTo) {
                    for (GradoopId src : localState.get(timestamp).keySet()) {
                        if (!localAdjacencyList.containsKey(src)) {
                            localAdjacencyList.put(src, new HashMap<>());
                        }
                        localAdjacencyList.get(src).putAll(localState.get(timestamp).get(src));
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        // Iterate over vertex IDs and all pairs of its neighbours (which forms a wedge) and check if these
        // two neighbours are connected with an egde, which makes it a triangle.
        // Only count the triangles if the srcId < neighbour1 < neighbour2 to avoid duplicates.


        AtomicInteger triangleCount = new AtomicInteger(0);
        int percentage = 0;
        long currentsrcId = 0;
        long allVertices = localAdjacencyList.keySet().size();
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
                            if (!triangle.get() && cache.containsKey(neighbour1)) {
                                if (cache.get(neighbour1).contains(neighbour2)) {
                                    triangle.set(true);
                                }
                            }
                            if (!triangle.get() && cache.containsKey(neighbour2)) {
                                if (cache.get(neighbour2).contains(neighbour1)) {
                                    triangle.set(true);
                                }
                            }
                            if (triangle.get()) {
                                triangleCount.getAndIncrement();
                            }
                            if (!triangle.get()  && !localAdjacencyList.containsKey(neighbour1) &&
                                    !localAdjacencyList.containsKey(neighbour2)) {
                                QSqueue.f0.add(neighbour1);
                                QSqueue.f1.add(neighbour2);
                                QSqueueSize.getAndIncrement();
                                if (QSqueueSize.get() > QSbatchsize) {
                                    LinkedList<GradoopId> listSrc = QSqueue.f0;
                                    LinkedList<GradoopId> listTrg = QSqueue.f1;
                                    for (int partitionToQuery : allKeys) {
                                        if (partitionToQuery != localKey) {
                                            int tries = 0;
                                            while (tries < 10) {
                                                try {
                                                    long start = System.currentTimeMillis();
                                                    Boolean[] temp = QS.ALcontainsEdgesFromTo(
                                                            partitionToQuery, listSrc, listTrg, from, maxValidTo);
                                                    long stop = System.currentTimeMillis();
                                                    QStimer.getAndAdd((stop - start));
                                                    for (int k = temp.length-1; k > -1; k--) {
                                                        if (temp[k]!= null && temp[k]) {
                                                            if (caching) {
                                                                if (!cache.containsKey(listSrc.get(k))) {
                                                                    cache.put(listSrc.get(k), new HashSet<>());
                                                                }
                                                                cache.get(listSrc.get(k)).add(listTrg.get(k));
                                                            }
                                                            triangleCount.getAndIncrement();
                                                            listSrc.remove(k);
                                                            listTrg.remove(k);
                                                        } else if(temp[k]!= null && !temp[k]) {
                                                            listSrc.remove(k);
                                                            listTrg.remove(k);
                                                        }
                                                    }
                                                    break;
                                                } catch (ConcurrentModificationException e) {
                                                    try {
                                                        Thread.sleep(1000);
                                                    } catch (InterruptedException ignored) { }
                                                    tries++;
                                                    if (tries >= 10) {
                                                        System.out.println("Error retrieving state. " + e);
                                                    }
                                                } catch (NullPointerException e) {
                                                    e.printStackTrace();
                                                } catch (Exception e) {
                                                    tries++;
                                                    Thread.sleep(10);
                                                    if (tries >= 10) {
                                                        System.out.print("ERROR, tried 10 times & failed using QS. " + e);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    QSqueueSize.set(0);
                                    QSqueue = new Tuple2<>(new LinkedList<>(), new LinkedList<>());
                                }
                            }
                        }
                    }
                }
            }
            currentsrcId++;
            int newper;
            if((newper = (int)(currentsrcId/allVertices*100.)) > percentage) {
                percentage = newper;
                System.out.println("At "+percentage+"%");
            }
        }
        // Retrieved info doesn't need to be put in cache, because we won't use it again after.
        if (QSqueueSize.get() != 0) {
            LinkedList<GradoopId> listSrc = QSqueue.f0;
            LinkedList<GradoopId> listTrg = QSqueue.f1;
            for (int partitionToQuery : allKeys) {
                if (partitionToQuery != localKey) {
                    int tries = 0;
                    while (tries < 10) {
                        try {
                            long start = System.currentTimeMillis();
                            Boolean[] temp = QS.ALcontainsEdgesFromTo(
                                    partitionToQuery, listSrc, listTrg, from, maxValidTo);
                            long stop = System.currentTimeMillis();
                            QStimer.getAndAdd((stop - start));
                            for (int k = temp.length-1; k > -1; k--) {
                                if (temp[k]!= null && temp[k]) {
                                    triangleCount.getAndIncrement();
                                    listSrc.remove(k);
                                    listTrg.remove(k);
                                } else if(temp[k]!= null && !temp[k]) {
                                    listSrc.remove(k);
                                    listTrg.remove(k);
                                }
                            }
                            break;
                        } catch (NullPointerException e) {
                            e.printStackTrace();
                        } catch (ConcurrentModificationException e) {
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException ignored) { }
                            tries++;
                            if (tries >= 10) {
                                System.out.println("Error retrieving state. " + e);
                            }
                        }catch (Exception e) {
                            tries++;
                            Thread.sleep(10);
                            if (tries >= 10) {
                                System.out.print("ERROR, tried 10 times & failed using QS. " + e);
                            }
                        }
                    }
                }
            }
        }

        String output = "In partition " + localKey + " we found " + triangleCount.get() + " triangles ";
        System.out.println("Time spend on QS in partition \t" + localKey + "\t:\t" + QStimer.get());
        //System.out.println(output);
        return output;
    }

}
