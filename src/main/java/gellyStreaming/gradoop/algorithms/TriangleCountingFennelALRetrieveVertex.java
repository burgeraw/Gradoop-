package gellyStreaming.gradoop.algorithms;

import gellyStreaming.gradoop.util.GradoopIdUtil;
import gellyStreaming.gradoop.model.QueryState;
import gellyStreaming.gradoop.partitioner.FennelPartitioning;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.MapState;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TriangleCountingFennelALRetrieveVertex implements Algorithm<String, MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>>> {

    // Granularity of retrieval: getting the vertexID & all its neighbours from remote partition.
    private final FennelPartitioning fennel;
    private final int QSbatchsize;
    private final boolean caching;

    public TriangleCountingFennelALRetrieveVertex(FennelPartitioning fennel, int QSbatchsize, boolean caching) {
        this.fennel = fennel;
        if (fennel == null) {
            System.out.println("Fennel vertex partitioning hasn't been instantiated.");
            throw new InstantiationError("Fennel vertex partitioning hasn't been instantiated.");
        }
        this.QSbatchsize = QSbatchsize;
        this.caching = caching;
    }

    @Override
    public String doAlgorithm(MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>> localState,
                              QueryState QS, Integer localKey, Integer[] allKeys, long from, long maxValidTo) throws InterruptedException {
        if (!QS.isInitilized()) {
            System.out.println("We don't have Queryable State initialized.");
        }

        // Queue : HashMap< remote partition key, Hashmap < ID to retrieve,
        // List< IDs to check if they make a triangle with this retrieved ID > >
        HashMap<Integer, HashMap<GradoopId, LinkedList<GradoopId>>> QSqueue = new HashMap<>();
        // Save the retrieved vertices and their neigbours in cache.
        HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>> cache = new HashMap<>();
        AtomicInteger QSqueueSize = new AtomicInteger(0);

        //Measure time spend on QS.
        AtomicLong QStimer = new AtomicLong(0);

        // If local state = null, as is the case in the fully decoupled case, retrieve the state first.
        int tries1 = 0;
        while (localState == null && tries1 < 10) {
            try {
                long start = System.currentTimeMillis();
                localState = QS.getALState(localKey);
                long stop = System.currentTimeMillis();
                QStimer.getAndAdd((stop-start));
            } catch (Exception e) {
                tries1++;
                if (tries1 == 10) {
                    System.out.println("Error retrieving state. " + e);
                }
            }
        }

        assert localState != null;
        // Get all relevant state together in one Hashmap.
        HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>> localAdjacencyList = new HashMap<>();
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
                            if (!triangle.get() && (cache.containsKey(neighbour1))) {
                                if (cache.get(neighbour1).containsKey(neighbour2)) {
                                    triangle.set(true);
                                }
                            }
                            if (!triangle.get() && cache.containsKey(neighbour2)) {
                                if (cache.get(neighbour2).containsKey(neighbour1)) {
                                    triangle.set(true);
                                }
                            }
                            if (triangle.get()) {
                                triangleCount.getAndIncrement();
                            }
                            if (!triangle.get() && !localAdjacencyList.containsKey(neighbour1) &&
                                    !localAdjacencyList.containsKey(neighbour2) && !cache.containsKey(neighbour1)
                                    && !cache.containsKey(neighbour2)) {
                                // Retrieve the partition the neighbour is in from the fennel partitioner.
                                Iterator<Byte> byteIterator = fennel.getPartitions(GradoopIdUtil.getLong(neighbour1));
                                List<Byte> byteList = new LinkedList<>();
                                while (byteIterator.hasNext()) {
                                    byteList.add(byteIterator.next());
                                }
                                for (Byte partition : byteList) {
                                    int partitionKey = allKeys[partition];
                                    // Add the request to the queue with the partition to retrieve the Vertex from, which
                                    // vertex we need, and add the edge we need to check if its present in the list after.
                                    if (!QSqueue.containsKey(partitionKey)) {
                                        QSqueue.put(partitionKey, new HashMap<>());
                                    }
                                    if (!QSqueue.get(partitionKey).containsKey(neighbour1)) {
                                        QSqueue.get(partitionKey).put(neighbour1, new LinkedList<>());
                                    }
                                    QSqueue.get(partitionKey).get(neighbour1).add(neighbour2);
                                    QSqueueSize.getAndIncrement();
                                    // If queue reaches size QSbatchsize, we go retrieve the requests.
                                    if (QSqueueSize.get() > QSbatchsize) {
                                        for (int partitionToQuery : QSqueue.keySet()) {
                                            GradoopId[] list = QSqueue.get(partitionToQuery).keySet().toArray(GradoopId[]::new);
                                            int tries = 0;
                                            while (tries < 10) {
                                                try {
                                                    long start = System.currentTimeMillis();
                                                    HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>> temp = QS.getALVerticesFromTo(
                                                            partitionToQuery, list, from, maxValidTo);
                                                    long stop = System.currentTimeMillis();
                                                    QStimer.getAndAdd((stop-start));
                                                    for (GradoopId beenQueried : list) {
                                                        if (caching) {
                                                            if (!cache.containsKey(beenQueried)) {
                                                                cache.put(beenQueried, new HashMap<>());
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
                                                //} catch (NullPointerException ignored) {
                                                } catch (Exception e) {
                                                    tries++;

                                                        Thread.sleep(10);

                                                    if (tries == 10) {
                                                        System.out.println("ERROR, tried 10 times & failed using QS. " + e);
                                                    }
                                                }
                                            }
                                        }
                                        QSqueueSize.set(0);
                                        QSqueue = new HashMap<>();
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        // Retrieved info doesn't need to be put in cache, because we won't use it again after.
        if (QSqueueSize.get() != 0) {
            for (int partitionToQuery : QSqueue.keySet()) {
                GradoopId[] list = QSqueue.get(partitionToQuery).keySet().toArray(GradoopId[]::new);
                int tries = 0;
                while (tries < 10) {
                    try {
                        long start = System.currentTimeMillis();
                        HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>> temp = QS.getALVerticesFromTo(
                                partitionToQuery, list, from, maxValidTo);
                        long stop = System.currentTimeMillis();
                        QStimer.getAndAdd((stop-start));
                        for (GradoopId beenQueried : list) {
                            for (GradoopId potentialTriangle : QSqueue.get(partitionToQuery).get(beenQueried)) {
                                if (temp.get(beenQueried).containsKey(potentialTriangle)) {
                                    triangleCount.getAndIncrement();
                                }
                            }
                        }
                        break;
                    } catch (NullPointerException ignored) {
                    } catch (Exception e) {

                            Thread.sleep(10);

                        tries++;
                        if (tries == 10) {
                            System.out.println("ERROR, tried 10 times & failed using QS. " + e);
                        }
                    }
                }
            }
        }
        String output = "In partition " + localKey + " we found " + triangleCount.get() + " triangles ";
        System.out.println("Time spend on QS in partition \t"+localKey+"\t:\t"+QStimer.get());
        //System.out.println(output);
        return output;
    }

    @Override
    public String doAlgorithm(MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>> localState, JobID jobID, Integer localKey, Integer[] allKeys, long from, long maxValidTo) throws InterruptedException {
        return null;
    }
}
