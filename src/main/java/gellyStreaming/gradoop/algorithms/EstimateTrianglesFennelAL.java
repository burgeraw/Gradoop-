package gellyStreaming.gradoop.algorithms;

import com.google.common.util.concurrent.AtomicDouble;
import gellyStreaming.gradoop.model.GradoopIdUtil;
import gellyStreaming.gradoop.model.QueryState;
import gellyStreaming.gradoop.partitioner.FennelPartitioning;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static java.lang.Double.NaN;

public class EstimateTrianglesFennelAL implements Algorithm<String, MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>>> {

    // Granularity of retrieval: getting the vertexID & all its neighbours from remote partition.
    private final FennelPartitioning fennel;
    private final int QSbatchsize;
    private final boolean caching;
    private final long timeToRun;

    public EstimateTrianglesFennelAL(FennelPartitioning fennel, int QSbatchsize, boolean caching, long timeToRun) {
        this.fennel = fennel;
        if (fennel == null) {
            System.out.println("Fennel vertex partitioning hasn't been instantiated.");
            throw new InstantiationError("Fennel vertex partitioning hasn't been instantiated.");
        }
        this.QSbatchsize = QSbatchsize;
        this.caching = caching;
        this.timeToRun = timeToRun;
    }

    @Override
    public String doAlgorithm(MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>> localState,
                              QueryState QS, Integer localKey, Integer[] allKeys, long from, long maxValidTo)
            throws Exception {
        if (!QS.isInitilized()) {
            throw new Exception("We don't have Queryable State initialized.");
        }
        HashMap<Integer, HashMap<GradoopId, LinkedList<GradoopId>>> QSqueue = new HashMap<>();
        HashMap<GradoopId, HashSet<GradoopId>> cache = new HashMap<>();
        AtomicInteger QSqueueSize = new AtomicInteger(0);

        int tries1 = 0;
        while (localState == null && tries1 < 10) {
            System.out.println("localstate = null");
            try {
                localState = QS.getALState(localKey);
            } catch (Exception e) {
                tries1++;
                if (tries1 == 10) {
                    System.out.println("Error retrieving state. " + e);
                }
            }
        }
        if (localState == null) {
            System.out.println("no state");
            throw new ExceptionInInitializerError("We couldnt retrieve state.");
        }

        HashMap<GradoopId, HashSet<GradoopId>> localAdjacencyList = new HashMap<>();

        for (long timestamp : localState.keys()) {
            if (timestamp >= from && timestamp <= maxValidTo) {
                for (GradoopId src : localState.get(timestamp).keySet()) {
                    if (!localAdjacencyList.containsKey(src)) {
                        localAdjacencyList.put(src, new HashSet<>());
                    }
                    if (localState.get(timestamp).get(src).keySet().size() < 1) {
                        System.out.println(localState.get(timestamp).get(src).keySet().toString());
                        System.out.println("src:" + src);
                    }

                    localAdjacencyList.get(src).addAll(localState.get(timestamp).get(src).keySet());
                    if (src.toString().equals("000000000001370000000000")) {
                        //System.out.println(localState.get(timestamp).get(src).keySet().toString());
                        //System.out.println(localAdjacencyList.get(src).toString());
                    }
                    if (localAdjacencyList.get(src).size() < 1) {
                        System.out.println("less than 1 for: " + src);
                    }
                }
            }
        }


        AtomicInteger lambdasCount = new AtomicInteger(0);
        AtomicDouble lambdas = new AtomicDouble(0);

        GradoopId[] vertexIds = localAdjacencyList.keySet().toArray(GradoopId[]::new);
        int numberLocalVertices = vertexIds.length;

        long runUntil = System.currentTimeMillis() + timeToRun;

        Random random = new Random();


        while (System.currentTimeMillis() < runUntil && numberLocalVertices!=0) {
            GradoopId id1 = vertexIds[random.nextInt(numberLocalVertices)];

            //System.out.println(id1);

            if (localAdjacencyList.get(id1).isEmpty()) {
                System.out.println(id1 + " returns empty");
                break;
            }

            Set<GradoopId> neighboursVertex1 = localAdjacencyList.get(id1);
            int degree1 = neighboursVertex1.size();

            GradoopId id2 = neighboursVertex1.toArray(GradoopId[]::new)[random.nextInt(degree1)];

            if (localAdjacencyList.containsKey(id2)) {
                Set<GradoopId> neighboursVertex2 = localAdjacencyList.get(id2);
                int degree2 = neighboursVertex2.size();
                Set<GradoopId> intersection = neighboursVertex2.stream()
                        .filter(neighboursVertex1::contains)
                        .collect(Collectors.toSet());
                double lambda = (intersection.size() * (degree1 * degree2)) / (3. * (degree1 + degree2));
                lambdas.getAndAdd(lambda);
                lambdasCount.getAndIncrement();
            } else if (cache.containsKey(id2)) {
                Set<GradoopId> neighboursVertex2 = cache.get(id2);
                int degree2 = neighboursVertex2.size();
                Set<GradoopId> intersection = neighboursVertex2.stream()
                        .filter(neighboursVertex1::contains)
                        .collect(Collectors.toSet());
                double lambda = (intersection.size() * (degree1 * degree2)) / (3. * (degree1 + degree2));
                lambdas.getAndAdd(lambda);
                lambdasCount.getAndIncrement();
            } else {
                Iterator<Byte> byteIterator = fennel.getPartitions(GradoopIdUtil.getLong(id2));
                List<Byte> byteList = new LinkedList<>();
                while (byteIterator.hasNext()) {
                    byteList.add(byteIterator.next());
                }
                for (Byte partition : byteList) {
                    int partitionKey = allKeys[partition];
                    if(partitionKey != localKey) {
                        if (!QSqueue.containsKey(partitionKey)) {
                            QSqueue.put(partitionKey, new HashMap<>());
                        }
                        if (!QSqueue.get(partitionKey).containsKey(id2)) {
                            QSqueue.get(partitionKey).put(id2, new LinkedList<>());
                        }
                        QSqueue.get(partitionKey).get(id2).add(id1);
                        QSqueueSize.getAndIncrement();
                    } else {
                        lambdasCount.getAndIncrement();
                    }
                }
            }

            if (QSqueueSize.get() >= QSbatchsize && System.currentTimeMillis() < runUntil) {
                for (int partition : QSqueue.keySet()) {
                    GradoopId[] toQuery = QSqueue.get(partition).keySet().toArray(GradoopId[]::new);
                    int tries = 0;
                    while (tries < 10) {
                        try {
                            HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>> retrieved =
                                    QS.getALVerticesFromTo(partition, toQuery, from, maxValidTo);
                            for (GradoopId id3 : toQuery) {
                                Set<GradoopId> neighbours3 = retrieved.get(id3).keySet();
                                if (caching) {
                                    if (!cache.containsKey(id3)) {
                                        cache.put(id3, new HashSet<>());
                                    }
                                    cache.get(id3).addAll(neighbours3);
                                }
                                int degree3 = neighbours3.size();
                                for (GradoopId id4 : QSqueue.get(partition).get(id3)) {
                                    Set<GradoopId> neighbours4 = localAdjacencyList.get(id4);
                                    int degree4 = neighbours4.size();
                                    Set<GradoopId> intersection = neighbours3.stream()
                                            .filter(neighbours4::contains)
                                            .collect(Collectors.toSet());
                                    double lambda = (intersection.size() * (degree3 * degree4)) / (3. * (degree3 + degree4));
                                    lambdas.getAndAdd(lambda);
                                    lambdasCount.getAndIncrement();
                                }
                            }
                            break;
                        } catch (Exception e) {
                            tries++;
                            if (tries >= 10) {
                                System.out.println("Error retrieving state. " + e);
                            }
                        }
                    }
                    if(System.currentTimeMillis() >= runUntil) {
                        break;
                    }
                }
                QSqueueSize.set(0);
                QSqueue = new HashMap<>();
            }
        }
        System.out.println("In " + timeToRun + "ms we sampled " + lambdasCount.get() + " times in partition " + localKey);
        int approxVertices = numberLocalVertices * allKeys.length;
        System.out.println("In partition " + localKey + " we found approximate " + approxVertices + " 'allvertexids'");
        double result = (lambdas.get() / lambdasCount.get()) * approxVertices;
        if(lambdasCount.get()==0) {
            result = 0;
        }
        String output = "In partition " + localKey + " we estimated " + result + " triangles";
        System.out.println(output);
        return output;
    }
}

