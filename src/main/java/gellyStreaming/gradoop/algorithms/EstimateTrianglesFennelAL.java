package gellyStreaming.gradoop.algorithms;

import gellyStreaming.gradoop.model.GradoopIdUtil;
import gellyStreaming.gradoop.model.QueryState;
import gellyStreaming.gradoop.partitioner.FennelPartitioning;
import it.unimi.dsi.fastutil.Hash;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class EstimateTrianglesFennelAL implements Algorithm<String, MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>>> {

    private final FennelPartitioning fennel;
    private final int QSbatchsize;
    private boolean cont;
    private final boolean caching;
    private final long timeToRun;

    public EstimateTrianglesFennelAL(FennelPartitioning fennel, Integer QSbatchsize, boolean caching, long timeToRun) {
        this.fennel = fennel;
        if(fennel==null) {
            throw new InstantiationError("Fennel vertex partitioning hasn't been instantiated.");
        }
        this.QSbatchsize = QSbatchsize;
        this.timeToRun = timeToRun;
        this.caching = caching;
    }

    @Override
    public String doAlgorithm(MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>> localState, QueryState QS, Integer localKey, Integer[] allKeys, long from, long maxValidTo) throws Exception {
        if (!QS.isInitilized()) {
            System.out.println("No QS");
            throw new Exception("We don't have Queryable State initialized.");
        }
        //this.cont = true;
        //Timer timer = new Timer();
        //timer.schedule(new stopAlgorithm(), timeToRun);
        long runUntil = System.currentTimeMillis() + timeToRun;
        ConcurrentHashMap<Integer, ConcurrentHashMap<GradoopId, LinkedList<GradoopId>>> QSqueue = new ConcurrentHashMap<>();
        ConcurrentHashMap<GradoopId, HashSet<GradoopId>> cache = new ConcurrentHashMap<>();
        AtomicInteger QSqueueSize = new AtomicInteger(0);
        long lambdas = 0L;
        int noLambdas = 0;

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
        //assert localState != null;
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
        GradoopId[] vertexIds = localAdjacencyList.keySet().toArray(GradoopId[]::new);
        HashSet<GradoopId> allVertexIds = new HashSet<>(Arrays.asList(vertexIds));
        int numberLocalVertices = vertexIds.length;
        Random random = new Random();
        //fennel.getState().record_map.size();
        int counterRepeats = 0;
        while(counterRepeats < 10) {
            counterRepeats++;
            if(counterRepeats >= 10) {
                if (System.currentTimeMillis() > runUntil) {
                    break;
                }
                counterRepeats = 0;
            }
            GradoopId vertexId1 = vertexIds[random.nextInt(numberLocalVertices)];
            List<GradoopId> neighboursVertex1 = new ArrayList<>(localAdjacencyList.get(vertexId1).keySet());
            GradoopId vertexId2 = neighboursVertex1.get(random.nextInt(neighboursVertex1.size()));
            allVertexIds.addAll(neighboursVertex1);
            if(localAdjacencyList.containsKey(vertexId2)) {
                Set<GradoopId> neighboursVertex2 = localAdjacencyList.get(vertexId2).keySet();
                int degreeVertex2 = neighboursVertex2.size();
                neighboursVertex2.retainAll(neighboursVertex1);
                int degreeVertex1 = neighboursVertex1.size();
                long lambda = neighboursVertex2.size() * (degreeVertex1 * degreeVertex2) / (3*(degreeVertex1+degreeVertex2));
                lambdas = lambdas + lambda;
                noLambdas++;
            } else if (cache.containsKey(vertexId2)) {
                Set<GradoopId> neighboursVertex2 = cache.get(vertexId2);
                int degreeVertex2 = neighboursVertex2.size();
                neighboursVertex2.retainAll(neighboursVertex1);
                int degreeVertex1 = neighboursVertex1.size();
                long lambda = neighboursVertex2.size() * (degreeVertex1 * degreeVertex2) / (3*(degreeVertex1+degreeVertex2));
                lambdas = lambdas + lambda;
                noLambdas++;
            } else {
                Iterator<Byte> byteIterator = fennel.getPartitions(GradoopIdUtil.getLong(vertexId2));
                List<Byte> byteList = new LinkedList<>();
                while (byteIterator.hasNext()) {
                    byteList.add(byteIterator.next());
                }
                for(Byte partition : byteList) {
                    int partitionKey = allKeys[partition];
                    if (!QSqueue.containsKey(partitionKey)) {
                        QSqueue.put(partitionKey, new ConcurrentHashMap<>());
                    }
                    if(!QSqueue.get(partitionKey).containsKey(vertexId2)) {
                        QSqueue.get(partitionKey).put(vertexId2, new LinkedList<>());
                    }
                    QSqueue.get(partitionKey).get(vertexId2).add(vertexId1);
                    QSqueueSize.getAndIncrement();

                    if(QSqueueSize.get() >= QSbatchsize) {
                        for (int partitionToQuery : QSqueue.keySet()) {
                            GradoopId[] list = QSqueue.get(partitionToQuery).keySet().toArray(GradoopId[]::new);
                            int tries = 0;
                            while (tries < 10 && cont) {
                                try {
                                    HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>> temp = QS.getALVerticesFromTo(
                                            partitionToQuery, list, from, maxValidTo);
                                    for(GradoopId beenQueried : list) {
                                        if (caching) {
                                            if (!cache.containsKey(beenQueried)) {
                                                cache.put(beenQueried, new HashSet<>());
                                            }
                                            cache.get(beenQueried).addAll(temp.get(beenQueried).keySet());
                                        }
                                        for (GradoopId potentialTriangle : QSqueue.get(partitionToQuery).get(beenQueried)) {
                                            Set<GradoopId> neighbours1 = localAdjacencyList.get(potentialTriangle).keySet();
                                            int degreeVertex1 = neighbours1.size();
                                            Set<GradoopId> neighbours2 = temp.get(beenQueried).keySet();
                                            allVertexIds.addAll(neighbours2);
                                            neighbours1.retainAll(neighbours2);
                                            int degreeVertex2 = neighbours2.size();
                                            long lambda = neighbours1.size() * (degreeVertex1 * degreeVertex2) / (3*(degreeVertex1+degreeVertex2));
                                            lambdas = lambdas + lambda;
                                            noLambdas++;
                                        }
                                    }
                                    break;
                                } //catch (NullPointerException ignored) {}
                                catch (Exception e) {
                                    tries++;
                                    //Thread.sleep(100);
                                    if (tries == 10) {
                                        System.out.println("ERROR, tried 10 times & failed using QS. "
                                                +"The request was: partition: "+partitionToQuery
                                                +" and list: "+ Arrays.toString(list) +" "
                                                +e);
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
        /*
        if(QSqueueSize.get() > 0) {
            for (int partitionToQuery : QSqueue.keySet()) {
                GradoopId[] list = QSqueue.get(partitionToQuery).keySet().toArray(GradoopId[]::new);
                int tries = 0;
                while (tries < 10) {
                    try {
                        HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>> temp = QS.getALVerticesFromTo(
                                partitionToQuery, list, from, maxValidTo);
                        for (GradoopId beenQueried : list) {
                            for (GradoopId potentialTriangle : QSqueue.get(partitionToQuery).get(beenQueried)) {
                                Set<GradoopId> neighbours1 = localAdjacencyList.get(potentialTriangle).keySet();
                                int degreeVertex1 = neighbours1.size();
                                Set<GradoopId> neighbours2 = temp.get(beenQueried).keySet();
                                neighbours1.retainAll(neighbours2);
                                int degreeVertex2 = neighbours2.size();
                                long lambda = neighbours1.size() * (degreeVertex1 * degreeVertex2) / (3 * (degreeVertex1 + degreeVertex2));
                                lambdas.add(lambda);
                            }
                        }
                        break;
                        //} catch (NullPointerException ignored) {}
                    }catch (Exception e) {
                        Thread.sleep(100);
                        tries++;
                        if (tries == 10) {
                            System.out.println("ERROR, tried 10 times & failed using QS. "+e);
                        }
                    }
                }
            }
        }

         */
        System.out.println("In "+timeToRun+"ms we sampled "+noLambdas+" times in partition "+localKey);
        System.out.println("In partition "+localKey+" we found "+allVertexIds.size()+" 'allvertexids'");
        int approxVertices = allVertexIds.size();
        long result = (lambdas / noLambdas) * approxVertices;
        String output = "In partition "+localKey+" we estimated "+result+" triangles";
        System.out.println(output);
        return output;
    }


    public class stopAlgorithm extends TimerTask {
        @Override
        public void run() {
            System.out.println("Stop running");
            cont = false;
            cancel();
        }

        @Override
        public boolean cancel() {
            return super.cancel();
        }
    }
}
