package gellyStreaming.gradoop.model;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

public class QueryState implements Serializable {

    private transient QueryableStateClient client;


    private final MapStateDescriptor<GradoopId, HashMap<GradoopId, TemporalEdge>> descriptor;
    private final MapStateDescriptor<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>> descriptorAL;
    private transient JobID jobID;
    private transient boolean initilized = false;

    public QueryState() {
        initilized = false;
        ExecutionConfig executionConfig = new ExecutionConfig();
        executionConfig.registerPojoType(GradoopId.class);
        executionConfig.registerPojoType(TemporalEdge.class);
        executionConfig.registerPojoType(HashMap.class);
        // For sortedEdgeList.
        this.descriptor =
                new MapStateDescriptor<GradoopId, HashMap<GradoopId, TemporalEdge>>(
                        "sortedEdgeList",
                        TypeInformation.of(new TypeHint<GradoopId>() {
                        }).createSerializer(executionConfig),
                        TypeInformation.of(new TypeHint<HashMap<GradoopId, TemporalEdge>>() {
                        }).createSerializer(executionConfig)
                );

        // Only using this descriptor, since AL is the best format for triangle counting.
        this.descriptorAL =
                new MapStateDescriptor<>(
                        "adjacencyList",
                        TypeInformation.of(new TypeHint<Long>() {
                        }).createSerializer(executionConfig),
                        TypeInformation.of(new TypeHint<HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>>() {
                        }).createSerializer(executionConfig)
                );
    }


    public void initialize(JobID jobID) {

        this.jobID = jobID;
        initilized = true;
        System.out.println("jobid: " + jobID.toString());
        if(client==null) {
            String tmHostname = null;
            try {
                tmHostname = TaskManagerLocation.getHostName(InetAddress.getLocalHost());
                System.out.println("tmhostname: "+tmHostname);
                int proxyPort = 9069;

                //For cluster
                this.client = new QueryableStateClient(tmHostname, proxyPort);

                // For local IDE
                //this.client = new QueryableStateClient("localhost", proxyPort);

            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean isInitilized() {
        return initilized;
    }

    public HashMap<GradoopId, TemporalEdge> getSrcVertex(Integer key, GradoopId srcVertex) throws Exception {

        CompletableFuture<MapState<GradoopId, HashMap<GradoopId, TemporalEdge>>> resultFuture =
        client.getKvState(
                jobID,
                "sortedEdgeList",
                key,
                new TypeHint<Integer>() {
                },
                descriptor);
        AtomicReference<Boolean> results = new AtomicReference<>(false);
        final Tuple1<HashMap<GradoopId, TemporalEdge>> def = new Tuple1<>();
        try {
            def.f0 = resultFuture.get().get(srcVertex);
            results.set(true);
        }catch (Exception e) {
            throw e;
        }
        if(results.get()) {
            return def.f0;
        } else {
            throw new Exception();
        }
    }

    public TemporalEdge getEdge(Integer key, GradoopId srcVertex, GradoopId trgVertex) throws Exception {
        CompletableFuture<MapState<GradoopId, HashMap<GradoopId, TemporalEdge>>> resultFuture =
                client.getKvState(
                        jobID,
                        "sortedEdgeList",
                        key,
                        new TypeHint<Integer>() {
                        },
                        descriptor);
        AtomicReference<TemporalEdge> result = new AtomicReference<>(null);
        AtomicReference<Boolean> succesfulRetrieval = new AtomicReference<>(false);
        try {
            result.set(resultFuture.get().get(srcVertex).get(trgVertex));
            succesfulRetrieval.set(true);
        } catch (NullPointerException e) {
            succesfulRetrieval.set(true);
        } catch (Exception e) {
            throw e;
        }
        if(succesfulRetrieval.get()) {
            return result.get();
        } else {
            throw new Exception();
        }
    }

    public MapState<GradoopId, HashMap<GradoopId, TemporalEdge>> getState(Integer key) throws Exception {
        CompletableFuture<MapState<GradoopId, HashMap<GradoopId, TemporalEdge>>> resultFuture =
                client.getKvState(
                        jobID,
                        "sortedEdgeList",
                        key,
                        new TypeHint<Integer>() {
                        },
                        descriptor);
        AtomicReference<Boolean> results = new AtomicReference<>(false);
        final Tuple1<MapState<GradoopId, HashMap<GradoopId, TemporalEdge>>> def = new Tuple1<>();
        try {
            def.f0 = resultFuture.get();
            results.set(true);
        }catch (Exception e) {
            throw e;
        }
        if(results.get()) {
            return def.f0;
        } else {
            throw new Exception();
        }
    }

    public MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>> getALState(Integer key) throws Exception {
        CompletableFuture<MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>>> resultFuture =
                client.getKvState(
                        jobID,
                        "adjacencyList",
                        key,
                        new TypeHint<Integer>() {
                        },
                        descriptorAL);
        AtomicReference<Boolean> results = new AtomicReference<>(false);
        final Tuple1<MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>>> def = new Tuple1<>();
        try {
            def.f0 = resultFuture.get();
            results.set(true);
        }catch (Exception e) {
            throw e;
            //System.out.println("We failed to get key: "+key+" in QS. Exception: "+e);
        }
        if(results.get()) {
            return def.f0;
        } else {
            throw new Exception();
        }
    }

    public HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>> getALVerticesFromTo(
            Integer key, GradoopId[] vertexIds, long From, long To) throws Exception {
        CompletableFuture<MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>>> resultFuture =
                client.getKvState(
                        jobID,
                        "adjacencyList",
                        key,
                        new TypeHint<Integer>() {
                        },
                        descriptorAL);
        AtomicReference<Boolean> results = new AtomicReference<>(false);
        final Tuple1<MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>>> def = new Tuple1<>();
        final Tuple1<HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>> toReturn = Tuple1.of(new HashMap<>());
        try {
            def.f0 = resultFuture.get();
            for(long timestamp: def.f0.keys()) {
                if(timestamp <= To && timestamp >= From) {
                    for (GradoopId id : vertexIds) {
                        if (def.f0.get(timestamp).containsKey(id)) {
                            if (!toReturn.f0.containsKey(id)) {
                                toReturn.f0.put(id, new HashMap<>());
                            }
                            toReturn.f0.get(id).putAll(def.f0.get(timestamp).get(id));
                        }
                    }
                }
            }
            results.set(true);
        }catch (Exception e) {
            throw e;
            //System.out.println("We failed to get key: "+key+" in QS. Exception: "+e);
        }
        if(results.get()) {
            return toReturn.f0;
        } else {
            throw new Exception();
        }
    }

    public Tuple2<HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>, Integer> getALVerticesFromToPlusTotal(
            Integer key, GradoopId[] vertexIds, long From, long To) throws Exception {
        CompletableFuture<MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>>> resultFuture =
                client.getKvState(
                        jobID,
                        "adjacencyList",
                        key,
                        new TypeHint<Integer>() {
                        },
                        descriptorAL);
        AtomicReference<Boolean> results = new AtomicReference<>(false);
        final Tuple1<MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>>> def = new Tuple1<>();
        final Tuple2<HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>, Integer> toReturn = Tuple2.of(new HashMap<>(),0);
        HashSet<GradoopId> allVertices = new HashSet<>();
        try {
            def.f0 = resultFuture.get();
            for(long timestamp: def.f0.keys()) {
                if(timestamp <= To && timestamp >= From) {
                    allVertices.addAll(def.f0.get(timestamp).keySet());
                    for (GradoopId id : vertexIds) {
                        if (def.f0.get(timestamp).containsKey(id)) {
                            if (!toReturn.f0.containsKey(id)) {
                                toReturn.f0.put(id, new HashMap<>());
                            }
                            toReturn.f0.get(id).putAll(def.f0.get(timestamp).get(id));
                        }
                    }
                }
            }
            toReturn.f1 = allVertices.size();
            results.set(true);
        }catch (Exception e) {
            throw e;
            //System.out.println("We failed to get key: "+key+" in QS. Exception: "+e);
        }
        if(results.get()) {
            return toReturn;
        } else {
            throw new Exception();
        }
    }

    public Boolean[] ALcontainsEdgesFromTo(
            Integer key, LinkedList<GradoopId> src, LinkedList<GradoopId> trg, long From, long To) throws Exception {
        CompletableFuture<MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>>> resultFuture =
                client.getKvState(
                        jobID,
                        "adjacencyList",
                        key,
                        new TypeHint<Integer>() {
                        },
                        descriptorAL);
        AtomicReference<Boolean> results = new AtomicReference<>(false);
        Boolean[] contains = new Boolean[src.size()];
        for(int j = 0; j < contains.length; j++) {
            contains[j] = null;
        }try {
            for (long timestamp : resultFuture.get().keys()) {
                if (timestamp <= To && timestamp >= From) {
                    for (int i = 0; i < src.size(); i++) {
                        if (resultFuture.get().get(timestamp).containsKey(src.get(i)) &&
                                resultFuture.get().get(timestamp).get(src.get(i)).containsKey(trg.get(i))) {
                            contains[i] = true;
                        } else if (resultFuture.get().get(timestamp).containsKey(src.get(i)) &&
                                !resultFuture.get().get(timestamp).get(src.get(i)).containsKey(trg.get(i)) &&
                                contains[i] == null) {
                            contains[i] = false;
                        }
                    }
                }
            }
        }catch (Exception e) {
            throw e;
        }
        results.set(true);
        if(results.get()) {
            return contains;
        } else {
            throw new Exception();
        }
    }

    public HashSet<GradoopId> getALVertexListFromTo(
            Integer key, long From, long To) throws Exception {
        CompletableFuture<MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>>> resultFuture =
                client.getKvState(
                        jobID,
                        "adjacencyList",
                        key,
                        new TypeHint<Integer>() {
                        },
                        descriptorAL);
        AtomicReference<Boolean> results = new AtomicReference<>(false);
        final Tuple1<MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>>> def = new Tuple1<>();
        final Tuple1<HashSet<GradoopId>> vertices = Tuple1.of(new HashSet<>());
        try {
            def.f0 = resultFuture.get();
            for(long timestamp: def.f0.keys()) {
                if(timestamp <= To && timestamp >= From) {
                    vertices.f0.addAll(def.f0.get(timestamp).keySet());
                }
            }
            results.set(true);
        }catch (Exception e) {
            throw e;
            //System.out.println("We failed to get key: "+key+" in QS. Exception: "+e);
        }
        if(results.get()) {
            return vertices.f0;
        } else {
            throw new Exception();
        }
    }

    public Boolean getALEdgeFromTo(Integer key, GradoopId srcVertex, GradoopId trgVertex, Long from, Long to) throws Exception {
        CompletableFuture<MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>>> resultFuture =
                client.getKvState(
                        jobID,
                        "adjacencyList",
                        key,
                        new TypeHint<Integer>() {
                        },
                        descriptorAL);
        AtomicReference<Boolean> results = new AtomicReference<>(false);
        AtomicReference<Boolean> exists = new AtomicReference<>(null);
        try {
            for (long timestamp : resultFuture.get().keys()) {
                if (timestamp <= to && timestamp >= from) {
                    if (resultFuture.get().get(timestamp).containsKey(srcVertex)) {
                        if (resultFuture.get().get(timestamp).get(srcVertex).containsKey(trgVertex)) {
                            exists.set(true);
                            break;
                        }
                    }
                }
            }
            results.set(true);
        } catch (Exception e) {
            throw e;
        }
        if(results.get()) {
            return exists.get();
        } else {
            throw new Exception();
        }
    }




    public Boolean edgeExists(Integer key, GradoopId srcVertex, GradoopId trgVertex) throws Exception {
        CompletableFuture<MapState<GradoopId, HashMap<GradoopId, TemporalEdge>>> resultFuture =
                client.getKvState(
                        jobID,
                        "sortedEdgeList",
                        key,
                        new TypeHint<Integer>() {
                        },
                        descriptor);
        AtomicReference<Boolean> results = new AtomicReference<>(false);
        AtomicReference<Boolean> answer = new AtomicReference<>(null);
        try {
            answer.set(resultFuture.get().get(srcVertex).containsKey(trgVertex));
            results.set(true);
        } catch (NullPointerException e) {
            answer.set(false);
            results.set(true);
        } catch (Exception e) {
            throw e;
        }
        if(results.get()) {
            return answer.get();
        } else {
            throw new Exception();
        }
    }

}
