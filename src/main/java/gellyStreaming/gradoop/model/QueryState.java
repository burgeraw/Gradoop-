package gellyStreaming.gradoop.model;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;


import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class QueryState {

    private QueryableStateClient client;
    private MapStateDescriptor<GradoopId, HashMap<GradoopId, TemporalEdge>> descriptor;
    private MapStateDescriptor<GradoopId, Integer> descriptor2;
    private JobID jobID;
    private boolean initilized = false;

    public QueryState() {
        initilized = false;
    }

    public void initialize(JobID jobID) throws UnknownHostException {
        //String tmHostname = TaskManagerLocation.getHostName(InetAddress.getLocalHost());
        String tmHostname = "127.0.0.1";
        int proxyPort = 9069;
        this.jobID = jobID;
        initilized = true;
        ExecutionConfig executionConfig = new ExecutionConfig();
        executionConfig.registerPojoType(GradoopId.class);
        executionConfig.registerPojoType(TemporalEdge.class);
        executionConfig.registerPojoType(HashMap.class);

        this.client = new QueryableStateClient(tmHostname, proxyPort);
        this.descriptor =
                new MapStateDescriptor<GradoopId, HashMap<GradoopId, TemporalEdge>>(
                        "sortedEdgeList",
                        TypeInformation.of(new TypeHint<GradoopId>() {
                        }).createSerializer(executionConfig),
                        TypeInformation.of(new TypeHint<HashMap<GradoopId, TemporalEdge>>() {
                        }).createSerializer(executionConfig)
                );
        this.descriptor2 =
                new MapStateDescriptor<GradoopId, Integer>(
                        "vertexDegree",
                        TypeInformation.of(new TypeHint<GradoopId>() {
                        }).createSerializer(executionConfig),
                        TypeInformation.of(new TypeHint<Integer>() {
                        }).createSerializer(executionConfig)
                );
        System.out.println("jobid: " + jobID.toString());
        System.out.println("tmHostname: " + tmHostname);
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
            System.out.println("We failed to get key: "+key+" with srcVertex: "+srcVertex+" in QS. Exception: "+e);
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
            e.printStackTrace();
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
            System.out.println("We failed to get key: "+key+" in QS. Exception: "+e);
        }
        if(results.get()) {
            return def.f0;
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
            System.out.println("We failed to get key: "+key+" in QS. Exception: "+e);
        }
        if(results.get()) {
            return answer.get();
        } else {
            throw new Exception();
        }
    }

    public MapState<GradoopId, Integer> getVertexDegree(int key) throws Exception {
        CompletableFuture<MapState<GradoopId, Integer>> resultFuture =
                client.getKvState(
                        jobID,
                        "vertexDegree",
                        key,
                        new TypeHint<Integer>() {},
                        descriptor2);
        AtomicReference<Boolean> results = new AtomicReference<>(false);
        final Tuple1<MapState<GradoopId, Integer>> def = new Tuple1<>();
        try {
            def.f0 = resultFuture.get();
            results.set(true);
        }catch (Exception e) {
            System.out.println("We failed to get key: "+key+" in QS. Exception: "+e);
        }
        if(results.get()) {
            return def.f0;
        } else {
            throw new Exception();
        }
    }


    public void initialize2(JobID jobID) throws UnknownHostException {
        //String tmHostname = TaskManagerLocation.getHostName(InetAddress.getLocalHost());
        String tmHostname = "127.0.0.1";
        int proxyPort = 9069;
        this.jobID = jobID;
        initilized = true;
        ExecutionConfig executionConfig = new ExecutionConfig();

        this.client = new QueryableStateClient(tmHostname, proxyPort);
        MapStateDescriptor<Integer, Integer> descriptor2 =
                new MapStateDescriptor<Integer, Integer>(
                        "state",
                        TypeInformation.of(new TypeHint<Integer>() {
                        }).createSerializer(executionConfig),
                        TypeInformation.of(new TypeHint<Integer>() {
                        }).createSerializer(executionConfig)
                );
        System.out.println("jobid: " + jobID.toString());
        System.out.println("tmHostname: " + tmHostname);
    }

    public MapState<GradoopId, HashSet<GradoopId>> getState2(Integer key) throws Exception {
        ExecutionConfig executionConfig = new ExecutionConfig();
        MapStateDescriptor<GradoopId, HashSet<GradoopId>> descriptor3 =
                new MapStateDescriptor<GradoopId, HashSet<GradoopId>>(
                        "adjacencyList",
                        TypeInformation.of(new TypeHint<GradoopId>() {
                        }).createSerializer(executionConfig),
                        TypeInformation.of(new TypeHint<HashSet<GradoopId>>() {
                        }).createSerializer(executionConfig)
                );
        CompletableFuture<MapState<GradoopId, HashSet<GradoopId>>> resultFuture =
                client.getKvState(
                        jobID,
                        "adjacencyList",
                        key,
                        new TypeHint<Integer>() {
                        },
                        descriptor3);
        AtomicReference<Boolean> results = new AtomicReference<>(false);
        final Tuple1<MapState<GradoopId, HashSet<GradoopId>>> def = new Tuple1<>();
        try{
            def.f0 = resultFuture.get();
            results.set(true);
        } catch (Exception e) {
            System.out.println("In QS: "+e);
        }

        if(results.get()) {
            return def.f0;
        } else {
            throw new Exception();
        }
    }
}
