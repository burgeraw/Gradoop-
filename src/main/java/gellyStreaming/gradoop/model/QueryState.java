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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class QueryState {

    private QueryableStateClient client;
    private MapStateDescriptor<GradoopId, HashMap<GradoopId, TemporalEdge>> descriptor;
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

    public Boolean edgeExists(Integer key, GradoopId srcVertex, GradoopId trgVertex) {
        return null;
    }

    /*

        while(true) {
            for (int key : keys) {
                CompletableFuture<MapState<GradoopId, HashMap<GradoopId, TemporalEdge>>> resultFuture =
                        client.getKvState(
                                jobID,
                                "queryableState",
                                key,
                                new TypeHint<Integer>(){},
                                descriptor);
                resultFuture.thenAccept(response -> {
                    try {
                        GradoopId gradoopId = new GradoopId(0, 196, (short)0, 0);
                        System.out.println(response.get(gradoopId));
                        System.out.println("We got state");
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.out.println("We dont have state");
                    }
                });
            }
            //Thread.sleep(100);
        }
        //client.shutdownAndWait();
    }
     */

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

    public MapState<Integer, Integer> getState2(Integer key) throws Exception {
        ExecutionConfig executionConfig = new ExecutionConfig();

        MapStateDescriptor<Integer, Integer> descriptor2 =
                new MapStateDescriptor<Integer, Integer>(
                        "state",
                        TypeInformation.of(new TypeHint<Integer>() {
                        }).createSerializer(executionConfig),
                        TypeInformation.of(new TypeHint<Integer>() {
                        }).createSerializer(executionConfig)
                );
        CompletableFuture<MapState<Integer, Integer>> resultFuture =
                client.getKvState(
                        jobID,
                        "state",
                        key,
                        new TypeHint<Integer>() {
                        },
                        descriptor2);
        AtomicReference<Boolean> results = new AtomicReference<>(false);
        final Tuple1<MapState<Integer, Integer>> def = new Tuple1<>();
        try{
            def.f0 = resultFuture.get();
            results.set(true);
        } catch (Exception e) {
            System.out.println(e);
        }
        /*
        resultFuture.thenAccept(response -> {
            try {
                //answer.set(response.get(srcVertex));
                //System.out.println("in QS: "+response.get(key).toString());
                results.set(true);
                def.f0 = response;
                //System.out.println("in QS def.f0: "+def.f0);
            } catch (Exception e) {
                System.out.println("We dont have state");
                results.set(true);
            }
        });

         */
        //System.out.println(answer.get());
        if(results.get()) {
            return def.f0;
        } else {
            throw new Exception();
        }
    }
}
