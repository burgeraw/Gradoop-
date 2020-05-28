package gellyStreaming.gradoop.model;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.rest.RestServerEndpoint;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.webmonitor.WebMonitorEndpoint;
import org.apache.flink.runtime.webmonitor.WebMonitorExtension;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
                        "edgeList",
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

    public HashMap<GradoopId, TemporalEdge> getSrcVertex(Integer key, GradoopId srcVertex) throws ExecutionException, InterruptedException {

        CompletableFuture<MapState<GradoopId, HashMap<GradoopId, TemporalEdge>>> resultFuture =
        client.getKvState(
                jobID,
                "edgeList",
                key,
                new TypeHint<Integer>() {
                },
                descriptor);
        AtomicReference<HashMap<GradoopId, TemporalEdge>> answer = new AtomicReference<>();
        resultFuture.thenAccept(response -> {
            try {
                answer.set(response.get(srcVertex));
                //System.out.println(response.get(srcVertex));
            } catch (Exception e) {
                System.out.println("We dont have state");
            }
        });
        System.out.println(answer.get());
        return answer.get();
    }

    public MapState<GradoopId, HashMap<GradoopId, TemporalEdge>> getState(Integer key) {
        return null;
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
}
