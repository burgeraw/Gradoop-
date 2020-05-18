package gellyStreaming.gradoop.model;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class QueryableState {
    public static void main(String[] args) throws Exception {
        int numberOfPartitions = 4;
        Configuration config = new Configuration();
        config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
        String tmHostname = TaskManagerLocation.getHostName(InetAddress.getLocalHost());
        int proxyPort = 9069;

        QueryableStateClient client = new QueryableStateClient(tmHostname, proxyPort);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(numberOfPartitions, config);
        env.setParallelism(numberOfPartitions);

        MapStateDescriptor<GradoopId, HashMap<GradoopId, TemporalEdge>> descriptor =
                new MapStateDescriptor<GradoopId, HashMap<GradoopId, TemporalEdge>>(
                        "edgeList",
                        TypeInformation.of(new TypeHint<GradoopId>() {}).createSerializer(new ExecutionConfig()),
                        TypeInformation.of(new TypeHint<HashMap<GradoopId, TemporalEdge>>() {}).createSerializer(new ExecutionConfig())
                        );
        String jobIdParam = "3ab4d1b92dfa89ecdce80b5a64eb5d9c";
        JobID jobId = JobID.fromHexString(jobIdParam);
        List<Integer> keys = Arrays.asList(4, 9, 1, 2);
        for(int key: keys) {
                CompletableFuture<MapState<GradoopId, HashMap<GradoopId, TemporalEdge>>> resultFuture =
                        client.getKvState(jobId,
                                "edgeList",
                                key,
                                new TypeHint<Integer>() {
                                },
                                descriptor);
                System.out.println(resultFuture.toString());
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
                System.out.println(key);
        }
        client.shutdownAndWait();
    }
}
