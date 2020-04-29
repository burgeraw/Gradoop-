package gellyStreaming.gradoop.StatefulFunctions;

import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.PersistedAppendingBuffer;
import org.apache.flink.statefun.sdk.state.PersistedValue;

import java.util.HashSet;

final class MyTriangleCountingFunction implements StatefulFunction {

    @Persisted
    private final transient PersistedValue<Integer> triangleCount = PersistedValue.of("triangleCount", Integer.class);
    @Persisted
    private final transient PersistedAppendingBuffer<String> adjacentEdges = PersistedAppendingBuffer.of("adjacentEdges", String.class);

    @Override
    public void invoke(Context context, Object input) {
        if (input instanceof MyMessages.MyInputMessage) {
            MyMessages.MyInputMessage in = (MyMessages.MyInputMessage) input;
            if(in.getTrgId().equals(context.self().id())) {
                MyMessages.CheckIfTriangle triangleCheck = new MyMessages.CheckIfTriangle(adjacentEdges.view());
                context.send(MyConstants.MY_FUNCTION_TYPE, in.getSrcId(), triangleCheck);
                adjacentEdges.append(in.getSrcId());
            } else if (in.getSrcId().equals(context.self().id())) {
                MyMessages.CheckIfTriangle triangleCheck = new MyMessages.CheckIfTriangle(adjacentEdges.view());
                context.send(MyConstants.MY_FUNCTION_TYPE, in.getTrgId(), triangleCheck);
                adjacentEdges.append(in.getTrgId());
            } else {
                throw new IllegalArgumentException(String.format("Message (%s) send to wrong address(%s)", in, context.self().id()));
            }

        } else if (input instanceof MyMessages.CheckIfTriangle) {
            MyMessages.CheckIfTriangle in = (MyMessages.CheckIfTriangle) input;
            HashSet<String> adjacentEdgesSet = new HashSet<>();
            for(String edge : adjacentEdges.view()) {
                adjacentEdgesSet.add(edge);
            }
            int newTriangleCount = 0;
            for(Object vertexID : in.getPotentialTriangles()) {
                String vert = (String) vertexID;
                if(adjacentEdgesSet.contains(vert)) {
                    newTriangleCount = newTriangleCount + 1;
                    context.send(MyConstants.MY_FUNCTION_TYPE, vert, new MyMessages.UpdateTriangleCount(1));
                    context.send(MyConstants.RESULT_EGRESS, new MyMessages.MyTriangleOutputMessage(
                            context.self().id(), context.caller().id(), vert, triangleCount.getOrDefault(0)+newTriangleCount
                    ));
                }
            }
            if(newTriangleCount > 0) {
                context.send(MyConstants.MY_FUNCTION_TYPE, context.caller().id(), new MyMessages.UpdateTriangleCount(newTriangleCount));
                int oldTriangleCount = triangleCount.getOrDefault(0);
                triangleCount.set(oldTriangleCount+newTriangleCount);
            }
        } else if (input instanceof MyMessages.UpdateTriangleCount) {
            MyMessages.UpdateTriangleCount in = (MyMessages.UpdateTriangleCount) input;
            int oldTriangleCount = triangleCount.getOrDefault(0);
            triangleCount.set(oldTriangleCount+in.getNewTriangles());
            context.send(MyConstants.RESULT_EGRESS, new MyMessages.MyTriangleOutputMessage(
                    context.self().id(), null, null, triangleCount.getOrDefault(0)
            ));
        } else {
            throw new IllegalArgumentException("Unknown message received " + input);
        }
    }
}
