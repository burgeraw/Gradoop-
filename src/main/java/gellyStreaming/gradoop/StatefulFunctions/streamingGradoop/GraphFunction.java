package gellyStreaming.gradoop.StatefulFunctions.streamingGradoop;


import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.PersistedValue;

public class GraphFunction implements StatefulFunction {

    @Persisted
    private final PersistedValue<Integer> degree = PersistedValue.of("degree", Integer.class);

    @Override
    public void invoke(Context context, Object input) {
        if (!(input instanceof GraphMessages.MyInputMessage)) {
            throw new IllegalArgumentException("Unknown message received " + input);
        }
        GraphMessages.MyInputMessage in = (GraphMessages.MyInputMessage) input;
        String vertexId = context.self().id();
        int oldDegree = this.degree.getOrDefault(0);
        int newDegree = oldDegree + 1;
        this.degree.set(newDegree);
        GraphMessages.MyOutputMessage out = new GraphMessages.MyOutputMessage(vertexId, ""+newDegree);

        context.send(GraphConstants.RESULT_EGRESS, out);
    }
}
