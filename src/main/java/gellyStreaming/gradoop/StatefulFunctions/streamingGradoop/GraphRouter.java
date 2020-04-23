package gellyStreaming.gradoop.StatefulFunctions.streamingGradoop;

import org.apache.flink.statefun.sdk.io.Router;

public class GraphRouter implements Router<GraphMessages.MyInputMessage> {

    @Override
    public void route(GraphMessages.MyInputMessage message, Downstream<GraphMessages.MyInputMessage> downstream) {
        downstream.forward(GraphConstants.MY_FUNCTION_TYPE, message.getSrcId(), message);
        //downstream.forward(GraphConstants.MY_FUNCTION_TYPE, message.getTrgId(), message);
    }
}