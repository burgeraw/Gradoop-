package gellyStreaming.gradoop.StatefulFunctions.streamingGradoop;


import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import gellyStreaming.gradoop.StatefulFunctions.streamingGradoop.GraphMessages.MyInputMessage;
import gellyStreaming.gradoop.StatefulFunctions.streamingGradoop.GraphMessages.MyOutputMessage;


final class GraphConstants {
    static final IngressIdentifier<MyInputMessage> REQUEST_INGRESS =
            new IngressIdentifier<>(
                    MyInputMessage.class, "streamingGradoop", "in");

    static final EgressIdentifier<MyOutputMessage> RESULT_EGRESS =
            new EgressIdentifier<>(
                    "streamingGradoop", "out", MyOutputMessage.class);

    static final FunctionType MY_FUNCTION_TYPE =
            new FunctionType("streamingGradoop", "graph-function");
}
