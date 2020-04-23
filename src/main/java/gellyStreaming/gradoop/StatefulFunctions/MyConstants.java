package gellyStreaming.gradoop.StatefulFunctions;


import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;

final class MyConstants {
    static final IngressIdentifier<MyMessages.MyInputMessage> REQUEST_INGRESS =
            new IngressIdentifier<>(
                    MyMessages.MyInputMessage.class, "org.apache.flink.statefun.examples.harness", "in");

    static final EgressIdentifier<MyMessages.MyOutputMessage> RESULT_EGRESS =
            new EgressIdentifier<>(
                    "org.apache.flink.statefun.examples.harness", "out", MyMessages.MyOutputMessage.class);

    static final FunctionType MY_FUNCTION_TYPE =
            new FunctionType("org.apache.flink.statefun.examples.harness", "my-function");
}