package gellyStreaming.gradoop.StatefulFunctions.streamingGradoop;

import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

import java.util.Map;

public class GraphModule implements StatefulFunctionModule {

    @Override
    public void configure(Map<String, String> globalConfiguration, Binder binder) {
        binder.bindIngressRouter(GraphConstants.REQUEST_INGRESS, new GraphRouter());
        binder.bindFunctionProvider(GraphConstants.MY_FUNCTION_TYPE, unused -> new GraphFunction());
    }
}