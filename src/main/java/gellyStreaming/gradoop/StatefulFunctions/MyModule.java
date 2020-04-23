package gellyStreaming.gradoop.StatefulFunctions;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

@AutoService(StatefulFunctionModule.class)
public class MyModule implements StatefulFunctionModule {

    @Override
    public void configure(Map<String, String> globalConfiguration, Binder binder) {
        binder.bindIngressRouter(MyConstants.REQUEST_INGRESS, new MyRouter());
        binder.bindFunctionProvider(MyConstants.MY_FUNCTION_TYPE, unused -> new MyFunction());
    }
}