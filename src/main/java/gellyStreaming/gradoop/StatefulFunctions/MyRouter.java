package gellyStreaming.gradoop.StatefulFunctions;

import org.apache.flink.statefun.sdk.io.Router;

final class MyRouter implements Router<MyMessages.MyInputMessage> {

    @Override
    public void route(MyMessages.MyInputMessage message, Downstream<MyMessages.MyInputMessage> downstream) {
        downstream.forward(MyConstants.MY_FUNCTION_TYPE, message.getUserId(), message);
    }
}