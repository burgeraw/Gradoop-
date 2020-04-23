package gellyStreaming.gradoop.StatefulFunctions;


import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;

final class MyFunction implements StatefulFunction {

    @Override
    public void invoke(Context context, Object input) {
        if (!(input instanceof MyMessages.MyInputMessage)) {
            throw new IllegalArgumentException("Unknown message received " + input);
        }
        MyMessages.MyInputMessage in = (MyMessages.MyInputMessage) input;
        MyMessages.MyOutputMessage out = new MyMessages.MyOutputMessage(in.getUserId(), in.getMessage());

        context.send(MyConstants.RESULT_EGRESS, out);
    }
}