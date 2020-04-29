package gellyStreaming.gradoop.StatefulFunctions;

import org.apache.flink.statefun.flink.harness.Harness;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.stream.Stream;

public class Test {
    public static void main(String[] args) throws Exception {

        Harness harness =
                    new Harness()
                            .withKryoMessageSerializer()
                            .withPrintingEgress(MyConstants.RESULT_EGRESS)
                            .withFlinkSourceFunction(MyConstants.REQUEST_INGRESS, new EdgeGenerator())
                 ;
         harness.start();
    }

    private static final class EdgeGenerator implements SourceFunction<MyMessages.MyInputMessage> {

        private volatile boolean isRunning = true;


        @Override
        public void run(SourceContext<MyMessages.MyInputMessage> sourceContext) throws Exception {
            Stream<String> lines = Files.lines(Path.of("src\\main\\resources\\aves-sparrow-social.edges"));
            Iterator<String> it = lines.iterator();
            while(isRunning && it.hasNext()) {
                try {
                    Thread.sleep(0_001);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Interrupted", e);
                }
                String line = it.next();
                String[] values = line.split(" ");
                sourceContext.collect(new MyMessages.MyInputMessage(
                        values[0],values[1], values[2]));
            }
            cancel();
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
