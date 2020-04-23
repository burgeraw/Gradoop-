package gellyStreaming.gradoop.StatefulFunctions;

import org.apache.flink.statefun.flink.harness.Harness;
import org.apache.flink.statefun.flink.harness.io.SerializableSupplier;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nonnull;
import java.util.concurrent.ThreadLocalRandom;

public class Test {
    public static void main(String[] args) throws Exception {
         Harness harness =
                    new Harness()
                            .withKryoMessageSerializer()
                            .withSupplyingIngress(MyConstants.REQUEST_INGRESS, new MessageGenerator())
                            .withPrintingEgress(MyConstants.RESULT_EGRESS);
         harness.start();
    }

        /** generate a random message, once a second a second. */
        private static final class MessageGenerator
                implements SerializableSupplier<MyMessages.MyInputMessage> {

            private static final long serialVersionUID = 1;

            @Override
            public MyMessages.MyInputMessage get() {
                try {
                    Thread.sleep(1_000);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Interrupted", e);
                }
                return randomMessage();
            }

            @Nonnull
            private MyMessages.MyInputMessage randomMessage() {
                final ThreadLocalRandom random = ThreadLocalRandom.current();
                final String userId = StringUtils.generateRandomAlphanumericString(random, 2);
                return new MyMessages.MyInputMessage(userId, "hello " + userId);
            }
        }
    }
