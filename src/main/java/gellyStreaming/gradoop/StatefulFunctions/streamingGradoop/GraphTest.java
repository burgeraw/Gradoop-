package gellyStreaming.gradoop.StatefulFunctions.streamingGradoop;

import org.apache.flink.statefun.flink.harness.Harness;
import org.apache.flink.statefun.flink.harness.io.SerializableSupplier;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nonnull;
import java.util.concurrent.ThreadLocalRandom;

public class GraphTest {

    public static void main(String[] args) throws Exception {
        Harness harness =
                new Harness()
                        //.withKryoMessageSerializer()
                        .withSupplyingIngress(GraphConstants.REQUEST_INGRESS, new EdgeGenerator())
                        .withPrintingEgress(GraphConstants.RESULT_EGRESS);
        harness.start();
    }

    /** generate a random message, once a second a second. */
    private static final class EdgeGenerator
            implements SerializableSupplier<GraphMessages.MyInputMessage> {

        private static final long serialVersionUID = 1;
       /*
        private LinkedList<String> edgeList;

        private EdgeGenerator() throws IOException {
            final File file = new File("aves-sparrow-social.edges");
            FileReader fr = new FileReader(file);
            BufferedReader bf = new BufferedReader(fr);
            this.edgeList = new LinkedList<>();
            String line = "";
            while ((bf.readLine()!=null) && !bf.readLine().isEmpty()) {
                line = bf.readLine();
                System.out.println(line);
                this.edgeList.add(line);
            }

        }

*/
        @Override
        public GraphMessages.MyInputMessage get() {
            try {
                Thread.sleep(1_000);
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted", e);
            }
            return newEdge();
        }

        @Nonnull
        private GraphMessages.MyInputMessage newEdge() {
            final ThreadLocalRandom random = ThreadLocalRandom.current();
            final String srcId = StringUtils.generateRandomAlphanumericString(random, 2);
            final String trgId = StringUtils.generateRandomAlphanumericString(random, 2);
            return new GraphMessages.MyInputMessage(srcId, trgId);
        }
    }
}
