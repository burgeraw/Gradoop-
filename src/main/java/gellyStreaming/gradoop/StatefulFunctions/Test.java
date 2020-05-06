package gellyStreaming.gradoop.StatefulFunctions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.statefun.flink.harness.Harness;
import org.apache.flink.statefun.flink.io.kafka.KafkaSourceProvider;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressBuilder;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressDeserializer;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressStartupPosition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Properties;
import java.util.stream.Stream;

public class Test {
    public static void main(String[] args) throws Exception {
        String kafkaTopic = "TOPIC-IN";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(kafkaTopic, new SimpleStringSchema(), properties);
        consumer.setStartFromEarliest();
        Harness harness =
                    new Harness()
                            .withKryoMessageSerializer()
                            .withPrintingEgress(MyConstants.RESULT_EGRESS)
                            .withFlinkSourceFunction(MyConstants.REQUEST_INGRESS, new EdgeGenerator())
                            //.withFlinkSourceFunction(MyConstants.REQUEST_INGRESS, new KafkaReader())

                 ;
         harness.start();

    }

    public static class ProducerStringSerializationSchema implements KafkaSerializationSchema<String> {

        private String topic;

        public ProducerStringSerializationSchema(String topic) {
            super();
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(String element, Long timestamp) {
            return new ProducerRecord<byte[], byte[]>(topic, element.getBytes(StandardCharsets.UTF_8));
        }
    }

    public static class IngressSpecs {

        public final IngressIdentifier<MyMessages.MyInputMessage> ID =
                new IngressIdentifier<>(MyMessages.MyInputMessage.class, "example", "input-ingress");

        public final IngressSpec<MyMessages.MyInputMessage> kafkaIngress =
                KafkaIngressBuilder.forIdentifier(ID)
                        .withKafkaAddress("localhost:9092")
                       // .withConsumerGroupId("greetings")
                        .withTopic("TOPIC-IN")
                        .withDeserializer(UserDeserializer.class)
                        .withStartupPosition(KafkaIngressStartupPosition.fromEarliest())
                        .build();
    }

    public class UserDeserializer implements KafkaIngressDeserializer<MyMessages.MyInputMessage> {

        private Logger LOG = LoggerFactory.getLogger(UserDeserializer.class);

        private final ObjectMapper mapper = new ObjectMapper();

        @Override
        public MyMessages.MyInputMessage deserialize(ConsumerRecord<byte[], byte[]> input) {
            try {
                return mapper.readValue(input.value(), MyMessages.MyInputMessage.class);
            } catch (IOException e) {
                LOG.debug("Failed to deserialize record", e);
                return null;
            }
        }
    }


    private static final class KafkaReader implements SourceFunction<MyMessages.MyInputMessage> {

        private volatile boolean isRunning = true;



        @Override
        public void run(SourceContext<MyMessages.MyInputMessage> sourceContext) throws Exception {
            String kafkaTopic = "TOPIC-IN";
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", "localhost:9092");
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(kafkaTopic, new SimpleStringSchema(), properties);
            consumer.setStartFromEarliest();
            DataStream<String> stream = env.addSource(consumer);

            //stream.print();
            //stream.rebalance().print();
            stream.rebalance().map(new MapFunction<String, String>() {
                private static final long serialVersionUID = -6867736771747690202L;

                @Override
                public String map(String value) throws Exception {
                    String[] vars = value.split(" ");
                    //sourceContext.collect(new MyMessages.MyInputMessage(vars[0], vars[1], vars[2]));
                    return value;
                }
            }).print();
            sourceContext.close();
            /*
            DataStream<MyMessages.MyInputMessage> stream2 = stream.flatMap(new FlatMapFunction<String, MyMessages.MyInputMessage>() {
                @Override
                public void flatMap(String s, Collector<MyMessages.MyInputMessage> collector) throws Exception {
                    System.out.println(s);
                    String[] vars = s.split(" ");
                    System.out.println(vars);
                    sourceContext.collect(new MyMessages.MyInputMessage(vars[0], vars[1], vars[2]));
                }
            });
            stream2.print();
            */
            //sourceContext.collect(new MyMessages.MyInputMessage("1", "2", "3"));
            env.execute();
            cancel();
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    private static final class EdgeGenerator implements SourceFunction<MyMessages.MyInputMessage> {

        private volatile boolean isRunning = true;


        @Override
        public void run(SourceContext<MyMessages.MyInputMessage> sourceContext) throws Exception {
            Stream<String> lines = Files.lines(Path.of("src/main/resources/aves-sparrow-social.edges"));
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
