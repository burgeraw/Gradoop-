package gellyStreaming.gradoop.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.util.concurrent.TimeUnit.SECONDS;

public class KafkaThings {
    public static void main(String[] args) throws Exception {
        testStatefulFunctions(args);
        testKafkaOutput();
    }

    public static void testStatefulFunctions(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        GradoopIdSet graphId = new GradoopIdSet();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String outputTopic = "flink_output";
        KafkaSerializationSchema<List<TemporalEdge>> schema = new KafkaSerializationSchema<List<TemporalEdge>>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(List<TemporalEdge> temporalEdges, @Nullable Long aLong) {
                return null;
            }
        };

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        FlinkKafkaProducer<List<TemporalEdge>> flinkKafkaProducer
                = new FlinkKafkaProducer<List<TemporalEdge>>(outputTopic,schema, properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        DataStream<TemporalEdge> edges2 = Tests.getSampleEdgeStream(env);
        SimpleTemporalEdgeStream edgestream = new SimpleTemporalEdgeStream(edges2, env, graphId);
        edgestream
                .getEdges()
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<TemporalEdge>() {
                    @Override
                    public long extractAscendingTimestamp(TemporalEdge temporalEdge) {
                        return temporalEdge.getValidFrom();
                    }
                })
                .keyBy(
                        (KeySelector<TemporalEdge, GradoopId>) temporalEdge -> temporalEdge.getSourceId())
                .window(SlidingEventTimeWindows.of(Time.of(2, SECONDS),Time.of(2, SECONDS)))
                .process(new ProcessWindowFunction<TemporalEdge, List<TemporalEdge>, GradoopId, TimeWindow>() {
                    @Override
                    public void process(GradoopId gradoopId, Context context, Iterable<TemporalEdge> iterable, Collector<List<TemporalEdge>> collector) throws Exception {
                        List<TemporalEdge> edges = new ArrayList<>();
                        for(TemporalEdge edge: iterable) {
                            edges.add(edge);
                        }
                        System.out.println("In "+context.window().toString()+" there are edges: "+edges.toString());
                        System.out.println("___________");
                        if(edges.size()>0) {
                            collector.collect(edges);
                        }
                    }
                })
                .addSink(flinkKafkaProducer)
        ;
        env.execute();
    }

    public static class ProducerStringSerializationSchema implements KafkaSerializationSchema<String>{

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

    public static class ObjSerializationSchema implements KafkaSerializationSchema<List<String>>{

        private String topic;
        private ObjectMapper mapper;

        ObjSerializationSchema(String topic) {
            super();
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(List<String> obj, Long timestamp) {
            byte[] b = null;
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            try {
                b= mapper.writeValueAsBytes(obj);
            } catch (JsonProcessingException e) {
                System.out.println(e);
            }
            return new ProducerRecord<byte[], byte[]>(topic, b);
        }

    }

    public static void testKafkaOutput() throws Exception {
        String kafkaTopic = "TOPIC-IN";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        KafkaSerializationSchema<String> schema = new ProducerStringSerializationSchema(kafkaTopic);
        //KafkaSerializationSchema<List<String>> schema = new ObjSerializationSchema(kafkaTopic);

        FlinkKafkaProducer<String> kafkaProducer =
                new FlinkKafkaProducer<>(kafkaTopic,
                        schema,
                        properties,
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        DataStream<String> edges = env.readTextFile("src/main/resources/aves-sparrow-social.edges");
        edges.addSink(kafkaProducer);
        env.execute();


    }
}
