package gellyStreaming.gradoop.model;

import gellyStreaming.gradoop.oldModel.GraphStream;
import gellyStreaming.gradoop.oldModel.SimpleEdgeStream;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import javax.xml.crypto.Data;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.*;

public class Tests {

    public static void testLoadingGraph() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        DataStream<Edge<Long, Long>> edges = env
                .readTextFile("src/main/resources/ml-100k/u.data")
                .map(new MapFunction<String, Edge<Long, Long>>() {
                    @Override
                    public Edge<Long, Long> map(String s) throws Exception {
                        String[] args = s.split("\t");
                        long src = Long.parseLong(args[0]);
                        long trg = Long.parseLong(args[1]) + 1000000;
                        long val = Long.parseLong(args[2]) * 10;
                        return new Edge<>(src, trg, val);
                    }
                });
        GraphStream<Long, NullValue, Long> graph = new SimpleEdgeStream<>(edges, env);
        //graph.getEdges().print();
        //graph.numberOfEdges().print();
        //graph.numberOfVertices().print();
        env.execute();
    }

        // Q4
        // TODO You do not maintain state for the graph at all. DataStream is a "flowing context"
        // TODO: Check how/if they store the graph in GRADOOP code and locate the specific classes!
        // TODO: In adjacency list (AL) format? In edge list (EL)? In a compressed form like Compressed Sparsed Row (CSR)
        // TODO: Read this one it is useful: https://arxiv.org/pdf/1912.12740.pdf
        // TODO: Do they use hash tables?
        // TODO: You use DataStream<TemporalVertex> vertices.
        // TODO: But, if you want to access a specific vertex, how are you going to do it efficiently?
        // TODO: Especially if you have a processing window then we should store the part of the graph inside the window at least
        // TODO: And be able to access it fast. Food for thought.

        // TODO: Oh, please write code as neatly as possible for me to read it fast, so that I can help. Similar to how I transformed it. :)

    public static void testTemporalGraph() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        GradoopIdSet graphId = new GradoopIdSet();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<TemporalEdge> edges = env.readTextFile("src/main/resources/ml-100k/u.data")
                .map(new MapFunction<String, TemporalEdge>() {
                    @Override
                    public TemporalEdge map(String s) throws Exception {
                        String[] values = s.split("\t");
                        Map<String, Object> properties = new HashMap<>();
                        properties.put("rating", values[2]);

                        return new TemporalEdge(GradoopId.get(),
                                "watched",
                                new GradoopId(0, Integer.parseInt(values[0]), (short)0,0),
                                new GradoopId(0, Integer.parseInt(values[1]), (short)1,0),
                                Properties.createFromMap(properties),
                                graphId,
                                Long.parseLong(values[3]), // (valid until) starting time
                                Long.MAX_VALUE             //               ending   time
                        );
                    }
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<TemporalEdge>() {
                    @Override
                    public long extractAscendingTimestamp(TemporalEdge temporalEdge) {
                        return temporalEdge.getValidFrom();
                    }
                });

        //edgestream.numberOfVertices().print();
        DataStream<TemporalEdge> edges2 = getSampleEdgeStream(env);
        SimpleTemporalEdgeStream edgestream = new SimpleTemporalEdgeStream(edges2, env, null);
        GradoopSnapshotStream snapshotStream = edgestream.slice(Time.of(10, SECONDS), Time.of(1, SECONDS), EdgeDirection.OUT, "EL");

        JobExecutionResult job = env.execute();
        System.out.println(job.getNetRuntime());
    }

    public static void main(String[] args) throws Exception {
        //testLoadingGraph();
        testTemporalGraph();
    }

    private static DataStream<TemporalEdge> getSampleEdgeStream(StreamExecutionEnvironment env) {
        return env.fromElements(
                new TemporalEdge(
                        new GradoopId(0, 1, (short)1, 0),
                        null,
                        new GradoopId(0, 1, (short)0, 0),
                        new GradoopId(0, 2, (short)0, 0),
                        null,
                        null,
                        800000000L,
                        Long.MAX_VALUE),
                new TemporalEdge(
                        new GradoopId(0, 2, (short)1, 0),
                        null,
                        new GradoopId(0, 1, (short)0, 0),
                        new GradoopId(0, 3, (short)0, 0),
                        null,
                        null,
                        800000000L,
                        Long.MAX_VALUE),
                new TemporalEdge(
                        new GradoopId(0, 3, (short)1, 0),
                        null,
                        new GradoopId(0, 2, (short)0, 0),
                        new GradoopId(0, 3, (short)0, 0),
                        null,
                        null,
                        800000000L,
                        Long.MAX_VALUE),
                new TemporalEdge(
                        new GradoopId(0, 4, (short)1, 0),
                        null,
                        new GradoopId(0, 1, (short)0, 0),
                        new GradoopId(0, 4, (short)0, 0),
                        null,
                        null,
                        800001000L,
                        Long.MAX_VALUE),
                new TemporalEdge(
                        new GradoopId(0, 5, (short)1, 0),
                        null,
                        new GradoopId(0, 1, (short)0, 0),
                        new GradoopId(0, 5, (short)0, 0),
                        null,
                        null,
                        800001000L,
                        Long.MAX_VALUE),
                new TemporalEdge(
                        new GradoopId(0, 6, (short)1, 0),
                        null,
                        new GradoopId(0, 4, (short)0, 0),
                        new GradoopId(0, 5, (short)0, 0),
                        null,
                        null,
                        800001000L,
                        Long.MAX_VALUE),
                new TemporalEdge(
                        new GradoopId(0, 7, (short)1, 0),
                        null,
                        new GradoopId(0, 1, (short)0, 0),
                        new GradoopId(0, 6, (short)0, 0),
                        null,
                        null,
                        800002000L,
                        Long.MAX_VALUE),
                new TemporalEdge(
                        new GradoopId(0, 8, (short)1, 0),
                        null,
                        new GradoopId(0, 1, (short)0, 0),
                        new GradoopId(0, 7, (short)0, 0),
                        null,
                        null,
                        800002000L,
                        Long.MAX_VALUE),
                new TemporalEdge(
                        new GradoopId(0, 9, (short)1, 0),
                        null,
                        new GradoopId(0, 2, (short)0, 0),
                        new GradoopId(0, 7, (short)0, 0),
                        null,
                        null,
                        800002000L,
                        Long.MAX_VALUE),
                new TemporalEdge(
                        new GradoopId(0, 10, (short)1, 0),
                        null,
                        new GradoopId(0, 7, (short)0, 0),
                        new GradoopId(0, 8, (short)0, 0),
                        null,
                        null,
                        800003000L,
                        Long.MAX_VALUE),
                new TemporalEdge(
                        new GradoopId(0, 11, (short)1, 0),
                        null,
                        new GradoopId(0, 8, (short)0, 0),
                        new GradoopId(0, 1, (short)0, 0),
                        null,
                        null,
                        800003000L,
                        Long.MAX_VALUE)).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<TemporalEdge>() {
            @Override
            public long extractAscendingTimestamp(TemporalEdge temporalEdge) {
                return temporalEdge.getValidFrom();
            }
        });
    }
}