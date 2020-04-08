package gellyStreaming.gradoop.examples;

import gellyStreaming.gradoop.algorithms.ConnectedComponents;
import gellyStreaming.gradoop.model.GraphStream;
import gellyStreaming.gradoop.model.SimpleEdgeStream;
import gellyStreaming.gradoop.util.DisjointSet;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * The Connected Components algorithm assigns a component ID to each vertex in the graph.
 * Vertices that belong to the same component have the same component ID.
 * This algorithm computes _weakly_ connected components, i.e. edge direction is ignored.
 * <p>
 * This is a single-pass implementation, which uses a {@link gellyStreaming.gradoop.model.SummaryAggregation} to periodically merge
 * the partitioned state. For an iterative implementation, see @link IterativeConnectedComponents.
 */
public class ConnectedComponentsExample implements ProgramDescription {

    public static void main(String[] args) throws Exception {

        if (!parseParameters(args)) {
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        GraphStream<Long, NullValue, NullValue> edges = getGraphStream(env);

        DataStream<DisjointSet<Long>> cc = edges.aggregate(new ConnectedComponents<Long, NullValue>(mergeWindowTime));

        // flatten the elements of the disjoint set and print
        // in windows of printWindowTime
        cc.flatMap(new FlattenSet()).keyBy(0)
                .timeWindow(Time.of(printWindowTime, TimeUnit.MILLISECONDS))
                .fold(new Tuple2<Long, Long>(0l, 0l), new IdentityFold()).print();

        env.execute("Streaming Connected Components");
    }

    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************

    private static boolean fileOutput = false;
    private static String edgeInputPath = "src/main/resources/aves-sparrow-social.edges";
    private static long mergeWindowTime = 1000;
    private static long printWindowTime = 2000;

    private static boolean parseParameters(String[] args) {

        if (args.length > 0) {
            if (args.length != 3) {
                System.err.println("Usage: ConnectedComponentsExample <input edges path> <merge window time (ms)> "
                        + "print window time (ms)");
                return false;
            }

            fileOutput = true;
            edgeInputPath = args[0];
            mergeWindowTime = Long.parseLong(args[1]);
            printWindowTime = Long.parseLong(args[2]);
        } else {
            System.out.println("Executing ConnectedComponentsExample example with default parameters and built-in default data.");
            System.out.println("  Provide parameters to read input data from files.");
            System.out.println("  See the documentation for the correct format of input files.");
            System.out.println("  Usage: ConnectedComponentsExample <input edges path> <merge window time (ms)> "
                    + "print window time (ms)");
        }
        return true;
    }


    @SuppressWarnings("serial")
    private static GraphStream<Long, NullValue, NullValue> getGraphStream(StreamExecutionEnvironment env) {

        if (edgeInputPath!= null) {
            return new SimpleEdgeStream<Long, NullValue>(env.readTextFile(edgeInputPath)
                    .map(new MapFunction<String, Edge<Long, NullValue>>() {
                        @Override
                        public Edge<Long, NullValue> map(String s) {
                            String[] fields = s.split("\\s");
                            long src = Long.parseLong(fields[0]);
                            long trg = Long.parseLong(fields[1]);
                            return new Edge<>(src, trg, NullValue.getInstance());
                        }
                    }), env);
        }

        return new SimpleEdgeStream<>(env.generateSequence(1, 100).flatMap(
                new FlatMapFunction<Long, Edge<Long, Long>>() {
                    @Override
                    public void flatMap(Long key, Collector<Edge<Long, Long>> out) throws Exception {
                        out.collect(new Edge<>(key, key + 2, key * 100));
                    }
                }),
                new AscendingTimestampExtractor<Edge<Long, Long>>() {
                    @Override

                    public long extractAscendingTimestamp(Edge<Long, Long> element) {
                        return element.getValue();
                    }
                }, env).mapEdges(new MapFunction<Edge<Long, Long>, NullValue>() {
            @Override
            public NullValue map(Edge<Long, Long> edge) {
                return NullValue.getInstance();
            }
        });
    }

    @SuppressWarnings("serial")
    public static final class FlattenSet implements FlatMapFunction<DisjointSet<Long>, Tuple2<Long, Long>> {

        private Tuple2<Long, Long> t = new Tuple2<>();

        @Override
        public void flatMap(DisjointSet<Long> set, Collector<Tuple2<Long, Long>> out) {
            for (Long vertex : set.getMatches().keySet()) {
                Long parent = set.find(vertex);
                t.setField(vertex, 0);
                t.setField(parent, 1);
                out.collect(t);
            }
        }
    }

    @SuppressWarnings("serial")
    public static final class IdentityFold implements FoldFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
        public Tuple2<Long, Long> fold(Tuple2<Long, Long> accumulator, Tuple2<Long, Long> value) throws Exception {
            return value;
        }
    }

    @Override
    public String getDescription() {
        return "Streaming Connected Components on Global Aggregation";
    }
}