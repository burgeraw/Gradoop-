package gellyStreaming.gradoop.examples;

import gellyStreaming.gradoop.algorithms.BipartitenessCheck;
import gellyStreaming.gradoop.algorithms.Candidates;
import gellyStreaming.gradoop.oldModel.GraphStream;
import gellyStreaming.gradoop.oldModel.SimpleEdgeStream;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

public class bipartiteExample implements ProgramDescription {

    public static void main(String[] args) throws Exception {

        // Set up the environment
        if (!parseParameters(args)) {
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        GraphStream<Long, NullValue, NullValue> graph = new SimpleEdgeStream<>(getEdgesDataSet(env), env);
        DataStream<Candidates> bipartition = graph.aggregate
                (new BipartitenessCheck<Long, NullValue>((long) 500));
        // Emit the results
        if (fileOutput) {
            bipartition.writeAsCsv(outputPath);
        } else {
            bipartition.print();
        }

        env.execute("Bipartiteness Check");
    }


    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************

    private static boolean fileOutput = false;
    private static String edgeInputPath = "src/main/resources/ml-100k/u.data";
    private static String outputPath = null;
    public static int counter = 0;

    private static boolean parseParameters(String[] args) {

        if (args.length > 0) {
            if (args.length != 2) {
                System.err.println("Usage: BipartitenessCheckExample <input edges path> <output path>");
                return false;
            }

            fileOutput = true;
            edgeInputPath = args[0];
            outputPath = args[1];
        } else {
            System.out.println("Executing BipartitenessCheckExample example with default parameters and built-in default data.");
            System.out.println("  Provide parameters to read input data from files.");
            System.out.println("  See the documentation for the correct format of input files.");
            System.out.println("  Usage: BipartitenessCheckExample <input edges path> <output path>");
        }
        return true;
    }

    @SuppressWarnings("serial")
    private static DataStream<Edge<Long, NullValue>> getEdgesDataSet(StreamExecutionEnvironment env) {

        if (edgeInputPath!= null) {
            return env.readTextFile(edgeInputPath)
                    .map(new MapFunction<String, Edge<Long, NullValue>>() {
                        @Override
                        public Edge<Long, NullValue> map(String s) throws Exception {
                            String[] fields = s.split("\\t");
                            long src = Long.parseLong(fields[0]);
                            long trg = Long.parseLong(fields[1])+1000000;
                            return new Edge<>(src, trg, NullValue.getInstance());
                        }
                    });
        }

        return env.generateSequence(1, 100).flatMap(
                new FlatMapFunction<Long, Edge<Long, NullValue>>() {
                    @Override
                    public void flatMap(Long key, Collector<Edge<Long, NullValue>> out) throws Exception {
                        for (int i = 0; i < 10; i++) {
                            long target = key * 2 + 1;
                            out.collect(new Edge<>(key, target, NullValue.getInstance()));
                        }
                    }
                });
    }

    @Override
    public String getDescription() {
        return "Bipartiteness Check";
    }
}