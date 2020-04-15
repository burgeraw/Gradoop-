package gellyStreaming.gradoop.oldModel.examples;

import gellyStreaming.gradoop.oldModel.SimpleEdgeStream;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class NewExactTriangleCount {

    public static void main(String[] args) {
        if (!parseParameters(args)) {
            return;
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SimpleEdgeStream<Integer, NullValue> edges = getGraphStream(env);



    }

    private static boolean fileOutput = false;
    //private static String edgeInputPath = "src/main/resources/as-733/textfile.txt";
    //private static String edgeInputPath = "src/main/resources/Cit-HepPh.txt";
    //private static String edgeInputPath = null;
    //private static String edgeInputPath = "src/main/resources/as-733/all days/as20000102.txt";
    private static String edgeInputPath = "src/main/resources/ml-100k/u.data";
    private static String resultPath = null;

    private static boolean parseParameters(String[] args) {

        if (args.length > 0) {
            if (args.length != 2) {
                System.err.println("Usage: ExactTriangleCount <input edges path> <result path>");
                return false;
            }

            fileOutput = true;
            edgeInputPath = args[0];
            resultPath = args[1];
        } else {
            System.out.println("Executing ExactTriangleCount example with default parameters and built-in default data.");
            System.out.println("  Provide parameters to read input data from files.");
            System.out.println("  See the documentation for the correct format of input files.");
            System.out.println("  Usage: ExactTriangleCount <input edges path> <result path>");
        }
        return true;
    }

    public static int counter2 = 0;
    public static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    public static HashSet<Integer> vertices68 = new HashSet<>();
    public static HashSet<Integer> vertices172 =  new HashSet<>();
    @SuppressWarnings("serial")
    private static SimpleEdgeStream<Integer, NullValue> getGraphStream(StreamExecutionEnvironment env) {
        if (edgeInputPath != null) {
            return new SimpleEdgeStream<>(env.readTextFile(edgeInputPath)
                    .flatMap(new FlatMapFunction<String, Edge<Integer, NullValue>>() {
                        @Override
                        public void flatMap(String s, Collector<Edge<Integer, NullValue>> out) {
                            lock.readLock().lock();
                            if (counter2 == 0) {
                                out.collect(new Edge<>(68, 172, NullValue.getInstance()));
                                //out.collect(new Edge<>(68, 305, NullValue.getInstance()));
                                //out.collect(new Edge<>(172, 305, NullValue.getInstance()));
                            }
                            lock.readLock().unlock();
                            String[] fields = s.split("\t");
                            if (!fields[0].equals("#")) {
                                if(Integer.parseInt(fields[0])==68) {
                                    vertices68.add(Integer.parseInt(fields[1]));
                                }
                                if(Integer.parseInt(fields[0])==172) {
                                    vertices172.add(Integer.parseInt(fields[1]));
                                }
                                int src = Integer.parseInt(fields[0]);
                                int trg = Integer.parseInt(fields[1])+100000;
                                lock.writeLock().lock();
                                counter2++;
                                lock.writeLock().unlock();
                                out.collect(new Edge<>(src, trg, NullValue.getInstance()));
                            }
                        }
                    }), env);
        }

        return new SimpleEdgeStream<>(env.fromElements(
                new Edge<>(1, 2, NullValue.getInstance()),
                new Edge<>(2, 3, NullValue.getInstance()),
                new Edge<>(2, 6, NullValue.getInstance()),
                new Edge<>(5, 6, NullValue.getInstance()),
                new Edge<>(1, 4, NullValue.getInstance()),
                new Edge<>(5, 3, NullValue.getInstance()),
                new Edge<>(3, 4, NullValue.getInstance()),
                new Edge<>(3, 6, NullValue.getInstance()),
                new Edge<>(1, 3, NullValue.getInstance()),
                new Edge<>(1, 7, NullValue.getInstance()),
                new Edge<>(2, 7, NullValue.getInstance()),
                new Edge<>(3, 8, NullValue.getInstance()),
                new Edge<>(2, 8, NullValue.getInstance()),
                new Edge<>(5, 9, NullValue.getInstance()),
                new Edge<>(6, 9, NullValue.getInstance())
        ),
                env);
    }

}
