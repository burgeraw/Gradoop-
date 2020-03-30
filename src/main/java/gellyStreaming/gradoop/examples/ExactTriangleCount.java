package gellyStreaming.gradoop.examples;

import gellyStreaming.gradoop.model.SimpleEdgeStream;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Single-pass, insertion-only exact Triangle Local and Global Count algorithm.
 * <p>
 * Based on http://www.kdd.org/kdd2016/papers/files/rfp0465-de-stefaniA.pdf.
 */
public class ExactTriangleCount {

    //TODO incorrect result, also not deterministic
    // But, based on paper on Triangle estimation

    public static void main(String[] args) throws Exception {

        if (!parseParameters(args)) {
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SimpleEdgeStream<Integer, NullValue> edges = getGraphStream(env);

        //edges.print();

        DataStream<Tuple2<Integer, Integer>> result =
                edges.buildNeighborhood(false)
                        .map(new ProjectCanonicalEdges())
                        .keyBy(0, 1).flatMap(new IntersectNeighborhoods())
                        .keyBy(0).flatMap(new SumAndEmitCounters());

        if (resultPath != null) {
            result.writeAsText(resultPath);
        }
        else {
            result.filter(new FilterFunction<Tuple2<Integer, Integer>>() {
                @Override
                public boolean filter(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {
                    return (integerIntegerTuple2.f0==-1);
                }
            }).print();
        }
        //edges.numberOfEdges().print();

        env.execute("Exact Triangle Count");

        System.out.println(counter2);
        System.out.println(vertices68);
        System.out.println(vertices68.size());
        System.out.println(vertices172);
        System.out.println(vertices172.size());
        for(int item: vertices68){
            if(vertices172.contains(item)) {
                System.out.println(item);
            }
        }

    }

    // *** Transformation Methods *** //


    /**
     * Receives 2 tuples from the same edge (src + target) and intersects the attached neighborhoods.
     * For each common neighbor, increase local and global counters.
     */
    public static final class IntersectNeighborhoods implements
            FlatMapFunction<Tuple4<Integer, Integer, TreeSet<Integer>, ReentrantReadWriteLock>,
                    Tuple2<Integer, Integer>> {
        ReentrantReadWriteLock lock2 = new ReentrantReadWriteLock();
        Map<Tuple2<Integer, Integer>, TreeSet<Integer>> neighborhoods = new HashMap<>();

        public void flatMap(Tuple4<Integer, Integer, TreeSet<Integer>, ReentrantReadWriteLock> t,
                            Collector<Tuple2<Integer, Integer>> out) {
            //intersect neighborhoods and emit local and global counters
            Tuple2<Integer, Integer> key = new Tuple2<>(t.f0, t.f1);
            if (neighborhoods.containsKey(key)) {
                // this is the 2nd neighborhood => intersect
                lock2.writeLock().lock();
                TreeSet<Integer> t1 = neighborhoods.remove(key);
                lock2.writeLock().unlock();
                TreeSet<Integer> t2 = t.f2;
                int counter = 0;
                if (t1.size() < t2.size()) {
                    // iterate t1 and search t2
                    for (int i : t1) {
                        if (t2.contains(i)) {
                            counter++;
                            out.collect(new Tuple2<>(i, 1));
                        }
                    }
                } else {
                    // iterate t2 and search t1
                    for (int i : t2) {
                        if (t1.contains(i)) {
                            counter++;
                            out.collect(new Tuple2<>(i, 1));
                        }
                    }
                }
                if (counter > 0) {
                    //emit counter for srcID, trgID, and total
                    out.collect(new Tuple2<>(t.f0, counter));
                    out.collect(new Tuple2<>(t.f1, counter));
                    // -1 signals the total counter
                    out.collect(new Tuple2<>(-1, counter));
                }
            } else {
                // first neighborhood for this edge: store and wait for next
                lock2.writeLock().lock();
                neighborhoods.put(key, t.f2);
                lock2.writeLock().unlock();
            }
        }
    }

    /**
     * Sums up and emits local and global counters.
     */
    public static final class SumAndEmitCounters implements FlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
        Map<Integer, Integer> counts = new HashMap<>();

        public void flatMap(Tuple2<Integer, Integer> t, Collector<Tuple2<Integer, Integer>> out) {
            if (counts.containsKey(t.f0)) {
                int newCount = counts.get(t.f0) + t.f1;
                counts.put(t.f0, newCount);
                out.collect(new Tuple2<>(t.f0, newCount));
            } else {
                counts.put(t.f0, t.f1);
                out.collect(new Tuple2<>(t.f0, t.f1));
            }
        }
    }

    public static final class ProjectCanonicalEdges implements
            MapFunction<Tuple4<Integer, Integer, TreeSet<Integer>, ReentrantReadWriteLock>, Tuple4<Integer,
                    Integer, TreeSet<Integer>, ReentrantReadWriteLock>> {
        @Override
        public Tuple4<Integer, Integer, TreeSet<Integer>, ReentrantReadWriteLock> map(Tuple4<
                Integer, Integer, TreeSet<Integer>, ReentrantReadWriteLock> t) {
            int source = Math.min(t.f0, t.f1);
            int trg = Math.max(t.f0, t.f1);
            t.f3.writeLock().lock();
            t.setField(source, 0);
            t.setField(trg, 1);
            t.f3.writeLock().unlock();
            return t;
        }
    }


    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************

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