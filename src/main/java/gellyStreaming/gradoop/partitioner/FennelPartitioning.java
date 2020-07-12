package gellyStreaming.gradoop.partitioner;

import gellyStreaming.gradoop.util.KeyGen;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Created by zainababbas on 21/02/2017.
 */
public class FennelPartitioning implements Serializable {

    private int FennelID;

    public FennelPartitioning() {
        this.FennelID = 0;
    }

    private static Partitioner<Long> partitioner;

    public DataStream<Tuple2<Edge<Long, String>, Integer>> getFennelPartitionedEdges(
            StreamExecutionEnvironment env,
            String inputPath, Integer numberOfPartitions,
            int vertexCount, int edgeCount) throws IOException {
        System.out.println("Started Fennel partitioning at \t "+System.currentTimeMillis());
        env.setParallelism(1);
        DataStream<Tuple2<Long, List<Long>>> input = getVertices(env, inputPath);
        CustomKeySelector2<Long, Long> keySelector = new CustomKeySelector2<>(0);
        partitioner = new FennelPartitioner<>(keySelector, numberOfPartitions, vertexCount, edgeCount);
        DataStream<Tuple2<Edge<Long, String>, Integer>> stream =  input
                .flatMap(new FlatMapFunction<Tuple2<Long, List<Long>>, Tuple2<Edge<Long, String>, Integer>>() {
                    @Override
                    public void flatMap(Tuple2<Long, List<Long>> vertexList, Collector<Tuple2<Edge<Long, String>, Integer>> collector) throws Exception {
                        Long keyEdge = keySelector.getKey(vertexList);
                        if(vertexList.f1==null) {
                            System.out.println("Error");
                        }
                        Long srcId = vertexList.f0;
                        int machineId = partitioner.partition(keyEdge, numberOfPartitions);
                        for(Long neighbour : vertexList.f1) {
                            collector.collect(Tuple2.of(new Edge<Long, String>(srcId, neighbour, ""),machineId));
                        }
                    }
                });
        env.setParallelism(numberOfPartitions);
        return stream.keyBy(new KeySelector<Tuple2<Edge<Long, String>, Integer>, Object>() {
                    @Override
                    public Object getKey(Tuple2<Edge<Long, String>, Integer> edgeIntegerTuple2) throws Exception {
                        return edgeIntegerTuple2.f1;
                    }
                });
    }

    public StoredVertexPartitionState getState() {
        return ((FennelPartitioner<Long, Long>)partitioner).getCurrentState();
    }

    public Iterator<Byte> getPartitions(Long key) {
        return ((FennelPartitioner<Long, Long>)partitioner).currentState.getPartitions(key);
    }


    public static DataStream<Tuple2<Long, List<Long>>> getVertices(StreamExecutionEnvironment env, String inputPath) throws IOException {

        return env.readTextFile(inputPath)
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String s) throws Exception {
                        return !s.isBlank();
                    }
                })
                .map(new MapFunction<String, Tuple2<Long, List<Long>>>() {
                    @Override
                    public Tuple2<Long, List<Long>> map(String s) throws Exception {
                        //System.out.println(s);
                        String[] fields = s.split("\\[");
                        String src = fields[0];
                        int h = src.indexOf(':');
                        src = src.substring(0,h);
                        Long source = Long.parseLong(src);
                        String neighbourList= fields[1];
                        long n = neighbourList.indexOf("]");
                        neighbourList =  neighbourList.substring(0, (int) n);
                        neighbourList = neighbourList.replaceAll("\\s","");
                        String[] neighbours = neighbourList.split(",");
                        List<Long> AL = new ArrayList<>();
                        for (String neighbour : neighbours) {
                            AL.add(Long.parseLong(neighbour));
                        }
                        return Tuple2.of(source, AL);
                    }
                });
    }


    ///////code for partitioner/////////

    private static class FennelPartitioner<K, EV> implements Serializable, Partitioner<K> {
        private static final long serialVersionUID = 1L;
        CustomKeySelector2<K, EV> keySelector;
        private final double alpha;  //parameters for formula
        private final double gamma;
        private final double loadlimit;
        private StoredVertexPartitionState currentState;
        private int[] keys;


        public FennelPartitioner(CustomKeySelector2<K, EV> keySelector, int numPartitions, int numVertices, int numEdges) {
            this.keySelector = keySelector;
            this.alpha = ((Math.pow(numPartitions, 0.5) * Math.pow(numVertices, 1.5)) + numEdges) / Math.pow(numVertices, 1.5);
            this.gamma = 1.5;
            this.loadlimit = (numPartitions * 1.1 + numVertices) / numPartitions;
            this.currentState = new StoredVertexPartitionState(numPartitions);
            KeyGen keyGenerator = new KeyGen(numPartitions,
                    KeyGroupRangeAssignment.computeDefaultMaxParallelism(numPartitions));
            keys = new int[numPartitions];
            for (int i = 0; i < numPartitions ; i++)
                keys[i] = keyGenerator.next(i);
        }

        public StoredVertexPartitionState getCurrentState() {
            return this.currentState;
        }


        @Override
        public int partition(K key, int numPartitions) {
            List<Long> neighbours = new ArrayList<>();
            try {
                neighbours = (List<Long>)keySelector.getValue(key);
            } catch (Exception e) {
                e.printStackTrace();
            }
            long source = (long) key;

            int machine_id = -1;

            StoredVertex first_vertex = currentState.getRecord(source);
            StoredVertex[] n_vertices = new StoredVertex[neighbours.size()];

            for(int i=0;i<neighbours.size();i++) {
                n_vertices[i]= currentState.getRecord((Long) neighbours.get(i));
            }

            LinkedList<Integer> candidates = new LinkedList<>();
            double MAX_SCORE =  Double.NEGATIVE_INFINITY;
            for (int p = 0; p < numPartitions; p++) {
                int occurences=0;
                for(int i=0; i < neighbours.size(); i++) {
                    if(n_vertices[i].hasReplicaInPartition(p)) {
                        occurences++;
                    }
                }
                double SCORE_m = -1;
                if(currentState.getMachineVerticesLoad(p) <= loadlimit) {
                    SCORE_m = (double) occurences - alpha * gamma * Math.pow((double) currentState.getMachineVerticesLoad(p), gamma - 1);
                }

                else if(currentState.getMachineVerticesLoad(p) > loadlimit) {
                    SCORE_m = Double.NaN;
                }

                if (SCORE_m > MAX_SCORE) {
                    MAX_SCORE = SCORE_m;
                    candidates.clear();
                    candidates.add(p);
                } else if (SCORE_m == MAX_SCORE) {
                    candidates.add(p);
                }
            }

            if (candidates.isEmpty()) {
                System.out.println("ERROR: GreedyObjectiveFunction.performStep -> candidates.isEmpty()");
                System.out.println("MAX_SCORE: " + MAX_SCORE);
                System.exit(-1);
            }

            //*** PICK A RANDOM ELEMENT FROM CANDIDATES
            Random r = new Random();
            int choice = r.nextInt(candidates.size());
            machine_id = candidates.get(choice);

            if (currentState.getClass() == StoredVertexPartitionState.class) {
                StoredVertexPartitionState cord_state = (StoredVertexPartitionState) currentState;
                //NEW UPDATE RECORDS RULE TO UPDATE THE SIZE OF THE PARTITIONS EXPRESSED AS THE NUMBER OF VERTICES THEY CONTAINS
                if (!first_vertex.hasReplicaInPartition(machine_id)) {
                    first_vertex.addPartition(machine_id);
                    cord_state.incrementMachineLoadVertices(machine_id);
                }

            } else {
                //1-UPDATE RECORDS
                if (!first_vertex.hasReplicaInPartition(machine_id)) {
                    first_vertex.addPartition(machine_id);
                }
            }
            return keys[machine_id];
        }
    }
}

