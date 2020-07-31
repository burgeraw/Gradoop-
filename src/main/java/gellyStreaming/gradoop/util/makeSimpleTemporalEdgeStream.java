package gellyStreaming.gradoop.util;

import gellyStreaming.gradoop.model.SimpleTemporalEdgeStream;
import gellyStreaming.gradoop.partitioner.CustomKeySelector2;
import gellyStreaming.gradoop.partitioner.StoredVertex;
import gellyStreaming.gradoop.partitioner.StoredVertexPartitionState;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;

public class makeSimpleTemporalEdgeStream implements Serializable {


    public static SimpleTemporalEdgeStream getVertexPartitionedStream(StreamExecutionEnvironment env,
                                                                      Integer numberOfPartitions,
                                                                      String filepath,
                                                                      Integer vertexCount,
                                                                      Integer edgeCount,
                                                                      Boolean makeInf) {
        long currentTime = System.currentTimeMillis();
        final boolean cont = makeInf;
        KeyGen keyGenerator = new KeyGen(numberOfPartitions, KeyGroupRangeAssignment.computeDefaultMaxParallelism(numberOfPartitions));
        int[] keys = new int[numberOfPartitions];
        for (int i = 0; i < numberOfPartitions; i++)
            keys[i] = keyGenerator.next(i);
        env.setParallelism(1);
        DataStream<Tuple2<Long, List<Long>>> vertices = null;
        try {
            vertices = getVertices(env, filepath);
        } catch (IOException e) {
            e.printStackTrace();
        }

        DataStream<Tuple2<Edge<Long, String>, Integer>> edges = vertices.flatMap(new FlatMapFunction<Tuple2<Long, List<Long>>, Tuple2<Edge<Long, String>, Integer>>() {
            CustomKeySelector2 keySelector = new CustomKeySelector2(0);
            FennelPartitioner fennel = new FennelPartitioner(keySelector, numberOfPartitions, vertexCount, edgeCount);

            @Override
            public void flatMap(Tuple2<Long, List<Long>> vertexList, Collector<Tuple2<Edge<Long, String>, Integer>> collector) throws Exception {
                Long keyEdge = (long) keySelector.getKey(vertexList);
                Long srcId = vertexList.f0;
                int machineId = fennel.partition(keyEdge, numberOfPartitions);
                int partition = keys[machineId];
                for (Long neighbour : vertexList.f1) {
                    collector.collect(Tuple2.of(new Edge<Long, String>(srcId, neighbour, ""), partition));
                }
            }
        });


        env.setParallelism(numberOfPartitions);

        // Map the edges to Temporal Gradoop Edges
        GradoopIdSet graphId = new GradoopIdSet();
        DataStream<TemporalEdge> tempEdges = edges.map(
                (MapFunction<Tuple2<Edge<Long, String>, Integer>, TemporalEdge>) edge -> {
                    Map<String, Object> properties = new HashMap<>();
                    properties.put("partitionID", edge.f1);
                    properties.put("sourceVertexId", edge.f0.getSource());
                    properties.put("targetVertexId", edge.f0.getTarget());
                    return new TemporalEdge(
                            GradoopId.get(), // new GradoopId
                            null,
                            new GradoopId(edge.f0.getSource().intValue(), 0, (short) 0, 0), //src
                            new GradoopId(edge.f0.getTarget().intValue(), 0, (short) 0, 0), //trg
                            Properties.createFromMap(properties),
                            graphId,
                            currentTime, //validFrom
                            Long.MAX_VALUE //validTo
                    );
                });

        // Make the stream infinite if necessary
        ParallelSourceFunction<TemporalEdge> infinite = new ParallelSourceFunction<TemporalEdge>() {
            @Override
            public void run(SourceContext<TemporalEdge> sourceContext) {
                while (cont) {
                    sourceContext.collect(new TemporalEdge(null, null, null, null,
                            null, null, null, null));
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ignored) {
                    }
                }
            }

            @Override
            public void cancel() {
            }
        };
        if (makeInf) {
            DataStream<TemporalEdge> makeInfinite = env.addSource(infinite);
            tempEdges = tempEdges.union(makeInfinite)
                    .filter(new FilterFunction<TemporalEdge>() {
                        @Override
                        public boolean filter(TemporalEdge edge) throws Exception {
                            return edge.getId() != null;
                        }
                    });
        }
        return new SimpleTemporalEdgeStream(tempEdges, env, graphId);
    }

    public static SimpleTemporalEdgeStream getEdgePartitionedStream(StreamExecutionEnvironment env,
                                                                    Integer numberOfPartitions,
                                                                    String filepath,
                                                                    Boolean makeInf) {
        boolean cont = makeInf;
        env.setParallelism(numberOfPartitions);
        DataStream<Edge<Long, String>> edges;

            edges = env.readTextFile(filepath)
                    .filter(new FilterFunction<String>() {
                        @Override
                        public boolean filter(String s) {
                            return !s.isBlank() && !s.startsWith("#");
                        }
                    })
                    .map(new MapFunction<String, Edge<Long, String>>() {
                        @Override
                        public Edge<Long, String> map(String s) {
                            String[] fields = s.split("\\s");
                            long src = Long.parseLong(fields[0]);
                            long trg = Long.parseLong(fields[1]);
                            return new Edge<>(src, trg, null);
                        }
                    });
        //}
        DataStream<Tuple2<Edge<Long, String>, Integer>> partitionedStream =
                new PartitionEdges<Long, String>().getPartitionedEdges(edges, numberOfPartitions);
        GradoopIdSet graphId = new GradoopIdSet();
        long currentTime = System.currentTimeMillis();
        DataStream<TemporalEdge> tempEdges = partitionedStream.map(new MapFunction<Tuple2<Edge<Long, String>, Integer>, TemporalEdge>() {
            @Override
            public TemporalEdge map(Tuple2<Edge<Long, String>, Integer> edge) throws Exception {
                Map<String, Object> properties = new HashMap<>();
                properties.put("sourceVertexId", edge.f0.getSource());
                properties.put("targetVertexId", edge.f0.getTarget());
                properties.put("partitionID", edge.f1);
                return new TemporalEdge(
                        GradoopId.get(), //get new GradoopId
                        null,
                        new GradoopId(edge.f0.getSource().intValue(), 0, (short) 0, 0), //src
                        new GradoopId(edge.f0.getTarget().intValue(), 0, (short) 0, 0), //trg
                        Properties.createFromMap(properties),
                        graphId,
                        currentTime, //validFrom
                        Long.MAX_VALUE //validTo
                );
            }
        });
        DataStream<TemporalEdge> finalEdges;
        if(makeInf) {
            SourceFunction infinite = new SourceFunction<TemporalEdge>() {
                @Override
                public void run(SourceContext<TemporalEdge> sourceContext) throws Exception {
                    while (true) {
                        sourceContext.collect(new TemporalEdge(null, null, null, null,
                                null, null, null, null));
                        Thread.sleep(1000);
                    }
                }

                @Override
                public void cancel() {

                }
            };
            DataStream<TemporalEdge> makeInfinite = env.addSource(infinite);
            finalEdges = tempEdges.union(makeInfinite)
                    .filter(new FilterFunction<TemporalEdge>() {
                        @Override
                        public boolean filter(TemporalEdge edge) {
                            return edge.getId() != null;
                        }
                    });
        } else {
            finalEdges = tempEdges;
        }

        return new SimpleTemporalEdgeStream(finalEdges, env, graphId);
    }


    private static class FennelPartitioner<T> implements Serializable, Partitioner<T> {
        private static final long serialVersionUID = 1L;
        CustomKeySelector2 keySelector;
        private double k;  //no. of partitions
        private double alpha = 0.0;  //parameters for formula
        private double gamma = 0.0;
        private double loadlimit = 0.0;     //k*v+n/n
        private double n = 0.0;        // no of vertices
        private double m = 0.0;        //no. of edges
        StoredVertexPartitionState currentState;

        public FennelPartitioner(CustomKeySelector2 keySelector, int k, int n, int m) {
            this.keySelector = keySelector;
            this.k = (double) k;
            this.n = (double) n;
            this.m = (double) m;
            this.alpha = ((Math.pow(k, 0.5) * Math.pow(n, 1.5)) + m) / Math.pow(n, 1.5);
            this.gamma = 1.5;
            this.loadlimit = (k * 1.1 + n) / k;
            this.currentState = new StoredVertexPartitionState(k);
        }

        @Override
        public int partition(Object key, int numPartitions) {

            List neighbours = new ArrayList<>();

            try {
                neighbours = (List) keySelector.getValue(key);
            } catch (Exception e) {
                e.printStackTrace();
            }

            long source = (long) key;

            int machine_id = -1;


            StoredVertex first_vertex = currentState.getRecord(source);
            StoredVertex[] n_vertices = new StoredVertex[neighbours.size()];

            for (int i = 0; i < neighbours.size(); i++) {
                n_vertices[i] = currentState.getRecord((Long) neighbours.get(i));

            }

            LinkedList<Integer> candidates = new LinkedList<Integer>();
            double MAX_SCORE = Double.NEGATIVE_INFINITY;

            for (int p = 0; p < numPartitions; p++) {

                int occurences = 0;
                for (int i = 0; i < neighbours.size(); i++) {

                    if (n_vertices[i].hasReplicaInPartition(p)) {
                        occurences++;
                    }

                }
                double SCORE_m = -1;
                if (currentState.getMachineVerticesLoad(p) <= loadlimit) {
                    SCORE_m = (double) occurences - alpha * gamma * Math.pow((double) currentState.getMachineVerticesLoad(p), gamma - 1);
                } else if (currentState.getMachineVerticesLoad(p) > loadlimit) {
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
            return machine_id;
        }
    }

    public static DataStream<Tuple2<Long, List<Long>>> getVertices(StreamExecutionEnvironment env, String filepath) throws IOException {

        List<Tuple2<Long, List<Long>>> vertices = new ArrayList<>();

        return env.readTextFile(filepath)
                .map(new MapFunction<String, Tuple2<Long, List<Long>>>() {
                    @Override
                    public Tuple2<Long, List<Long>> map(String s) throws Exception {
                        String[] fields = s.split("\\[");
                        String src = fields[0];
                        int h = src.indexOf(':');
                        src = src.substring(0, h);
                        Long source = Long.parseLong(src);
                        String trg = fields[1];

                        long n = trg.indexOf("]");
                        String j = trg.substring(0, (int) n);
                        String fg = j.replaceAll("\\s", "");
                        String[] ne = fg.split("\\,");
                        int f = ne.length;
                        List<Long> neg = new ArrayList<Long>();
                        neg.add(Long.parseLong(ne[0]));
                        for (int k = 1; k < f; k++) {
                            neg.add(Long.parseLong(String.valueOf(ne[k])));

                        }
                        return new Tuple2<Long, List<Long>>(source, neg);
                    }
                });

    }

    private static DataStream<Tuple2<Long, List<Long>>> getVerticesWithThroughput(StreamExecutionEnvironment env,
                                                                                  String filepath,
                                                                                  Long timeBetweenElements) throws IOException {


        ParallelSourceFunction<Tuple2<Long, List<Long>>> mySourceFuntion = new ParallelSourceFunction<Tuple2<Long, List<Long>>>() {
            boolean isRunning=true;

            @Override
            public void run(SourceContext<Tuple2<Long, List<Long>>> sourceContext) throws Exception {
                Stream<String> stream = Files.lines(Path.of(filepath));
                Iterator<String> it = stream.iterator();
                while (isRunning && it.hasNext()) {
                    synchronized (sourceContext.getCheckpointLock()) {

                        String s = it.next();
                        String[] fields = s.split("\\[");
                        String src = fields[0];
                        int h = src.indexOf(':');
                        src = src.substring(0, h);
                        Long source = Long.parseLong(src);
                        String trg = fields[1];

                        long n = trg.indexOf("]");
                        String j = trg.substring(0, (int) n);
                        String fg = j.replaceAll("\\s", "");
                        String[] ne = fg.split("\\,");
                        int f = ne.length;
                        List<Long> neg = new ArrayList<Long>();
                        neg.add(Long.parseLong(ne[0]));
                        for (int k = 1; k < f; k++) {
                            neg.add(Long.parseLong(String.valueOf(ne[k])));

                        }
                        sourceContext.collect(new Tuple2<>(source, neg));
                        try {
                            Thread.sleep(timeBetweenElements);
                        } catch (InterruptedException ignored) {
                        }
                    }
                }
                cancel();
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        };

        return env.addSource(mySourceFuntion);

    }

}
