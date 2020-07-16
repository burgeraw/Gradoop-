package gellyStreaming.gradoop.util;

import gellyStreaming.gradoop.model.SimpleTemporalEdgeStream;
import gellyStreaming.gradoop.partitioner.FennelPartitioning;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class makeSimpleTemporalEdgeStream implements Serializable {

    private transient boolean cont;
    public FennelPartitioning fennel;

    public makeSimpleTemporalEdgeStream() {
        fennel = new FennelPartitioning();
    }

    public SimpleTemporalEdgeStream getVertexPartitionedStream(StreamExecutionEnvironment env,
                                                                      Integer numberOfPartitions,
                                                                      String filepath,
                                                                      Integer vertexCount,
                                                                      Integer edgeCount,
                                                                      Boolean makeInf)  {
        long currentTime = System.currentTimeMillis();
        cont = makeInf;
        DataStream<Tuple2<Edge<Long, String>, Integer>> edges = null;
        edges = fennel.getFennelPartitionedEdges(
                env, filepath, numberOfPartitions, vertexCount, edgeCount);
        GradoopIdSet graphId = new GradoopIdSet();
        env.setParallelism(numberOfPartitions);

        // Map the edges to Temporal Gradoop Edges
        assert edges != null;
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
        SourceFunction<TemporalEdge> infinite = new SourceFunction<TemporalEdge>() {
            @Override
            public void run(SourceContext<TemporalEdge> sourceContext) {
                while (true) {
                    System.out.println("Im still running ");
                    sourceContext.collect(new TemporalEdge(null, null, null, null,
                            null, null, null, null));
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ignored) { }
                }
            }

            @Override
            public void cancel() {
                cont = false;
            }
        };
        DataStream<TemporalEdge> makeInfinite = env.addSource(infinite);
        DataStream<TemporalEdge> finalEdges = tempEdges.union(makeInfinite)
                .filter(new FilterFunction<TemporalEdge>() {
                    @Override
                    public boolean filter(TemporalEdge edge) throws Exception {
                        return edge.getId() != null;
                    }
                });
        return new SimpleTemporalEdgeStream(finalEdges, env, graphId);
    }

    public SimpleTemporalEdgeStream getEdgePartitionedStream(StreamExecutionEnvironment env,
                                                                    Integer numberOfPartitions,
                                                                    String filepath,
                                                                    Boolean makeInf) {
        cont = makeInf;
        env.setParallelism(numberOfPartitions);
        DataStream<Edge<Long, String>> edges = env.readTextFile(filepath)
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
                        new GradoopId(edge.f0.getSource().intValue(),0, (short) 0, 0), //src
                        new GradoopId(edge.f0.getTarget().intValue(),0, (short) 0, 0), //trg
                        Properties.createFromMap(properties),
                        graphId,
                        currentTime, //validFrom
                        Long.MAX_VALUE //validTo
                );
            }
        });
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
                cont = false;
            }
        };
        DataStream<TemporalEdge> makeInfinite = env.addSource(infinite);
        DataStream<TemporalEdge> finalEdges = tempEdges.union(makeInfinite)
                .filter(new FilterFunction<TemporalEdge>() {
                    @Override
                    public boolean filter(TemporalEdge edge) throws Exception {
                        return edge.getId() != null;
                    }
                });
        return new SimpleTemporalEdgeStream(finalEdges, env, graphId);
    }

    public void stopInfiniteStream() {
        cont = false;
        //infinite.cancel();
    }

    public FennelPartitioning getFennel() {
        return fennel;
    }

}
