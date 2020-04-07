package gellyStreaming.gradoop.model;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;


public class TemporalGraphStream<K, VV, EV> extends GraphStream<K, VV, EV> {

    private final StreamExecutionEnvironment context;
    //private final GradoopFlinkConfig gradoopConfig;
    private final DataStream<TemporalEdge> edges;
    private final DataStream<TemporalVertex> vertices;

    public TemporalGraphStream(StreamExecutionEnvironment context, DataStream<TemporalEdge> edges,
                               DataStream<TemporalVertex> vertices) {
        this.context = context;
        //this.gradoopConfig = gradoopConfig;
        this.edges = edges;
        this.vertices = vertices;
    }


    @Override
    public StreamExecutionEnvironment getContext() {
        return context;
    }

    //TODO figure out how to efficiently return vertices & edges since this errors
    @Override
    public DataStream<Vertex<K, VV>> getVertices() {
        return vertices.map((MapFunction<TemporalVertex, Vertex<K, VV>>) temporalVertex -> {
            return (Vertex<K, VV>) new Vertex<Integer, String>(Integer.parseInt(temporalVertex.getId().toString()),
                    temporalVertex.toString());
        });
    }

    public DataStream<TemporalVertex> getTempVertices() {
        return vertices;
    }

    public DataStream<TemporalEdge> getTempEdges() {
        return edges;
    }

    //FIXME
    @Override
    public DataStream<Edge<K, EV>> getEdges() {
        return edges.map(new MapFunction<TemporalEdge, Edge<K, EV>>() {
            @Override
            public Edge<K, EV> map(TemporalEdge temporalEdge) throws Exception {
                return new Edge<K, EV>((K)temporalEdge.getSourceId(), (K)temporalEdge.getTargetId(),
                        (EV)temporalEdge.toString());
            }
        });
    }

    public DataStreamSink<TemporalEdge> printEdges() {
        return this.edges.print();
    }

    public DataStreamSink<TemporalVertex> printVertices() {return this.vertices.print();}

    @Override
    public GraphStream<K, VV, EV> distinct() {
        return null;
    }

    @Override
    public DataStream<Vertex<K, Long>> getDegrees() throws Exception {
        return null;
    }

    @Override
    public DataStream<Vertex<K, Long>> getInDegrees() throws Exception {
        return null;
    }

    @Override
    public DataStream<Vertex<K, Long>> getOutDegrees() throws Exception {
        return null;
    }

    @Override
    public DataStream<Long> numberOfEdges() {
        return this.edges.map(new MapFunction<TemporalEdge, Long>() {
            private long edgeCount = 0;
            @Override
            public Long map(TemporalEdge temporalEdge) throws Exception {
                edgeCount++;
                return edgeCount;
            }
        }).setParallelism(1);
    }

    @Override
    public DataStream<Long> numberOfVertices() {
        return this.vertices.flatMap(new FlatMapFunction<TemporalVertex, Long>() {
            private Set<GradoopId> vertices = new HashSet<>();
            @Override
            public void flatMap(TemporalVertex temporalVertex, Collector<Long> collector) throws Exception {
                vertices.add(temporalVertex.getId());
                collector.collect((long) vertices.size());
            }
        }).setParallelism(1);
    }

    @Override
    public GraphStream<K, VV, EV> undirected() {
        return null;
    }

    @Override
    public GraphStream<K, VV, EV> reverse() {
        return null;
    }

    @Override
    public DataStream aggregate(SummaryAggregation summaryAggregation) {
        return null;
    }
    @Override
    public GraphStream filterEdges(FilterFunction filter) {
        return null;
    }

    @Override
    public GraphStream filterVertices(FilterFunction filter) {
        return null;
    }

    @Override
    public GraphStream mapEdges(MapFunction mapper) {
        return null;
    }
}
