package gellyStreaming.gradoop.model;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;

public class SimpleTemporalEdgeStream<TemporalEdge> extends GradoopGraphStream<NullValue, NullValue, TemporalEdge> {

    private final StreamExecutionEnvironment context;
    private final DataStream<TemporalEdge> edges;

    public SimpleTemporalEdgeStream(DataStream<TemporalEdge> edges, StreamExecutionEnvironment context) {
        this.context = context;
        this.edges = edges;
    }

    @Override
    public StreamExecutionEnvironment getContext() {
        return this.context;
    }

    //TODO
    //How to get only earliest mention of vertex in edge file as timestamp
    @Override
    public DataStream<NullValue> getVertices() {
        return null;
    }

    @Override
    public DataStream<TemporalEdge> getEdges() {
        return this.edges;
    }

    @Override
    public DataStream<NullValue> getGraphHead() {
        return null;
    }

    @Override
    public GradoopGraphStream<NullValue, NullValue, TemporalEdge> distinct() {
        return null;
    }

    @Override
    public DataStream<NullValue> getDegrees() {
        return null;
    }

    @Override
    public DataStream<NullValue> getInDegrees() {
        return null;
    }

    @Override
    public DataStream<NullValue> getOutDegrees() {
        return null;
    }

    @Override
    public DataStream<Long> numberOfEdges() {
        return null;
    }

    @Override
    public GradoopGraphStream<NullValue, NullValue, TemporalEdge> undirected() {
        return null;
    }

    @Override
    public GradoopGraphStream<NullValue, NullValue, TemporalEdge> reverse() {
        return null;
    }

    @Override
    public GraphStream<NullValue, NullValue, TemporalEdge> filterEdges(FilterFunction<NullValue> filter) {
        return null;
    }

    @Override
    public GraphStream<NullValue, NullValue, TemporalEdge> filterVertices(FilterFunction<TemporalEdge> filter) {
        return null;
    }

    @Override
    public <NewEdgeType> GraphStream<NullValue, NullValue, NewEdgeType> mapEdges(MapFunction<TemporalEdge, NewEdgeType> mapper) {
        return null;
    }
}
