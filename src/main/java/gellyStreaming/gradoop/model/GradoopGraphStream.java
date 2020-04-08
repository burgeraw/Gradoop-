package gellyStreaming.gradoop.model;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public abstract class GradoopGraphStream<TemporalGraphHead, TemporalVertex, TemporalEdge> {

    public abstract StreamExecutionEnvironment getContext();

    // TODO : prettier solution?
    public abstract DataStream<?> getVertices();

    public abstract DataStream<TemporalEdge> getEdges();

    // TODO : prettier solution?
    public abstract DataStream<?> getGraphHead();

    public abstract GradoopGraphStream<TemporalGraphHead, TemporalVertex, TemporalEdge> distinct();

    public abstract DataStream<TemporalVertex> getDegrees();

    public abstract DataStream<TemporalVertex> getInDegrees();

    public abstract DataStream<TemporalVertex> getOutDegrees();

    public abstract DataStream<Long> numberOfEdges();

    public DataStream<Long> numberOfVertices() {
        return null;
    }

    public abstract GradoopGraphStream<TemporalGraphHead, TemporalVertex, TemporalEdge> undirected();

    public abstract GradoopGraphStream<TemporalGraphHead, TemporalVertex, TemporalEdge> reverse();

    //TODO also requires new SummaryAggregation class
    //public abstract <S extends Serializable, T> DataStream<T> aggregate(
    //        SummaryAggregation<K,EV,S,T> summaryAggregation);

    public abstract GraphStream<TemporalGraphHead, TemporalVertex, TemporalEdge> filterEdges(
            FilterFunction<TemporalVertex> filter);

    public abstract GraphStream<TemporalGraphHead, TemporalVertex, TemporalEdge> filterVertices(
            FilterFunction<TemporalEdge> filter);

    public abstract <NewEdgeType> GraphStream<TemporalGraphHead, TemporalVertex, NewEdgeType> mapEdges(
            final MapFunction<TemporalEdge, NewEdgeType> mapper);
}
