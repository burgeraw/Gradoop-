package gellyStreaming.gradoop.model;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public abstract class GradoopGraphStream<TemporalGraphHead, TemporalVertex, TemporalEdge> {

    public abstract StreamExecutionEnvironment getContext();

    public abstract DataStream<TemporalVertex> getVertices();

    public abstract DataStream<TemporalEdge> getEdges();

    public abstract DataStream<TemporalGraphHead> getGraphHead();

    public abstract GradoopGraphStream<TemporalGraphHead, TemporalVertex, TemporalEdge> distinct();

    public abstract DataStream<TemporalVertex> getDegrees();

    public abstract DataStream<TemporalVertex> getInDegrees();

    public abstract DataStream<TemporalVertex> getOutDegrees();

    public abstract DataStream<Long> numberOfEdges();

    public abstract DataStream<Long> numberOfVertices();

    public abstract GradoopGraphStream<TemporalGraphHead, TemporalVertex, TemporalEdge> undirected();

    public abstract GradoopGraphStream<TemporalGraphHead, TemporalVertex, TemporalEdge> reverse();

    //TODO also requires new SummaryAggregation class
    //public abstract <S extends Serializable, T> DataStream<T> aggregate(
    //        SummaryAggregation<K,EV,S,T> summaryAggregation);

    public abstract GradoopGraphStream<TemporalGraphHead, TemporalVertex, TemporalEdge> filterEdges(
            FilterFunction<TemporalEdge> filter);

    public abstract GradoopGraphStream<TemporalGraphHead, TemporalVertex, TemporalEdge> filterVertices(
            FilterFunction<TemporalVertex> filter);

    public abstract GradoopGraphStream<TemporalGraphHead, TemporalVertex, TemporalEdge> mapEdges(
            final MapFunction<TemporalEdge, TemporalEdge> mapper);
}
