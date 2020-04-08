package gellyStreaming.gradoop.model;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.GraphId;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

import java.util.HashMap;
import java.util.Map;

public class SimpleTemporalEdgeStream extends GradoopGraphStream<NullValue, NullValue, TemporalEdge> {

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

    //TODO doesn't work
    //How to get only earliest mention of vertex in edge file as timestamp
    @Override
    public DataStream<TemporalVertex> getVertices() {
        DataStream<TemporalEdge> edges = this.edges;
        return edges.flatMap(new FlatMapFunction<TemporalEdge, Tuple2<GradoopId, Long>>() {
            @Override
            public void flatMap(TemporalEdge temporalEdge, Collector<Tuple2<GradoopId, Long>> collector) throws Exception {
                collector.collect(Tuple2.of(temporalEdge.getSourceId(), temporalEdge.getValidFrom()));
                collector.collect(Tuple2.of(temporalEdge.getTargetId(), temporalEdge.getValidFrom()));
            }
        })
                .setParallelism(1)
                .keyBy(0)
                .minBy(1)
                .map(new MapFunction<Tuple2<GradoopId, Long>, TemporalVertex>() {
                    @Override
                    public TemporalVertex map(Tuple2<GradoopId, Long> tuple) throws Exception {
                        return new TemporalVertex(
                                tuple.f0,
                                null,
                                null,
                                null,
                                tuple.f1,
                                Long.MAX_VALUE);
                    }
                }).setParallelism(1);

        /*
        return edges.flatMap(
                new FlatMapFunction<TemporalEdge, Tuple3<String, Long, TemporalVertex>>() {
                    @Override
                    public void flatMap(TemporalEdge temporalEdge, Collector<Tuple3<String, Long, TemporalVertex>> collector) throws Exception {
                        GradoopId src = temporalEdge.getSourceId();
                        GradoopId trg = temporalEdge.getTargetId();
                        System.out.println(src.toString());
                        System.out.println(trg.toString());
                        Long validFrom = temporalEdge.getValidFrom();
                        collector.collect(new Tuple3<>(src.toString(),
                                validFrom,
                                new TemporalVertex(
                                src,
                                null,
                                null,
                                temporalEdge.getGraphIds(),
                                validFrom,
                                Long.MAX_VALUE)));
                        collector.collect(new Tuple3<>(trg.toString(),
                                validFrom,
                                new TemporalVertex(trg,
                                null,
                                null,
                                temporalEdge.getGraphIds(),
                                validFrom,
                                Long.MAX_VALUE)));
                    }
                }).keyBy(0)
                .minBy(1)
                .map(new MapFunction<Tuple3<String, Long, TemporalVertex>, TemporalVertex>() {
                    @Override
                    public TemporalVertex map(Tuple3<String, Long, TemporalVertex> tuple) throws Exception {
                        return tuple.f2;
                    }
                }).setParallelism(1);

                .reduce(new ReduceFunction<Tuple2<String, TemporalVertex>>() {
                    @Override
                    public Tuple2<String, TemporalVertex> reduce(Tuple2<String, TemporalVertex> t1, Tuple2<String, TemporalVertex> t2) throws Exception {
                        if(t1.f1.getValidFrom() <= t2.f1.getValidFrom()) {
                            System.out.println("it was smaller");
                            return t1;
                        } else {
                            return t2;
                        }
                    }
                }).map(new MapFunction<Tuple2<String, TemporalVertex>, TemporalVertex>() {
                    @Override
                    public TemporalVertex map(Tuple2<String, TemporalVertex> tuple) throws Exception {
                        return tuple.f1;
                    }
                }).setParallelism(1);

                 */
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
