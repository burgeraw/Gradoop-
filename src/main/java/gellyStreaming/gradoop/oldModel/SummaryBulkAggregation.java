package gellyStreaming.gradoop.oldModel;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

public class SummaryBulkAggregation<K, EV, S extends Serializable, T> extends SummaryAggregation<K, EV, S, T> {

    private static final long serialVersionUID = 1L;
    protected long timeMillis;


    public SummaryBulkAggregation(EdgesFold<K, EV, S> updateFun, ReduceFunction<S> combineFun,
                                  MapFunction<S, T> transformFun, S initialVal, long timeMillis, boolean transientState) {
        super(updateFun, combineFun, transformFun, initialVal, transientState);
        this.timeMillis = timeMillis;
    }

    public SummaryBulkAggregation(EdgesFold<K, EV, S> updateFun, ReduceFunction<S> combineFun,
                                  S initialVal, long timeMillis, boolean transientState) {
        this(updateFun, combineFun, null, initialVal, timeMillis, transientState);
    }
    //TODO depricated fold
    @SuppressWarnings("unchecked")
    @Override
    public DataStream<T> run(final DataStream<Edge<K, EV>> edgeStream) {

        //For parallel window support we key the edge stream by partition and apply a parallel fold per partition.
        //Finally, we merge all locally combined results into our final graph aggregation property.
        TupleTypeInfo edgeTypeInfo = (TupleTypeInfo) edgeStream.getType();
        TypeInformation<S> returnType = TypeExtractor.createTypeInfo(EdgesFold.class, getUpdateFun().getClass(),
                2, edgeTypeInfo.getTypeAt(0), edgeTypeInfo.getTypeAt(2));

        TypeInformation<Tuple2<Integer, Edge<K, EV>>> typeInfo = new TupleTypeInfo<>
                (BasicTypeInfo.INT_TYPE_INFO, edgeStream.getType());

        //Old method with fold
        DataStream<S> partialAggOld = edgeStream
                .map(new PartitionMapper<>()).returns(typeInfo)
                .keyBy(0)
                .timeWindow(Time.of(timeMillis, TimeUnit.MILLISECONDS))
                .fold(getInitialValue(), new PartialAggOld<>(getUpdateFun(),returnType))
                .timeWindowAll(Time.of(timeMillis, TimeUnit.MILLISECONDS))
                .reduce(getCombineFun())
                .flatMap(getAggregator(edgeStream)).setParallelism(1);

        // New method with aggregate
        DataStream<S> partialAgg = edgeStream
                .map(new PartitionMapper<>()).returns(typeInfo)
                .keyBy(0)
                .timeWindow(Time.of(timeMillis, TimeUnit.MILLISECONDS))
                .aggregate(new PartialAgg<>(getUpdateFun(), returnType, getInitialValue()))
                .timeWindowAll(Time.of(timeMillis, TimeUnit.MILLISECONDS))
                .reduce(getCombineFun())
                .flatMap(getAggregator(edgeStream)).setParallelism(1);

        if (getTransform() != null) {
            return partialAgg.map(getTransform());
        }

        return (DataStream<T>) partialAgg;
    }

    @SuppressWarnings("serial")
    protected static final class PartitionMapper<Y> extends RichMapFunction<Y, Tuple2<Integer, Y>> {

        private int partitionIndex;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.partitionIndex = getRuntimeContext().getIndexOfThisSubtask();
        }

        @Override
        public Tuple2<Integer, Y> map(Y state) throws Exception {
            return new Tuple2<>(partitionIndex, state);
        }
    }

    protected static final class PartialAgg<K, EV, S> implements
            ResultTypeQueryable<S>, AggregateFunction<Tuple2<Integer, Edge<K, EV>>, S, S> {

        private EdgesFold<K, EV, S> foldFunction;
        private TypeInformation<S> returnType;
        private S initialValue;

        public PartialAgg(EdgesFold<K, EV, S> foldFunction, TypeInformation<S> returnType, S initialValue) {
            this.foldFunction = foldFunction;
            this.returnType = returnType;
            this.initialValue = initialValue;
        }

        @Override
        public S createAccumulator() {
            return initialValue;
        }

        @Override
        public S add(Tuple2<Integer, Edge<K, EV>> o, S s) {
            try {
                return this.foldFunction.foldEdges(s, o.f1.getSource(), o.f1.getTarget(), o.f1.getValue());
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }

        @Override
        public S getResult(S acc) {
            return acc;
        }

        @Override
        public S merge(S o, S acc1) {
            return null;
        }

        @Override
        public TypeInformation<S> getProducedType() {
            return returnType;
        }
    }

    @SuppressWarnings("serial")
    protected static final class PartialAggOld<K, EV, S>
            implements ResultTypeQueryable<S>, FoldFunction<Tuple2<Integer, Edge<K, EV>>, S> {

        private EdgesFold<K, EV, S> foldFunction;
        private TypeInformation<S> returnType;

        public PartialAggOld(EdgesFold<K, EV, S> foldFunction, TypeInformation<S> returnType) {
            this.foldFunction = foldFunction;
            this.returnType = returnType;
        }

        @Override
        public S fold(S s, Tuple2<Integer, Edge<K, EV>> o) throws Exception {
            return this.foldFunction.foldEdges(s, o.f1.getSource(), o.f1.getTarget(), o.f1.getValue());
        }

        @Override
        public TypeInformation<S> getProducedType() {
            return returnType;
        }

    }
}
