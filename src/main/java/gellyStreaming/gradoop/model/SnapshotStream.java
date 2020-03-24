package gellyStreaming.gradoop.model;

import java.util.Iterator;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class  SnapshotStream<K, EV> {

    private WindowedStream<Edge<K, EV>, K, TimeWindow> windowedStream;

    SnapshotStream(WindowedStream<Edge<K, EV>, K, TimeWindow> window) {
        this.windowedStream = window;
    }

    //TODO deprecated method rewrite, is this even useful?

    /**
     * Performs a neighborhood fold on the graph window stream.
     *
     * @param initialValue
     * @param foldFunction
     * @return the result stream after applying the user-defined fold operation on the window
     */
//*
    public <T> DataStream<T> foldNeighbors(T initialValue, final EdgesFold<K, EV, T> foldFunction) {
        return windowedStream.fold(initialValue, new EdgesFoldFunction<K, EV, T>(foldFunction));
    }


    @SuppressWarnings("serial")
    public static final class EdgesFoldFunction<K, EV, T>
            implements FoldFunction<Edge<K, EV>, T>, ResultTypeQueryable<T>
    {

        private final EdgesFold<K, EV, T> foldFunction;

        public EdgesFoldFunction(EdgesFold<K, EV, T> foldFunction) {
            this.foldFunction = foldFunction;
        }

        @Override
        public T fold(T accumulator, Edge<K, EV> edge) throws Exception {
            return foldFunction.foldEdges(accumulator, edge.getSource(), edge.getTarget(), edge.getValue());
        }

        @Override
        public TypeInformation<T> getProducedType() {
            return TypeExtractor.createTypeInfo(EdgesFold.class, foldFunction.getClass(), 2,
                    null, null);
        }
    }
//*/
    /**
     //	 * Performs an aggregation on the neighboring edges of each vertex on the graph window stream.
     * <p>
     * For each vertex, the transformation consecutively calls a
     * {@link EdgesReduce} function until only a single value for each edge remains.
     * The {@link EdgesReduce} function combines two edge values into one new value of the same type.
     *
     * @param reduceFunction the aggregation function
     * @return a result stream of Tuple2, containing one tuple per vertex.
     * The first field is the vertex ID and the second field is the final value,
     * after applying the user-defined aggregation operation on the neighborhood.
     */
    public DataStream<Tuple2<K, EV>> reduceOnEdges(final EdgesReduce<EV> reduceFunction) {
        return windowedStream.reduce(new EdgesReduceFunction<K, EV>(reduceFunction))
                .project(0, 2);
    }

    @SuppressWarnings("serial")
    public static final class EdgesReduceFunction<K, EV> implements ReduceFunction<Edge<K, EV>> {

        private final EdgesReduce<EV> reduceFunction;

        public EdgesReduceFunction(EdgesReduce<EV> reduceFunction) {
            this.reduceFunction = reduceFunction;
        }

        @Override
        public Edge<K, EV> reduce(Edge<K, EV> firstEdge, Edge<K, EV> secondEdge) throws Exception {
            EV reducedValue = this.reduceFunction.reduceEdges(firstEdge.getValue(), secondEdge.getValue());
            firstEdge.setValue(reducedValue);
            return firstEdge;
        }
    }

    /**
     * Performs a generic neighborhood aggregation in the graph window stream.
     * Each vertex can produce zero, one or more values from the computation on its neighborhood.
     *
     * @param applyFunction the neighborhood computation function
     * @return the result stream after applying the user-defined operation on the window
     */
    public <T> DataStream<T> applyOnNeighbors(final EdgesApply<K, EV, T> applyFunction) {
        return windowedStream.apply(new SnapshotFunction<>(applyFunction));
    }

    @SuppressWarnings("serial")
    public static final class SnapshotFunction<K, EV, T> implements
            WindowFunction<Edge<K, EV>, T, K, TimeWindow>, ResultTypeQueryable<T> {

        private final EdgesApply<K, EV, T> applyFunction;

        public SnapshotFunction(EdgesApply<K, EV, T> applyFunction) {
            this.applyFunction = applyFunction;
        }

        public void apply(K key, TimeWindow window, final Iterable<Edge<K, EV>> edges, Collector<T> out)
                throws Exception {

            final Iterator<Tuple2<K, EV>> neighborsIterator = new Iterator<Tuple2<K, EV>>() {

                final Iterator<Edge<K, EV>> edgesIterator = edges.iterator();

                @Override
                public boolean hasNext() {
                    return edgesIterator.hasNext();
                }

                @Override
                public Tuple2<K, EV> next() {
                    Edge<K, EV> nextEdge = edgesIterator.next();
                    return new Tuple2<K, EV>(nextEdge.f1, nextEdge.f2);
                }

                @Override
                public void remove() {
                    edgesIterator.remove();
                }
            };
            Iterable<Tuple2<K, EV>> neighborsIterable = new Iterable<Tuple2<K, EV>>() {
                public Iterator<Tuple2<K, EV>> iterator() {
                    return neighborsIterator;
                }
            };

            applyFunction.applyOnEdges(key, neighborsIterable, out);
        }

        @Override
        public TypeInformation<T> getProducedType() {
            return TypeExtractor.createTypeInfo(EdgesApply.class, applyFunction.getClass(), 2,
                    null, null);
        }
    }
}
