package gellyStreaming.gradoop.model;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;


/**
 *
 * Represents a graph stream where the stream consists solely of {@link org.apache.flink.graph.Edge edges}.
 * <p>
 *
 * @see org.apache.flink.graph.Edge
 *
 * @param <K> the key type for edge and vertex identifiers.
 * @param <EV> the value type for edges.
 */
public class SimpleEdgeStream<K, EV> extends GraphStream<K, NullValue, EV> {

    private final StreamExecutionEnvironment context;
    private final DataStream<Edge<K, EV>> edges;

    /**
     * Creates a graph from an edge stream.
     * The time characteristic is set to ingestion time  by default.
     *
     * @see {@link org.apache.flink.streaming.api.TimeCharacteristic}
     *
     * @param edges a DataStream of edges.
     * @param context the flink execution environment.
     */
    public SimpleEdgeStream(DataStream<Edge<K, EV>> edges, StreamExecutionEnvironment context) {
        context.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        this.edges = edges;
        this.context = context;
    }

    /**
     * Creates a graph from an edge stream operating in event time specified by timeExtractor .
     *
     * The time characteristic is set to event time.
     *
     * @see {@link org.apache.flink.streaming.api.TimeCharacteristic}
     *
     * @param edges a DataStream of edges.
     * @param timeExtractor the timestamp extractor.
     * @param context the execution environment.
     */
    public SimpleEdgeStream(DataStream<Edge<K, EV>> edges, AscendingTimestampExtractor<Edge<K,EV>> timeExtractor,
                            StreamExecutionEnvironment context) {
        context.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        this.edges = edges.assignTimestampsAndWatermarks(timeExtractor);
        this.context = context;
    }

/**
 * @return the flink streaming execution environment.
 */
    public StreamExecutionEnvironment getContext() {
        return this.context;
    }

    private static final class EmitSrcAndTarget<K, EV>
            implements FlatMapFunction<Edge<K, EV>, Vertex<K, NullValue>> {
        @Override
        public void flatMap(Edge<K, EV> edge, Collector<Vertex<K, NullValue>> out) {
            out.collect(new Vertex<>(edge.getSource(), NullValue.getInstance()));
            out.collect(new Vertex<>(edge.getTarget(), NullValue.getInstance()));
        }
    }

    private static final class FilterDistinctVertices<K>
            implements FilterFunction<Vertex<K, NullValue>> {
        Set<K> keys = new HashSet<>();

        @Override
        public boolean filter(Vertex<K, NullValue> vertex) throws Exception {
            if (!keys.contains(vertex.getId())) {
                keys.add(vertex.getId());
                return true;
            }
            return false;
        }
    }

/**
 * @return the vertex DataStream.
 */
@Override
    public DataStream<Vertex<K, NullValue>> getVertices() {
        return this.edges
        .flatMap(new EmitSrcAndTarget<K, EV>())
        .keyBy(0)
        .filter(new FilterDistinctVertices<K>());
    }

/**
 * @return the edge DataStream.
 */
    public DataStream<Edge<K, EV>> getEdges() {
        return this.edges;
    }
}