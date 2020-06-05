package gellyStreaming.gradoop.partitioner;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.io.Serializable;

public class HashVertexPartitioner<V> implements Serializable {

    public final DataStream<Tuple2<Edge<Long, V>, Integer>> getPartitionedEdges(DataStream<Edge<Long, V>> input, Integer numberOfPartitions) {
        return input
                .flatMap(new FlatMapFunction<Edge<Long, V>, Tuple2<Edge<Long, V>, Integer>>() {
                    final VertexPartitionState2 state = new VertexPartitionState2(numberOfPartitions);

                    @Override
                    public void flatMap(Edge<Long, V> kvEdge, Collector<Tuple2<Edge<Long, V>, Integer>> collector) throws Exception {
                        int[] partitions = state.getPartition(kvEdge.f0, kvEdge.f1);
                        for (int partition : partitions) {
                            collector.collect(Tuple2.of(kvEdge, partition));
                        }
                    }
                });
    }

}
