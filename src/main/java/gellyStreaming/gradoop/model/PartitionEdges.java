package gellyStreaming.gradoop.model;

import gellyStreaming.gradoop.partitioner.CustomKeySelector;
import gellyStreaming.gradoop.partitioner.DBHPartitioner;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;

public class PartitionEdges<K, V> implements Serializable {

    public final DataStream<Tuple2<Edge<K, V>, Integer>> getPartitionedEdges(DataStream<Edge<K, V>> input, Integer numberOfPartitions) {
        return input
                .map(new MapFunction<Edge<K, V>, Tuple2<Edge<K,V>,Integer>>() {
                    final CustomKeySelector<K, V> keySelector = new CustomKeySelector<>(0);
                    final Partitioner<K> partitioner = new DBHPartitioner<>(keySelector, numberOfPartitions);
            @Override
            public Tuple2<Edge<K, V>, Integer> map(Edge<K, V> edge) throws Exception {
                K keyEdge = keySelector.getKey(edge);
                int machineId = partitioner.partition(keyEdge, numberOfPartitions);
                return Tuple2.of(edge, machineId);
            }
        });
    }
}


