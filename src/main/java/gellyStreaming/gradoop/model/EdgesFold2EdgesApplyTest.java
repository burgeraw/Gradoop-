package gellyStreaming.gradoop.model;

import gellyStreaming.gradoop.algorithms.BipartitenessCheck;
import gellyStreaming.gradoop.algorithms.Candidates;
import gellyStreaming.gradoop.algorithms.NewBipartitenessCheck;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

public class EdgesFold2EdgesApplyTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        GraphStream<Long, NullValue, NullValue> graph = new SimpleEdgeStream<>(getEdgesDataSet(env), env);
        DataStream<Candidates> bipartition = graph.aggregate
                (new BipartitenessCheck<Long, NullValue>((long) 500));
        bipartition.print();

        graph = new SimpleEdgeStream<>(getEdgesDataSet(env), env);
        DataStream<Candidates> newBipartition = graph.aggregate
                (new NewBipartitenessCheck<Long, NullValue>((long) 500));
        newBipartition.print();
        env.execute("Bipartiteness Check");
    }


    @SuppressWarnings("serial")
    private static DataStream<Edge<Long, NullValue>> getEdgesDataSet(StreamExecutionEnvironment env) {
    		return env.generateSequence(1, 100).flatMap(
				new FlatMapFunction<Long, Edge<Long, NullValue>>() {
        @Override
        public void flatMap(Long key, Collector<Edge<Long, NullValue>> out) {
            for (int i = 0; i < 2; i++) {
                long target = key * 2 + 1;
                out.collect(new Edge<>(key, target, NullValue.getInstance()));
            }
        }
    });
}
}
