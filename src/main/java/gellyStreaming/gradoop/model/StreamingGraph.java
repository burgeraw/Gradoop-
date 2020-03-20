package gellyStreaming.gradoop.model;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;

import java.math.BigInteger;

public class StreamingGraph {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<Edge<Long, Long>> edges = env
                .readTextFile("src/main/resources/ml-100k/u.data")
                .map(new MapFunction<String, Edge<Long, Long>>() {
                    @Override
                    public Edge<Long, Long> map(String s) throws Exception {
                        String[] args = s.split("\t");
                        long src = Long.parseLong(args[0]);
                        long trg = Long.parseLong(args[1]) + 1000000;
                        long val = Long.parseLong(args[2]) * 10;
                        return new Edge<>(src, trg, val);
                    }
                });

        GraphStream<Long, NullValue, Long> graph = new SimpleEdgeStream<>(edges, env);

        graph.getEdges().print();
        env.execute();
    }
}

/*

public class StreamingGraph {
    private int userID;
    private int itemID;
    private int rating;
    private BigInteger timestamp;

    public static StreamingGraph fromString(String line) {
        if (line == null || line.equals("")) throw new IllegalArgumentException("Invalid input string.");
        String[] elements = line.split("\t");
        StreamingGraph rating =  new StreamingGraph();
        this.userID = Integer.parseInt(elements[0]);
        this.itemID = Integer.parseInt(elements[1]);
        this.rating = Integer.parseInt(elements[2]);
        this.timestamp = new BigInteger(elements[3]);
        return rating;
    }


    public static void main(String[] args) {
        String filePath = "src/main/resources/ml-100k/u.data";
        StreamExecutionEnvironment STREAM_EXECUTION_ENVIRONMENT = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream = STREAM_EXECUTION_ENVIRONMENT.readTextFile(filePath);
        DataStream<StreamingGraph> streamingGraph = dataStream.map(StreamingGraph::fromString);

    }

}
*/