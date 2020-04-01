package gellyStreaming.gradoop.util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;

public class MatchingEvent extends Tuple2<MatchingEvent.Type, Edge<Long, Long>> {

    public enum Type {ADD, REMOVE};

    public MatchingEvent() {}

    public MatchingEvent(MatchingEvent.Type type, Edge<Long, Long> edge) throws Exception {
        this.f0 = type;
        this.f1 = edge;
    }

    public MatchingEvent.Type getType() {
        return this.f0;
    }

    public Edge<Long, Long> getEdge() {
        return this.f1;
    }
}