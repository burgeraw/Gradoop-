package gellyStreaming.gradoop.model;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopConfig;

import java.util.Iterator;

public class GradoopSnapshotStream {

    private WindowedStream<TemporalEdge, GradoopId, TimeWindow> windowedStream;

    public GradoopSnapshotStream(WindowedStream<TemporalEdge, GradoopId, TimeWindow> stream, String strategy) {
        this.windowedStream = stream;
        switch (strategy) {
            case "CSR":
                ;
            case "EL":
                ;
            case "AL":
                //;
            default:
                throw new IllegalArgumentException("Illegal state strategy, choose CSR, AL, or EL.");
        }
    }


}
