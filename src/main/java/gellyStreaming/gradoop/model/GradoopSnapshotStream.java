package gellyStreaming.gradoop.model;

import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

public class GradoopSnapshotStream {

    private WindowedStream<TemporalEdge, GradoopId, TimeWindow> windowedStream;

    GradoopSnapshotStream(WindowedStream<TemporalEdge, GradoopId, TimeWindow> stream) {
        this.windowedStream = stream;
    }

    


}
