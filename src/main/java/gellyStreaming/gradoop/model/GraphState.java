package gellyStreaming.gradoop.model;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;

public class GraphState implements Serializable {

    private final KeyedStream<TemporalEdge, Integer> input;


    public GraphState(KeyedStream<TemporalEdge, Integer> input, String strategy) {
        this.input = input;
        switch (strategy) {
            case "EL": input.map(new createEdgeList()).writeAsText("out", FileSystem.WriteMode.OVERWRITE);
            case "EL2" : input.process(new createEdgeList2()).writeAsText("out", FileSystem.WriteMode.OVERWRITE);
        }
    }

    public GraphState(KeyedStream<TemporalEdge, Integer> input, String strategy,
                      Time windowSize, Time slide) {
        this.input = input;
        switch (strategy) {
            //case "EL" :
        }
    }


    public KeyedStream<TemporalEdge, Integer> getData() {
        return input;
    }

    //public MapState<GradoopId, HashSet<TemporalEdge>> getState() {
      //  return sortedEdgeList;
    //}

    private class createWindowEdgeList extends ProcessWindowFunction<TemporalEdge, String, Integer, Window> {

        @Override
        public void process(Integer integer, Context context, Iterable<TemporalEdge> iterable, Collector<String> collector) throws Exception {

        }
    }

    private class createEdgeList2 extends KeyedProcessFunction<Integer, TemporalEdge, String> {
        private transient MapState<GradoopId, HashMap<GradoopId, TemporalEdge>> sortedEdgeList;
        private transient MapStateDescriptor<GradoopId, HashMap<GradoopId, TemporalEdge>> ELdescriptor;

        @Override
        public void open(Configuration parameters) throws Exception {
            ELdescriptor =
                    new MapStateDescriptor<>(
                            "edgeList",
                            TypeInformation.of(new TypeHint<GradoopId>() {}),
                            TypeInformation.of(new TypeHint<HashMap<GradoopId, TemporalEdge>>() {})
                    );
            sortedEdgeList = getRuntimeContext().getMapState(ELdescriptor);        }

        @Override
        public void processElement(TemporalEdge edge, Context context, Collector<String> collector) throws Exception {
            if(!sortedEdgeList.contains(edge.getSourceId())) {
                sortedEdgeList.put(edge.getSourceId(), new HashMap<GradoopId, TemporalEdge>());
            }
            if(!sortedEdgeList.get(edge.getSourceId()).containsKey(edge.getTargetId())) {
                sortedEdgeList.get(edge.getSourceId()).put(edge.getTargetId(),edge);
            } else {
                // What if edge with same src&trg get re-mentioned, perhaps with different
                // parameters/timestamps/properties. Keep newest for now.
                sortedEdgeList.get(edge.getSourceId()).put(edge.getTargetId(),edge);
            }
            collector.collect("Edge ("+edge.getSourceId()+","+edge.getTargetId()+"), with timestamp " +
                    edge.getValidFrom() + " and properties "+
                    edge.getProperties().toString() + " added");
        }
    }

    private class createEdgeList extends RichMapFunction<TemporalEdge, String> {
        private transient MapState<GradoopId, HashSet<TemporalEdge>> sortedEdgeList;
        private transient MapStateDescriptor<GradoopId, HashSet<TemporalEdge>> ELdescriptor;
        @Override
        public void open(Configuration parameters) throws Exception {
            ELdescriptor =
                    new MapStateDescriptor<>(
                            "edgeList",
                            TypeInformation.of(new TypeHint<GradoopId>() {}),
                            TypeInformation.of(new TypeHint<HashSet<TemporalEdge>>() {})
                    );
            sortedEdgeList = getRuntimeContext().getMapState(ELdescriptor);
        }

        @Override
        public String map(TemporalEdge edge) throws Exception {
            if(!sortedEdgeList.contains(edge.getSourceId())) {
                sortedEdgeList.put(edge.getSourceId(), new HashSet<TemporalEdge>());
            }
            sortedEdgeList.get(edge.getSourceId()).add(edge);
            return "Edge ("+edge.getSourceId()+","+edge.getTargetId()+"), with timestamp " +
                    edge.getValidFrom() + " and properties "+
                    edge.getProperties().toString() + " added";
        }
    }
}
