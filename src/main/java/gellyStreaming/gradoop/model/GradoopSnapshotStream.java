package gellyStreaming.gradoop.model;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopConfig;

import java.sql.Time;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class GradoopSnapshotStream {

    private WindowedStream<TemporalEdge, GradoopId, TimeWindow> windowedStream;
    

    public GradoopSnapshotStream(WindowedStream<TemporalEdge, GradoopId, TimeWindow> stream, String strategy) {
        this.windowedStream = stream;

        switch (strategy) {
            case "CSR":
                createCSR();
                break;
            case "EL":
                windowedStream.process(new createEL2()).print();
                //edges.print();
                break;
            case "AL":
                createAL();
                break;
            default:
                throw new IllegalArgumentException("Illegal state strategy, choose CSR, AL, or EL.");
        }
    }
    
    private void createCSR() {
        
    }

    public class SetInWindow {
        public GradoopId key;
        public Set<TemporalEdge> adjacentEdges;
    }

    // TODO: problem : not serializable
    private class createEL2 extends ProcessWindowFunction<TemporalEdge, String, GradoopId, TimeWindow> {
        private ValueStateDescriptor<SetInWindow> state = new ValueStateDescriptor<SetInWindow>
                ("myStateDescriptor", SetInWindow.class);


        @Override
        public void clear(Context context) throws Exception {
            ValueState<SetInWindow> previous = context.windowState().getState(state);
            previous.clear();
            super.clear(context);
        }

        @Override
        public void process(GradoopId gradoopId, Context context, Iterable<TemporalEdge> iterable, Collector<String> collector) throws Exception {
            SetInWindow oldState = context.windowState().getState(state).value();
            if(oldState == null) {
                oldState = new SetInWindow();
                oldState.key = gradoopId;
            }
            for(TemporalEdge edge : iterable) {
                oldState.adjacentEdges.add(edge);
            }
            context.windowState().getState(state).update(oldState);
            collector.collect("Vertex "+gradoopId.toString()+" has "+ oldState.adjacentEdges.size()+" edges in window "
            +context.window().toString());
        }
    }

    //public Set<TemporalEdge> edges;

    private class createEL extends ProcessWindowFunction<TemporalEdge, Object, GradoopId, TimeWindow> {
        @Override
        public void process(GradoopId key, Context context, Iterable<TemporalEdge> iterable, Collector<Object> collector) throws Exception {
            Set<TemporalEdge> edges1 = new HashSet<>();
            //iterable.forEach(temporalEdge -> edges1.add(temporalEdge));
            for (TemporalEdge edge : iterable) {
                edges1.add(edge);
                System.out.println(edge.getSourceId() + " to "+edge.getTargetId()+" : "+edge.toString());
            }
            System.out.println(edges1.size());
            System.out.println(context.window().toString());
            System.out.println("___________________");
            //edges=edges1;
        }
    }
    
    //private DataStream<Tuple2<Set<TemporalEdge>, String>> createELIncrementalProcessing() {
       // return this.windowedStream.aggregate(new myAggregateFunction(), new myProcessFunction());
    //}

    private static class myAggregateFunction implements AggregateFunction<TemporalEdge, Set<TemporalEdge>, Set<TemporalEdge>> {

        @Override
        public Set<TemporalEdge> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public Set<TemporalEdge> add(TemporalEdge temporalEdge, Set<TemporalEdge> set) {
            set.add(temporalEdge);
            return set;
        }

        @Override
        public Set<TemporalEdge> getResult(Set<TemporalEdge> set) {
            return set;
        }

        @Override
        public Set<TemporalEdge> merge(Set<TemporalEdge> set, Set<TemporalEdge> acc1) {
            set.addAll(acc1);
            return set;
        }
    }

    private class myProcessFunction extends ProcessWindowFunction
            <Set<TemporalEdge>, Tuple2<Set<TemporalEdge>, String>, Tuple2<GradoopId, GradoopId>, TimeWindow> {

        @Override
        public void process(Tuple2<GradoopId, GradoopId> key, Context context, Iterable<Set<TemporalEdge>> iterable, Collector<Tuple2<Set<TemporalEdge>, String>> collector) throws Exception {
            collector.collect(Tuple2.of(iterable.iterator().next(), context.window().toString()));
        }
    }
    
    private void createAL() {
        
    }
    
    
}
