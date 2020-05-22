package gellyStreaming.gradoop.model;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.dispatcher.SingleJobJobGraphStore;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class GraphState implements Serializable {

    private final KeyedStream<TemporalEdge, Integer> input;
    private static StreamExecutionEnvironment env;
    private static Integer[] keys;
    private static QueryState QS;
    private static JobID jobID;


    public GraphState(KeyedStream<TemporalEdge, Integer> input, String strategy) {
        this.input = input;
        switch (strategy) {
            case "EL": input.map(new createEdgeList()).writeAsText("out", FileSystem.WriteMode.OVERWRITE);
            case "EL2" : input.process(new createEdgeList2()).print();
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + strategy);
        }
    }


    // State in windows using Incremental Window Aggregation with Aggregate function.
    // Not in use currently.
    public GraphState(KeyedStream<TemporalEdge, Integer> input, String strategy,
                      Long windowSize, Long slide) {
        this.input = input;

        switch (strategy) {
            case "EL":
               input
                       .window(SlidingEventTimeWindows.of(
                               org.apache.flink.streaming.api.windowing.time.Time.of(windowSize, TimeUnit.SECONDS),
                               org.apache.flink.streaming.api.windowing.time.Time.of(slide, TimeUnit.SECONDS)))
                       .aggregate(new SetAggregate(), new Processor())
               .writeAsText("out", FileSystem.WriteMode.OVERWRITE)
               ;
        }
    }

    // State in windows using Incremental Window Aggregation with Aggregate function.
    public GraphState(StreamGraph jobID, StreamExecutionEnvironment env,
                      KeyedStream<TemporalEdge, Integer> input,
                      String strategy,
                      org.apache.flink.streaming.api.windowing.time.Time windowSize,
                      org.apache.flink.streaming.api.windowing.time.Time slide,
                      Integer numPartitions) throws UnknownHostException, InterruptedException {
        this.input = input;
        this.env = env;
        this.jobID = jobID.getJobGraph().getJobID();
        jobID.getJobGraph().setJobID(this.jobID);
        //this.QS = new QueryState(sg.getJobGraph().getJobID());
        KeyGen keyGenerator = new KeyGen(numPartitions,
                KeyGroupRangeAssignment.computeDefaultMaxParallelism(numPartitions));
        keys = new Integer[numPartitions];
        for (int i = 0; i < numPartitions; i++)
            keys[i] = keyGenerator.next(i);
        Thread.sleep(10);
        QS = new QueryState(this.jobID);
        switch (strategy) {
            case "EL-event" :
                input
                    .window(SlidingEventTimeWindows.of(
                            windowSize,
                            slide))
                    .aggregate(new SetAggregate(), new Processor())
                    .writeAsText("out", FileSystem.WriteMode.OVERWRITE)
            ;
            case "EL-proc":
                input
                        .window(SlidingProcessingTimeWindows.of(
                                windowSize,
                                slide))
                        .aggregate(new SetAggregate(), new Processor())
                        .writeAsText("out", FileSystem.WriteMode.OVERWRITE)
                ;
        }
    }

    public GraphState(StreamExecutionEnvironment env, KeyedStream<TemporalEdge, Integer> input, String strategy,
                      Long windowSize, Long slide) {
        this.input = input;

        switch (strategy) {
            case "EL2" : input.process(new createEdgeList3(windowSize, slide))
                    .writeAsText("out", FileSystem.WriteMode.OVERWRITE);
            case "TTL" : input.process(new createEdgeList4(windowSize, slide))
                    .writeAsText("out", FileSystem.WriteMode.OVERWRITE);
        }
    }

    public static class createEdgeList4 extends KeyedProcessFunction<Integer, TemporalEdge, String> {
        private transient MapState<GradoopId, HashMap<GradoopId, TemporalEdge>> sortedEdgeList;
        private Long window;
        private Long slide;
        private transient ValueState<Long> lastOutput;
        private transient ValueState<Integer> totalEdges;
        //private StreamExecutionEnvironment env;

        public createEdgeList4(Long window, Long slide) {
            //this.env = env;
            this.window = window;
            this.slide = slide;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.milliseconds(window))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) //default
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) //default
                    .build();
            MapStateDescriptor<GradoopId, HashMap<GradoopId, TemporalEdge>> ELdescriptor = new MapStateDescriptor<>(
                    "edgeList",
                    TypeInformation.of(new TypeHint<GradoopId>() {
                    }),
                    TypeInformation.of(new TypeHint<HashMap<GradoopId, TemporalEdge>>() {
                    })
            );
            ELdescriptor.enableTimeToLive(ttlConfig);
            sortedEdgeList = getRuntimeContext().getMapState(ELdescriptor);
            ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<Long>(
                    "lastOutputTime", Long.class);
            lastOutput = getRuntimeContext().getState(descriptor);
            ValueStateDescriptor<Integer> descriptor1 = new ValueStateDescriptor<Integer>(
                    "total edges", Integer.class
            );
            totalEdges = getRuntimeContext().getState(descriptor1);
        }

        @Override
        public void processElement(TemporalEdge edge, Context context, Collector<String> collector) throws Exception {
            if(lastOutput.value() == null) {
                lastOutput.update(context.timerService().currentProcessingTime());
                context.timerService().registerProcessingTimeTimer(lastOutput.value()+slide);
            }
            while(context.timerService().currentProcessingTime()>(lastOutput.value()+slide)) {
                lastOutput.update(lastOutput.value()+slide);
                context.timerService().registerProcessingTimeTimer(lastOutput.value()+slide);
            }
            try {
                sortedEdgeList.get(edge.getSourceId()).put(edge.getTargetId(), edge);
            } catch (NullPointerException e) {
                HashMap<GradoopId, TemporalEdge> toPut = new HashMap<>();
                toPut.put(edge.getTargetId(), edge);
                sortedEdgeList.put(edge.getSourceId(), toPut);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            AtomicInteger counter = new AtomicInteger();
            long beginWindow = timestamp - window;
            //List<Long> processingTimes = new LinkedList<>();
            List<GradoopId> keys =
                    StreamSupport.stream(sortedEdgeList.keys().spliterator(), false)
                            .collect(Collectors.toList());
            for(GradoopId key : keys) {
                //out.collect(Tuple2.of(key, ctx.getCurrentKey()));
                try {
                    Set<GradoopId> trgkeys = sortedEdgeList.get(key).keySet();
                    for (GradoopId key2 : trgkeys) {
                        //out.collect(Tuple2.of(key2, ctx.getCurrentKey()));
                        counter.getAndIncrement();
                    }
                } catch (NullPointerException ignored) {}
            }
            //for (GradoopId srcId : keys) {
            //    counter++;
                //try {
                //    for(GradoopId trgId : sortedEdgeList.get(srcId).keySet()) {
                //        processingTimes.add(sortedEdgeList.get(srcId).get(trgId).getTxFrom());
                //    }
                //} catch (NullPointerException ignored) {}
            //}
            int oldCounter = 0;
            try{
                    oldCounter = (int)totalEdges.value();}
            catch (NullPointerException ignored) {}
            int newValue = oldCounter + counter.intValue();
            totalEdges.update(newValue);
            out.collect("The window " + beginWindow + " until " + timestamp + " contained " + counter +
                    " edges");//, and the following processing times: "+ processingTimes.toString());
            out.collect("Total edges so far: "+newValue);

        }
    }

    public static class createEdgeList3 extends KeyedProcessFunction<Integer, TemporalEdge, String> {

        private transient MapState<GradoopId, HashMap<GradoopId, TemporalEdge>> sortedEdgeList;
        private transient ValueState<Long> startCurrentWindow;
        private final Long window;
        private final Long slide;

        public createEdgeList3(Long window, Long slide) {
            this.window = window;
            this.slide = slide;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<GradoopId, HashMap<GradoopId, TemporalEdge>> ELdescriptor = new MapStateDescriptor<>(
                    "edgeList",
                    TypeInformation.of(new TypeHint<GradoopId>() {
                    }),
                    TypeInformation.of(new TypeHint<HashMap<GradoopId, TemporalEdge>>() {
                    })
            );
            sortedEdgeList = getRuntimeContext().getMapState(ELdescriptor);
            ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<Long>(
                    "lastOutputTime", Long.class);
            startCurrentWindow = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(TemporalEdge edge, Context context, Collector<String> collector) throws Exception {
            if (startCurrentWindow.value() == null) {
                startCurrentWindow.update(context.timestamp());
                context.timerService().registerEventTimeTimer(startCurrentWindow.value()+window);
            }
            while(context.timestamp() >= startCurrentWindow.value()+slide) {
                startCurrentWindow.update(startCurrentWindow.value()+slide);
                context.timerService().registerEventTimeTimer(startCurrentWindow.value()+window);
            }

            if (!sortedEdgeList.contains(edge.getSourceId())) {
                sortedEdgeList.put(edge.getSourceId(), new HashMap<GradoopId, TemporalEdge>());
            }
            sortedEdgeList.get(edge.getSourceId()).put(edge.getTargetId(), edge);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            long startWindow = timestamp - window;
            List<Tuple2<GradoopId, GradoopId>> toRemove = new LinkedList<Tuple2<GradoopId, GradoopId>>();
            Iterable<GradoopId> keys = sortedEdgeList.keys();
            for (GradoopId srcId : keys) {
                ConcurrentHashMap<GradoopId, TemporalEdge> adjacentEdges =
                        new ConcurrentHashMap<>(sortedEdgeList.get(srcId));
                for (GradoopId trgId : adjacentEdges.keySet()) {
                    TemporalEdge edge = adjacentEdges.get(trgId);
                    if (edge.getValidFrom() < startWindow ) {
                        toRemove.add(Tuple2.of(srcId, trgId));
                    }
                }
            }
            //sortedEdgeList.iterator().forEachRemaining();
            for (Tuple2<GradoopId, GradoopId> edge : toRemove) {
                sortedEdgeList.get(edge.f0).remove(edge.f1);
                if (sortedEdgeList.get(edge.f0).isEmpty()) {
                    sortedEdgeList.remove(edge.f0);
                }
            }
            MapState<GradoopId, HashMap<GradoopId, TemporalEdge>> windowState = sortedEdgeList;
            toRemove = new LinkedList<>();
            for (GradoopId srcId : windowState.keys()) {
                ConcurrentHashMap<GradoopId, TemporalEdge> adjacentEdges =
                        new ConcurrentHashMap<>(windowState.get(srcId));
                for (GradoopId trgId : adjacentEdges.keySet()) {
                    TemporalEdge edge = adjacentEdges.get(trgId);
                    if (edge.getValidFrom() > timestamp ) {
                        toRemove.add(Tuple2.of(srcId, trgId));
                    }
                }
            }
            for (Tuple2<GradoopId, GradoopId> edge : toRemove) {
                windowState.get(edge.f0).remove(edge.f1);
                if (windowState.get(edge.f0).isEmpty()) {
                    windowState.remove(edge.f0);
                }
            }
             /*
            for(GradoopId srcId : sortedEdgeList.keys()) {
                for(GradoopId trgId : sortedEdgeList.get(srcId).keySet()) {
                    if(sortedEdgeList.get(srcId).get(trgId).getValidFrom() < startWindow ||
                            sortedEdgeList.get(srcId).get(trgId).getValidFrom() > timestamp) {
                        sortedEdgeList.get(srcId).remove(trgId);
                        if(sortedEdgeList.get(srcId).isEmpty()) {
                            sortedEdgeList.remove(srcId);
                        }
                    }
                }
            }
             */
            int counter = 0;
            List<GradoopId> edges = new LinkedList();
            for (GradoopId srcId : windowState.keys()) {
                counter++;
                edges.add(srcId);
            }
            out.collect("The window " + startWindow + " until " + timestamp + " contained " + counter +
                            " sourcevertices, being "+ edges.toString()
                    );
        }
    }

    public static class SetAggregate implements AggregateFunction<TemporalEdge,
            Map<GradoopId, HashMap<GradoopId, TemporalEdge>>,
            Map<GradoopId, HashMap<GradoopId, TemporalEdge>>> {

        @Override
        public Map<GradoopId, HashMap<GradoopId, TemporalEdge>> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public Map<GradoopId, HashMap<GradoopId, TemporalEdge>> add(
                TemporalEdge edge,
                Map<GradoopId, HashMap<GradoopId, TemporalEdge>> state) {
            if(!state.containsKey(edge.getSourceId())) {
                state.put(edge.getSourceId(), new HashMap<GradoopId, TemporalEdge>());
            }
            state.get(edge.getSourceId()).put(edge.getTargetId(),edge);
            return state;
        }

        @Override
        public Map<GradoopId, HashMap<GradoopId, TemporalEdge>> getResult(
                Map<GradoopId, HashMap<GradoopId, TemporalEdge>> state) {
            return state;
        }

        @Override
        public Map<GradoopId, HashMap<GradoopId, TemporalEdge>> merge(
                Map<GradoopId, HashMap<GradoopId, TemporalEdge>> state,
                Map<GradoopId, HashMap<GradoopId, TemporalEdge>> acc) {
            state.putAll(acc);
            return state;
        }
    }

    public static transient MapStateDescriptor<GradoopId, HashMap<GradoopId, TemporalEdge>> ELdescriptor;

    public static class Processor<Integer> extends ProcessWindowFunction<Map<GradoopId, HashMap<GradoopId, TemporalEdge>>, String, Integer, Window> {

            private transient MapState<GradoopId, HashMap<GradoopId, TemporalEdge>> sortedEdgeList;
            private transient ValueState<java.lang.Integer> totalEdges;

            @Override
            public void open(Configuration parameters) throws Exception {
                ELdescriptor = new MapStateDescriptor<>(
                        "edgeList",
                        TypeInformation.of(new TypeHint<GradoopId>() {}),
                        TypeInformation.of(new TypeHint<HashMap<GradoopId, TemporalEdge>>() {})
                );
                ELdescriptor.setQueryable("edgeList");
                sortedEdgeList = getRuntimeContext().getMapState(ELdescriptor);
                ValueStateDescriptor<java.lang.Integer> descriptor = new ValueStateDescriptor<java.lang.Integer>(
                        "totalEdges",
                        TypeInformation.of(new TypeHint<java.lang.Integer>() {})
                );
                totalEdges = getRuntimeContext().getState(descriptor);
                //QS = new QueryState(env.getStreamGraph("myTests").getJobGraph().getJobID());
            }

            @Override
            public void clear(Context context) throws Exception {
                getRuntimeContext().getMapState(ELdescriptor).clear();
                super.clear(context);
            }

            @Override
            public void process(Integer key,
                                Context context,
                                Iterable<Map<GradoopId, HashMap<GradoopId, TemporalEdge>>> iterable,
                                Collector<String> collector) throws Exception {
                iterable.forEach(x ->
                {
                    try {
                        sortedEdgeList.putAll(x);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                AtomicInteger total = new AtomicInteger();
                AtomicInteger duplicates = new AtomicInteger();


                for(GradoopId srcId : sortedEdgeList.keys()) {
                    for(java.lang.Integer otherkey : keys) {
                        if(otherkey != key) {
                            //QS = new QueryState(env.getStreamGraph("myTests").getJobGraph().getJobID());
                            if(QS.getSrcVertex(otherkey, srcId) != null) {
                                duplicates.getAndIncrement();
                                break;
                            }
                        }

                    }
                    total.getAndIncrement();
                }
                collector.collect("We found "+total+" sourceVertices in this partition of which " +
                        duplicates+" are also in other partitions.");

                /*
            //Used to check if all edges get properly added to state.
                AtomicInteger counter = new AtomicInteger();
                //Collection<String> edges = new ArrayList<>();
                for(GradoopId srcId : sortedEdgeList.keys()) {
                    HashMap<GradoopId, TemporalEdge> values = sortedEdgeList.get(srcId);
                    for(GradoopId trg: values.keySet()){
                        counter.incrementAndGet();
                        //edges.add(sortedEdgeList.get(srcId).get(trg).toString());
                    }
                }
                collector.collect("At "+context.window().toString()+" the state has "
                        + counter + " edges");//, being: "+edges.toString());
                int oldValue = 0;
                try {
                    oldValue = (int) totalEdges.value();
                } catch (NullPointerException ignored) { }
                int newValue = oldValue + counter.intValue();
                totalEdges.update(newValue);
                collector.collect("Total edges so far: "+newValue);
                Thread.sleep(100);
            */
            //collector.collect("We ran the process function");
        }

    }

    public KeyedStream<TemporalEdge, Integer> getData() {
        return this.input;
    }

    //public MapState<GradoopId, HashSet<TemporalEdge>> getState() {
      //  return sortedEdgeList;
    //}


    private class createEdgeList2 extends KeyedProcessFunction<Integer, TemporalEdge, MapState<GradoopId, HashMap<GradoopId, TemporalEdge>>> {
        private transient MapState<GradoopId, HashMap<GradoopId, TemporalEdge>> sortedEdgeList;
        private transient ValueState<Long> startCurrentWindow;
        long windowsize = 100000;
        long slidesize = 10000;

        @Override
        public void open(Configuration parameters) throws Exception {

            MapStateDescriptor<GradoopId, HashMap<GradoopId, TemporalEdge>> ELdescriptor = new MapStateDescriptor<>(
                    "edgeList",
                    TypeInformation.of(new TypeHint<GradoopId>() {}),
                    TypeInformation.of(new TypeHint<HashMap<GradoopId, TemporalEdge>>() {})
            );
            sortedEdgeList = getRuntimeContext().getMapState(ELdescriptor);
            ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<Long>(
                    "lastOutputTime", Long.class);
            startCurrentWindow = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(TemporalEdge edge, Context context, Collector<MapState<GradoopId, HashMap<GradoopId, TemporalEdge>>> collector) throws Exception {
            edge.setValidTo(edge.getValidFrom()+ windowsize);
            Long startWindow = startCurrentWindow.value();
            if(startWindow == null) {
                startCurrentWindow.update(edge.getValidFrom());
                context.timerService().registerEventTimeTimer(startCurrentWindow.value()+windowsize);
            }
            while(startCurrentWindow.value()+slidesize < edge.getValidFrom()) {
                startCurrentWindow.update(startCurrentWindow.value()+slidesize);
                context.timerService().registerEventTimeTimer(startCurrentWindow.value()+windowsize);
            }
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

            //collector.collect("Edge ("+edge.getSourceId()+","+edge.getTargetId()+"), with timestamp " +
            //        edge.getValidFrom() + " and properties "+
            //        edge.getProperties().toString() + " added");
        }
        // Never gets called??
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<MapState<GradoopId, HashMap<GradoopId, TemporalEdge>>> out) throws Exception {
            long beginWindow = timestamp - windowsize;
            MapState<GradoopId, HashMap<GradoopId, TemporalEdge>> state = sortedEdgeList;
            for(GradoopId srcId: state.keys()) {
                HashMap<GradoopId, TemporalEdge> adjacentEdges = state.get(srcId);
                for(GradoopId trgId: adjacentEdges.keySet()) {
                    TemporalEdge edge = adjacentEdges.get(trgId);
                    if(edge.getValidTo() < beginWindow) {
                        state.get(srcId).remove(trgId);
                    }
                }
            }
            out.collect(state);
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
