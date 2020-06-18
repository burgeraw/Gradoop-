package gellyStreaming.gradoop.model;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class GraphState implements Serializable {

    private static KeyedStream<TemporalEdge, Integer> input;
    private static Integer[] keys;
    private static QueryState QS;
    private static boolean lazyPurging;
    private static int batchSize;
    private static Long windowSize;
    private static Long slide;
    private SingleOutputStreamOperator<Tuple3<Integer, Integer[], Long>> decoupledOutput = null;
    private SingleOutputStreamOperator<String> algorithmOutput = null;
    private static Algorithm algorithm;
    private static long firstTimestamp;

    public GraphState(QueryState QS,
                      KeyedStream<TemporalEdge, Integer> input,
                      String strategy,
                      Long windowSize,
                      Long slide,
                      Integer numPartitions,
                      Boolean lazyPurging,
                      int batchSize,
                      Algorithm algorithm) {
        GraphState.QS = QS;
        GraphState.input = input;
        GraphState.windowSize = windowSize;
        GraphState.slide = slide;
        KeyGen keyGenerator = new KeyGen(numPartitions,
                KeyGroupRangeAssignment.computeDefaultMaxParallelism(numPartitions));
        keys = new Integer[numPartitions];
        for (int i = 0; i < numPartitions; i++)
            keys[i] = keyGenerator.next(i);
        GraphState.lazyPurging = lazyPurging;
        GraphState.batchSize = batchSize;
        GraphState.algorithm = algorithm;
        GraphState.firstTimestamp = System.currentTimeMillis();

        if (algorithm == null) {
            switch (strategy) {
                //Example how to use decoupled output:
                case "triangle" :
                input.process(new ALdecoupled())
                            .keyBy(
                                    (KeySelector<Tuple3<Integer, Integer[], Long>, Integer>) integerLongTuple3 -> integerLongTuple3.f0)
                            .process(new TriangleCounter()).print();
                case "EL":
                    decoupledOutput = input.process(new ELDecoupled());
                    break;
                case "sortedEL":
                    decoupledOutput = input.process(new SortedELDecoupled());
                    break;
                case "AL":
                    decoupledOutput = input.process(new ALdecoupled());
                    break;
            }
        } else {
            switch (strategy) {
                case "vertices":
                    algorithmOutput = input.process(new CountingVertices(windowSize, slide));
                    break;
                case "triangles":
                    algorithmOutput = input.process(new CountTriangles(windowSize, slide));
                    break;
                case "EL":
                    algorithmOutput = input.process(new ELwithAlg());
                    break;
                case "sortedEL":
                    algorithmOutput = input.process(new SortedELwithAlg());
                    break;
                case "AL":
                    algorithmOutput = input.process(new ALwithAlg());
                    break;
            }
        }
    }


    public void overWriteQS(JobID jobID) throws UnknownHostException {
        QS.initialize(jobID);
    }

    public DataStream<Tuple3<Integer, Integer[], Long>> getDecoupledOutput() throws Exception {
        if(this.decoupledOutput == null) {
            throw new Exception("We have only algorithm output, no decoupled one. Set algorithm = null for " +
                    "decoupled output.");
        } else {
            return this.decoupledOutput;
        }
    }

    public DataStream<String> getAlgorithmOutput() throws Exception {
        if(this.algorithmOutput == null) {
            throw new Exception("We have only decoupled output, no algorithm one. Set algorithm != null for " +
                    "algorithm output.");
        } else {
            return this.algorithmOutput;
        }
    }

    //TODO rewrite to be in Algorithm format
    //TriangleCounter
    public static class TriangleCounter extends KeyedProcessFunction<Integer, Tuple3<Integer, Integer[], Long>, Integer> {

        @Override
        public void processElement(Tuple3<Integer, Integer[], Long> input, Context context, Collector<Integer> collector) throws Exception {
            System.out.println("The input triangle counter received is: "+input.toString());
            if (!GraphState.QS.isInitilized()) {
                throw new Exception("We don't have Queryable State initialized.");
            }
            AtomicInteger triangleCounter = new AtomicInteger(0);
            if (input.f0 != null) {
                long maxValidTo = input.f2;
                HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>> localAdjacencyList = new HashMap<>();
                int tries = 0;
                int maxTries = 10;
                for (int key : input.f1) {
                    tries = 0;
                    while (tries < maxTries ){//&& key != input.f0) {
                        try {
                            MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>> tempState =
                                    QS.getALState(key);
                            for (Long timestamp : tempState.keys()) {
                                if (timestamp <= maxValidTo) {
                                    for(GradoopId src : tempState.get(timestamp).keySet()) {
                                        if (!localAdjacencyList.containsKey(src)) {
                                            localAdjacencyList.put(src, new HashMap<>());
                                        }
                                        localAdjacencyList.get(src).putAll(tempState.get(timestamp).get(src));
                                    }
                                }
                            }
                            tries = maxTries;
                        } catch (Exception e) {
                            tries++;
                            if (tries >= maxTries) {
                                throw new Exception("We tried to get state " + maxTries + " times, but failed. ");
                            }
                        }
                    }
                }
                System.out.println("We got all states & now start counting triangles.");
                System.out.println("Size state: "+localAdjacencyList.entrySet().size());
                for (GradoopId srcId : localAdjacencyList.keySet()) {
                    if(GradoopIdUtil.getModulo(srcId, input.f0, input.f1)) {
                        Set<GradoopId> neighboursSet = localAdjacencyList.get(srcId).keySet();
                        GradoopId[] neighbours = neighboursSet.toArray(GradoopId[]::new);
                        for (int i = 0; i < neighbours.length; i++) {
                            GradoopId neighbour1 = neighbours[i];
                            if (neighbour1.compareTo(srcId) > 0) {
                                for (int j = 0; j < neighbours.length; j++) {
                                    GradoopId neighbour2 = neighbours[j];
                                    if (i != j && neighbour2.compareTo(neighbour1) > 0) {
                                        AtomicBoolean triangle = new AtomicBoolean(false);
                                        if (localAdjacencyList.containsKey(neighbour1)) {
                                            if (localAdjacencyList.get(neighbour1).containsKey(neighbour2)) {
                                                triangle.set(true);
                                            }
                                        }
                                        // Not necessary since it should've been saved in both directions, but
                                        // can be considered as a making sure check.
                                        if (!triangle.get() && localAdjacencyList.containsKey(neighbour2)) {
                                            if (localAdjacencyList.get(neighbour2).containsKey(neighbour1)) {
                                                triangle.set(true);
                                                System.out.println("Something went wrong saving the edges in state." +
                                                        " They haven't been saved in both directions. ");
                                            }
                                        }
                                        if (triangle.get()) {
                                            triangleCounter.getAndIncrement();
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            System.out.println("In partition "+input.f0+" we found "+triangleCounter.get()+" triangles.");
            collector.collect(triangleCounter.get());
        }
    }

    // Sorted EL decoupled
    public static class SortedELDecoupled extends KeyedProcessFunction<Integer, TemporalEdge, Tuple3<Integer, Integer[], Long>> {

        private transient ValueState<Integer> edgeCountSinceTimestamp;
        private transient ValueState<Long> lastTimestamp;
        private transient MapState<Long, HashMap<GradoopId, List<Tuple2<GradoopId, TemporalEdge>>>> sortedEdgeList;
        private transient ValueState<Long> nextOutputTimestamp;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<Long, HashMap<GradoopId, List<Tuple2<GradoopId, TemporalEdge>>>> descriptor =
                    new MapStateDescriptor<>(
                            "sortedEdgeList",
                            TypeInformation.of(new TypeHint<>() {
                            }),
                            TypeInformation.of(new TypeHint<>() {
                            })
                    );
            descriptor.setQueryable("sortedEdgeList");
            sortedEdgeList = getRuntimeContext().getMapState(descriptor);
            ValueStateDescriptor<Integer> descriptor2 = new ValueStateDescriptor<Integer>(
                    "edgeCountSinceTimestamp", Integer.class);
            edgeCountSinceTimestamp = getRuntimeContext().getState(descriptor2);
            ValueStateDescriptor<Long> descriptor3 = new ValueStateDescriptor<Long>(
                    "lastTimestamp", Long.class);
            lastTimestamp = getRuntimeContext().getState(descriptor3);
            ValueStateDescriptor<Long> descriptor4 = new ValueStateDescriptor<Long>(
                    "nextOutputTimestamp", Long.class);
            nextOutputTimestamp = getRuntimeContext().getState(descriptor4);
        }

        @Override
        public void processElement(TemporalEdge edge, Context context, Collector<Tuple3<Integer, Integer[], Long>> collector) throws Exception {

            if(edgeCountSinceTimestamp.value() == null) {
                edgeCountSinceTimestamp.update(0);
            }

            if(lastTimestamp.value() == null) {
                lastTimestamp.update(firstTimestamp);
                if(!lazyPurging) {
                    context.timerService().registerProcessingTimeTimer(lastTimestamp.value() + windowSize);
                }
            }

            if(nextOutputTimestamp.value() == null) {
                nextOutputTimestamp.update(firstTimestamp + slide);
                context.timerService().registerProcessingTimeTimer(nextOutputTimestamp.value());
            }

            if(edgeCountSinceTimestamp.value() == batchSize) {
                edgeCountSinceTimestamp.update(0);
                lastTimestamp.update(context.timerService().currentProcessingTime());
                if(!lazyPurging) {
                    context.timerService().registerProcessingTimeTimer(lastTimestamp.value() + windowSize);
                }
            }

            long currentTime = lastTimestamp.value();
            long validTo = currentTime + windowSize;

            if(edgeCountSinceTimestamp.value() == 0) {
                sortedEdgeList.put(validTo, new HashMap<>());
            }

            GradoopId source = edge.getSourceId();
            GradoopId target = edge.getTargetId();
            edge.setValidTo(validTo);

            try {
                sortedEdgeList.get(validTo).get(source).add(Tuple2.of(target, edge));
            } catch (NullPointerException e) {
                List<Tuple2<GradoopId, TemporalEdge>> toPut =
                        Collections.synchronizedList(new LinkedList<Tuple2<GradoopId, TemporalEdge>>());
                toPut.add(Tuple2.of(target, edge));
                sortedEdgeList.get(validTo).put(source, toPut);
            }
            edgeCountSinceTimestamp.update(edgeCountSinceTimestamp.value()+1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<Integer, Integer[], Long>> out) throws Exception {
            if(lazyPurging) {

            } else {
                sortedEdgeList.remove(timestamp);
            }
            if (timestamp == nextOutputTimestamp.value()) {
                System.out.println("We are now triggering output");
                nextOutputTimestamp.update(timestamp + slide);
                ctx.timerService().registerProcessingTimeTimer(timestamp + slide);
                out.collect(Tuple3.of(ctx.getCurrentKey(), keys, timestamp + windowSize));
            }
        }
    }

    // Sorted EL with Algorithm
    public static class SortedELwithAlg extends KeyedProcessFunction<Integer, TemporalEdge, String> {

        private transient ValueState<Integer> edgeCountSinceTimestamp;
        private transient ValueState<Long> lastTimestamp;
        private transient MapState<Long, HashMap<GradoopId, List<Tuple2<GradoopId, TemporalEdge>>>> sortedEdgeList;
        private transient ValueState<Long> nextOutputTimestamp;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<Long, HashMap<GradoopId, List<Tuple2<GradoopId, TemporalEdge>>>> descriptor =
                    new MapStateDescriptor<>(
                            "sortedEdgeList",
                            TypeInformation.of(new TypeHint<>() {
                            }),
                            TypeInformation.of(new TypeHint<>() {
                            })
                    );
            descriptor.setQueryable("sortedEdgeList");
            sortedEdgeList = getRuntimeContext().getMapState(descriptor);
            ValueStateDescriptor<Integer> descriptor2 = new ValueStateDescriptor<Integer>(
                    "edgeCountSinceTimestamp", Integer.class);
            edgeCountSinceTimestamp = getRuntimeContext().getState(descriptor2);
            ValueStateDescriptor<Long> descriptor3 = new ValueStateDescriptor<Long>(
                    "lastTimestamp", Long.class);
            lastTimestamp = getRuntimeContext().getState(descriptor3);
            ValueStateDescriptor<Long> descriptor4 = new ValueStateDescriptor<Long>(
                    "nextOutputTimestamp", Long.class);
            nextOutputTimestamp = getRuntimeContext().getState(descriptor4);
        }

        @Override
        public void processElement(TemporalEdge edge, Context context, Collector<String> collector) throws Exception {
            if (edgeCountSinceTimestamp.value() == null) {
                edgeCountSinceTimestamp.update(0);
            }
            if (lastTimestamp.value() == null) {
                lastTimestamp.update(firstTimestamp);
                if(!lazyPurging) {
                    context.timerService().registerProcessingTimeTimer(lastTimestamp.value() + windowSize);
                }
            }
            if (nextOutputTimestamp.value() == null) {
                nextOutputTimestamp.update(firstTimestamp + slide);
                context.timerService().registerProcessingTimeTimer(nextOutputTimestamp.value());
            }
            if (edgeCountSinceTimestamp.value() == batchSize) {
                edgeCountSinceTimestamp.update(0);
                lastTimestamp.update(context.timerService().currentProcessingTime());
                if(!lazyPurging) {
                    context.timerService().registerProcessingTimeTimer(lastTimestamp.value() + windowSize);
                }
            }

            long currentTime = lastTimestamp.value();
            long validTo = currentTime + windowSize;

            if (edgeCountSinceTimestamp.value() == 0) {
                sortedEdgeList.put(validTo, new HashMap<>());
            }

            GradoopId source = edge.getSourceId();
            GradoopId target = edge.getTargetId();
            edge.setValidTo(validTo);

            try {
                sortedEdgeList.get(validTo).get(source).add(Tuple2.of(target, edge));
            } catch (NullPointerException e) {
                List<Tuple2<GradoopId, TemporalEdge>> toPut =
                        Collections.synchronizedList(new LinkedList<Tuple2<GradoopId, TemporalEdge>>());
                toPut.add(Tuple2.of(target, edge));
                sortedEdgeList.get(validTo).put(source, toPut);
            }
            edgeCountSinceTimestamp.update(edgeCountSinceTimestamp.value() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            if(lazyPurging) {

            } else {
                sortedEdgeList.remove(timestamp);
            }
            if (timestamp == nextOutputTimestamp.value()) {
                System.out.println("We are now triggering output");
                nextOutputTimestamp.update(timestamp + slide);
                ctx.timerService().registerProcessingTimeTimer(timestamp + slide);
                out.collect("Result at " + timestamp + " : " + algorithm.doAlgorithm(sortedEdgeList, QS,
                        ctx.getCurrentKey(), keys, timestamp,timestamp+windowSize));
            }
        }
    }

    // Edge list decoupled.
    public static class ELDecoupled extends KeyedProcessFunction<Integer, TemporalEdge, Tuple3<Integer, Integer[], Long>> {

        private transient ValueState<Integer> edgeCountSinceTimestamp;
        private transient ValueState<Long> lastTimestamp;
        private transient MapState<Long, List<Tuple3<GradoopId, GradoopId, TemporalEdge>>> edgeList;
        private transient ValueState<Long> nextOutputTimestamp;


        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<Long, List<Tuple3<GradoopId, GradoopId, TemporalEdge>>> descriptor = new MapStateDescriptor<>(
                    "edgeList",
                    TypeInformation.of(new TypeHint<Long>() {
                    }),
                    TypeInformation.of(new TypeHint<List<Tuple3<GradoopId, GradoopId, TemporalEdge>>>() {
                    })
            );
            descriptor.setQueryable("edgeList");
            edgeList = getRuntimeContext().getMapState(descriptor);
            ValueStateDescriptor<Integer> descriptor2 = new ValueStateDescriptor<Integer>(
                    "edgeCountSinceTimestamp", Integer.class);
            edgeCountSinceTimestamp = getRuntimeContext().getState(descriptor2);
            ValueStateDescriptor<Long> descriptor3 = new ValueStateDescriptor<Long>(
                    "lastTimestamp", Long.class);
            lastTimestamp = getRuntimeContext().getState(descriptor3);
            ValueStateDescriptor<Long> descriptor4 = new ValueStateDescriptor<Long>(
                    "nextOutputTimestamp", Long.class);
            nextOutputTimestamp = getRuntimeContext().getState(descriptor4);
        }

        @Override
        public void processElement(TemporalEdge edge, Context context, Collector<Tuple3<Integer, Integer[], Long>> collector) throws Exception {
            if(edgeCountSinceTimestamp.value() == null) {
                edgeCountSinceTimestamp.update(0);
            }

            if (nextOutputTimestamp.value() == null) {
                nextOutputTimestamp.update(firstTimestamp + slide);
                context.timerService().registerProcessingTimeTimer(nextOutputTimestamp.value());
            }

            if(lastTimestamp.value() == null) {
                lastTimestamp.update(firstTimestamp);
                if(!lazyPurging) {
                    context.timerService().registerProcessingTimeTimer(lastTimestamp.value() + windowSize);
                }
            }

            if(edgeCountSinceTimestamp.value() == batchSize) {
                edgeCountSinceTimestamp.update(0);
                lastTimestamp.update(context.timerService().currentProcessingTime());
                if(!lazyPurging) {
                    context.timerService().registerProcessingTimeTimer(lastTimestamp.value() + windowSize);
                }
            }

            long currentTime = lastTimestamp.value();
            long validTo = currentTime + windowSize;

            if(edgeCountSinceTimestamp.value() == 0) {
                List<Tuple3<GradoopId, GradoopId, TemporalEdge>> list =
                        Collections.synchronizedList(new LinkedList<>());
                edgeList.put(validTo, list);
            }

            GradoopId source = edge.getSourceId();
            GradoopId target = edge.getTargetId();
            edge.setValidTo(validTo);

            edgeList.get(validTo).add(Tuple3.of(source, target, edge));

            edgeCountSinceTimestamp.update(edgeCountSinceTimestamp.value()+1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<Integer, Integer[], Long>> out) throws Exception {
            if(lazyPurging) {

            } else {
                edgeList.remove(timestamp);
            }
            if (timestamp == nextOutputTimestamp.value()) {
                System.out.println("We are now triggering output");
                nextOutputTimestamp.update(timestamp + slide);
                ctx.timerService().registerProcessingTimeTimer(timestamp + slide);
                out.collect(Tuple3.of(ctx.getCurrentKey(), GraphState.keys, timestamp+windowSize));
            }
        }
    }

    // Edge list with Algorithm.
    public static class ELwithAlg extends KeyedProcessFunction<Integer, TemporalEdge, String> {

        private transient ValueState<Integer> edgeCountSinceTimestamp;
        private transient ValueState<Long> lastTimestamp;
        private transient MapState<Long, List<Tuple3<GradoopId, GradoopId, TemporalEdge>>> edgeList;
        private transient ValueState<Long> nextOutputTimestamp;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<Long, List<Tuple3<GradoopId, GradoopId, TemporalEdge>>> descriptor = new MapStateDescriptor<>(
                    "edgeList",
                    TypeInformation.of(new TypeHint<Long>() {
                    }),
                    TypeInformation.of(new TypeHint<List<Tuple3<GradoopId, GradoopId, TemporalEdge>>>() {
                    })
            );
            descriptor.setQueryable("edgeList");
            edgeList = getRuntimeContext().getMapState(descriptor);
            ValueStateDescriptor<Integer> descriptor2 = new ValueStateDescriptor<Integer>(
                    "edgeCountSinceTimestamp", Integer.class);
            edgeCountSinceTimestamp = getRuntimeContext().getState(descriptor2);
            ValueStateDescriptor<Long> descriptor3 = new ValueStateDescriptor<Long>(
                    "lastTimestamp", Long.class);
            lastTimestamp = getRuntimeContext().getState(descriptor3);
            ValueStateDescriptor<Long> descriptor4 = new ValueStateDescriptor<Long>(
                    "nextOutputTimestamp", Long.class);
            nextOutputTimestamp = getRuntimeContext().getState(descriptor4);
        }

        @Override
        public void processElement(TemporalEdge edge, Context context, Collector<String> collector) throws Exception {
            if(edgeCountSinceTimestamp.value() == null) {
                edgeCountSinceTimestamp.update(0);
            }
            if(lastTimestamp.value() == null) {
                lastTimestamp.update(firstTimestamp);
                if(!lazyPurging) {
                    context.timerService().registerProcessingTimeTimer(lastTimestamp.value() + windowSize);
                }
            }
            if(nextOutputTimestamp.value() == null) {
                nextOutputTimestamp.update( firstTimestamp + slide);
                context.timerService().registerProcessingTimeTimer(nextOutputTimestamp.value());
            }

            if(edgeCountSinceTimestamp.value() == batchSize) {
                edgeCountSinceTimestamp.update(0);
                lastTimestamp.update(context.timerService().currentProcessingTime());
                if(!lazyPurging) {
                    context.timerService().registerProcessingTimeTimer(lastTimestamp.value() + windowSize);
                }
            }

            long currentTime = lastTimestamp.value();
            long validTo = currentTime + windowSize;

            if(edgeCountSinceTimestamp.value() == 0) {
                List<Tuple3<GradoopId, GradoopId, TemporalEdge>> list =
                        Collections.synchronizedList(new LinkedList<>());
                edgeList.put(validTo, list);
            }

            GradoopId source = edge.getSourceId();
            GradoopId target = edge.getTargetId();
            edge.setValidTo(validTo);

            edgeList.get(validTo).add(Tuple3.of(source, target, edge));

            edgeCountSinceTimestamp.update(edgeCountSinceTimestamp.value()+1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            if(lazyPurging) {

            } else {
                edgeList.remove(timestamp);
            }
            if (timestamp == nextOutputTimestamp.value()) {
                System.out.println("We are now triggering output");
                nextOutputTimestamp.update(timestamp + slide);
                ctx.timerService().registerProcessingTimeTimer(timestamp + slide);
                out.collect("Result at time " + timestamp + " : " +
                        algorithm.doAlgorithm(edgeList, QS, ctx.getCurrentKey(), keys,
                                timestamp, timestamp+windowSize));
            }
        }
    }

    // Adjacency List
    public static class ALdecoupled extends KeyedProcessFunction<Integer, TemporalEdge, Tuple3<Integer, Integer[], Long>> {

        private transient ValueState<Integer> edgeCountSinceTimestamp;
        private transient ValueState<Long> lastTimestamp;
        private transient MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>> adjacencyList;
        private transient ValueState<Long> nextOutputTimestamp;


        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>> descriptor = new MapStateDescriptor<>(
                    "adjacencyList",
                    TypeInformation.of(new TypeHint<Long>() {
                    }),
                    TypeInformation.of(new TypeHint<HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>>() {
                    })
            );
            descriptor.setQueryable("adjacencyList");
            adjacencyList = getRuntimeContext().getMapState(descriptor);
            ValueStateDescriptor<Integer> descriptor2 = new ValueStateDescriptor<Integer>(
                    "edgeCountSinceTimestamp", Integer.class);
            edgeCountSinceTimestamp = getRuntimeContext().getState(descriptor2);
            ValueStateDescriptor<Long> descriptor3 = new ValueStateDescriptor<Long>(
                    "lastTimestamp", Long.class);
            lastTimestamp = getRuntimeContext().getState(descriptor3);
            ValueStateDescriptor<Long> descriptor4 = new ValueStateDescriptor<Long>(
                    "nextOutputTimestamp", Long.class);
            nextOutputTimestamp = getRuntimeContext().getState(descriptor4);
        }

        @Override
        public void processElement(TemporalEdge edge, Context context, Collector<Tuple3<Integer, Integer[], Long>> collector) throws Exception {
            if(edgeCountSinceTimestamp.value() == null) {
                edgeCountSinceTimestamp.update(0);
            }

            if(nextOutputTimestamp.value() == null) {
                nextOutputTimestamp.update(firstTimestamp + slide);
                context.timerService().registerProcessingTimeTimer(nextOutputTimestamp.value());
            }

            if(lastTimestamp.value() == null) {
                lastTimestamp.update(firstTimestamp);
                if(!lazyPurging) {
                    context.timerService().registerProcessingTimeTimer(lastTimestamp.value() + windowSize);
                }
            }

            if(edgeCountSinceTimestamp.value() == batchSize) {
                edgeCountSinceTimestamp.update(0);
                lastTimestamp.update(context.timerService().currentProcessingTime());
                if(!lazyPurging) {
                    context.timerService().registerProcessingTimeTimer(lastTimestamp.value()+windowSize);
                }
            }

            long currentTime = lastTimestamp.value();
            long validTo = currentTime + windowSize;

            if(edgeCountSinceTimestamp.value() == 0) {
                adjacencyList.put(validTo, new HashMap<>());
            }

            GradoopId source = edge.getSourceId();
            GradoopId target = edge.getTargetId();
            edge.setValidTo(validTo);

            try {
                adjacencyList.get(validTo).get(source).put(target, edge);
            } catch (NullPointerException e) {
                HashMap<GradoopId, TemporalEdge> toPut = new HashMap<>();
                toPut.put(target, edge);
                adjacencyList.get(validTo).put(source, toPut);
            }
            edgeCountSinceTimestamp.update(edgeCountSinceTimestamp.value()+1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<Integer, Integer[], Long>> out) throws Exception {
            if(lazyPurging) {

            } else {
                adjacencyList.remove(timestamp);
            }
            if(timestamp == nextOutputTimestamp.value()) {
                System.out.println("We are now triggering output");
                nextOutputTimestamp.update(timestamp + slide);
                ctx.timerService().registerProcessingTimeTimer(timestamp + slide);
                out.collect(Tuple3.of(ctx.getCurrentKey(), GraphState.keys, timestamp+windowSize));
            }
        }
    }

    // Adjacency List with Algorithm onTimer
    public static class ALwithAlg extends KeyedProcessFunction<Integer, TemporalEdge, String> {

        private transient ValueState<Integer> edgeCountSinceTimestamp;
        private transient ValueState<Long> lastTimestamp;
        private transient MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>> adjacencyList;
        private transient ValueState<Long> nextOutputTimestamp;
        private static LinkedList<Long> timestamps = new LinkedList<>();

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>> descriptor = new MapStateDescriptor<>(
                    "adjacencyList",
                    TypeInformation.of(new TypeHint<Long>() {
                    }),
                    TypeInformation.of(new TypeHint<HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>>() {
                    })
            );
            descriptor.setQueryable("adjacencyList");
            adjacencyList = getRuntimeContext().getMapState(descriptor);
            ValueStateDescriptor<Integer> descriptor2 = new ValueStateDescriptor<Integer>(
                    "edgeCountSinceTimestamp", Integer.class);
            edgeCountSinceTimestamp = getRuntimeContext().getState(descriptor2);
            ValueStateDescriptor<Long> descriptor3 = new ValueStateDescriptor<Long>(
                    "lastTimestamp", Long.class);
            lastTimestamp = getRuntimeContext().getState(descriptor3);
            ValueStateDescriptor<Long> descriptor4 = new ValueStateDescriptor<Long>(
                    "nextOutputTimestamp", Long.class);
            nextOutputTimestamp = getRuntimeContext().getState(descriptor4);
        }

        @Override
        public void processElement(TemporalEdge edge, Context context, Collector<String> collector) throws Exception {
            if(edgeCountSinceTimestamp.value() == null) {
                edgeCountSinceTimestamp.update(0);
            }

            if(nextOutputTimestamp.value() == null) {
                nextOutputTimestamp.update(firstTimestamp + slide);
                context.timerService().registerProcessingTimeTimer(nextOutputTimestamp.value());
            }

            if(lastTimestamp.value() == null) {
                lastTimestamp.update(firstTimestamp);
                if(!lazyPurging) {
                    context.timerService().registerProcessingTimeTimer(lastTimestamp.value() + windowSize);
                } else {
                    timestamps.add(lastTimestamp.value());
                }
            }

            if(edgeCountSinceTimestamp.value() == batchSize) {
                edgeCountSinceTimestamp.update(0);
                lastTimestamp.update(context.timerService().currentProcessingTime());
                if(!lazyPurging) {
                    context.timerService().registerProcessingTimeTimer(lastTimestamp.value()+windowSize);
                } else {
                    timestamps.add(lastTimestamp.value());
                }
            }

            long currentTime = lastTimestamp.value();
            long validTo = currentTime + windowSize;

            if(edgeCountSinceTimestamp.value() == 0) {
                adjacencyList.put(validTo, new HashMap<>());
            }

            GradoopId source = edge.getSourceId();
            GradoopId target = edge.getTargetId();
            edge.setValidTo(validTo);

            try {
                adjacencyList.get(validTo).get(source).put(target, edge);
            } catch (NullPointerException e) {
                HashMap<GradoopId, TemporalEdge> toPut = new HashMap<>();
                toPut.put(target, edge);
                adjacencyList.get(validTo).put(source, toPut);
            }
            edgeCountSinceTimestamp.update(edgeCountSinceTimestamp.value()+1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            if(lazyPurging) {
                boolean keepRemoving = true;
                while(keepRemoving) {
                    long timestamp1;
                    try {
                        timestamp1 = timestamps.peek();
                    } catch (NullPointerException e) {
                        break;
                    }
                    if((timestamp1 + windowSize) <= timestamp) {
                        adjacencyList.remove(timestamp1);
                        timestamps.poll();
                    } else {
                        keepRemoving = false;
                    }
                }
            }else {
                adjacencyList.remove(timestamp);
            }

            if(timestamp == nextOutputTimestamp.value()) {
                System.out.println("We are now triggering output");
                nextOutputTimestamp.update(timestamp + slide);
                ctx.timerService().registerProcessingTimeTimer(timestamp + slide);
                out.collect("Result at time '"+ timestamp + " : "+
                        algorithm.doAlgorithm(adjacencyList, QS, ctx.getCurrentKey(), keys,
                                timestamp, timestamp+windowSize));
                System.out.println("This took "+(ctx.timerService().currentProcessingTime()-timestamp)+" ms");
            }
        }
    }

    public static class CountTriangles extends KeyedProcessFunction<Integer, TemporalEdge, String> {

        private transient MapState<GradoopId, HashMap<GradoopId, TemporalEdge>> sortedEdgeList;
        private final Long window;
        private final Long slide;
        private transient ValueState<Long> lastOutput;
        private transient ValueState<Integer> triangleCount;
        private transient ValueState<Integer> edgeCountSinceTimestamp;
        private transient ValueState<Long> lastTimestamp;

        public CountTriangles(long window, long slide) {
            this.slide = slide;
            this.window = window;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<GradoopId, HashMap<GradoopId, TemporalEdge>> ELdescriptor = new MapStateDescriptor<>(
                    "sortedEdgeList",
                    TypeInformation.of(new TypeHint<GradoopId>() {
                    }),
                    TypeInformation.of(new TypeHint<HashMap<GradoopId, TemporalEdge>>() {
                    })
            );
            ELdescriptor.setQueryable("sortedEdgeList");
            sortedEdgeList = getRuntimeContext().getMapState(ELdescriptor);
            ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<Long>(
                    "lastOutputTime", Long.class);
            lastOutput = getRuntimeContext().getState(descriptor);
            ValueStateDescriptor<Integer> descriptor1 = new ValueStateDescriptor<Integer>(
                    "triangleCount", Integer.class);
            triangleCount = getRuntimeContext().getState(descriptor1);
            ValueStateDescriptor<Integer> descriptor2 = new ValueStateDescriptor<Integer>(
                    "edgeCountSinceTimestamp", Integer.class);
            edgeCountSinceTimestamp = getRuntimeContext().getState(descriptor2);
            ValueStateDescriptor<Long> descriptor3 = new ValueStateDescriptor<Long>(
                    "lastTimestamp", Long.class);
            lastTimestamp = getRuntimeContext().getState(descriptor3);
        }

        @Override
        public void processElement(TemporalEdge edge, Context context, Collector<String> collector) throws Exception {
            while(!QS.isInitilized()) {
                Thread.sleep(100);
            }
            if(triangleCount.value() == null) {
                triangleCount.update(0);
            }
            if(edgeCountSinceTimestamp.value() == null) {
                edgeCountSinceTimestamp.update(0);
            }
            if(lastTimestamp.value() == null) {
                lastTimestamp.update(context.timerService().currentProcessingTime());
            }
            long currentTime = lastTimestamp.value();
            if(lastOutput.value() == null) {
                lastOutput.update(currentTime);
                context.timerService().registerProcessingTimeTimer(lastOutput.value()+slide);
                context.timerService().registerProcessingTimeTimer(lastOutput.value()+slide+slide);
            }
            while(currentTime>(lastOutput.value()+slide)) {
                lastOutput.update(lastOutput.value()+slide);
                context.timerService().registerProcessingTimeTimer(lastOutput.value()+slide);
                context.timerService().registerProcessingTimeTimer(lastOutput.value()+slide+slide);
                currentTime = context.timerService().currentProcessingTime();
                lastTimestamp.update(currentTime);
                edgeCountSinceTimestamp.update(0);
            }
            edgeCountSinceTimestamp.update(edgeCountSinceTimestamp.value()+1);
            if(edgeCountSinceTimestamp.value() == 50) {
                edgeCountSinceTimestamp.update(0);
                currentTime = context.timerService().currentProcessingTime();
                lastTimestamp.update(currentTime);
            }

            edge.setValidTo(currentTime+window);
            GradoopId source = edge.getSourceId();
            GradoopId target = edge.getTargetId();
            try {
                sortedEdgeList.get(source).put(target, edge);
            } catch (NullPointerException e) {
                HashMap<GradoopId, TemporalEdge> toPut = new HashMap<>();
                toPut.put(target, edge);
                sortedEdgeList.put(source, toPut);
            }
            try {
                sortedEdgeList.get(target).put(source, edge);
            } catch (NullPointerException e) {
                HashMap<GradoopId, TemporalEdge> toPut = new HashMap<>();
                toPut.put(source, edge);
                sortedEdgeList.put(target, toPut);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            AtomicInteger triangleCounter = new AtomicInteger(0);
            for(GradoopId srcId : sortedEdgeList.keys()) {
                GradoopId[] neighbours = sortedEdgeList.get(srcId).keySet().toArray(GradoopId[]::new);
                for(int i = 0; i < neighbours.length; i++) {
                    GradoopId neighbour1 = neighbours[i];
                    if(neighbour1.compareTo(srcId) > 0 && sortedEdgeList.get(srcId).get(neighbour1).getValidTo() > timestamp) {
                        for(int j = 0; j < neighbours.length; j++) {
                            GradoopId neighbour2 = neighbours[j];
                            if(i != j && neighbour2.compareTo(neighbour1) > 0 && sortedEdgeList.get(srcId).get(neighbour2).getValidTo() > timestamp) {
                                try {
                                    TemporalEdge edge = sortedEdgeList.get(neighbour1).get(neighbour2);
                                    if(edge.getValidTo() > timestamp) {
                                        triangleCounter.getAndIncrement();
                                    }
                                } catch (NullPointerException e) {
                                    try {
                                        TemporalEdge edge = sortedEdgeList.get(neighbour2).get(neighbour1);
                                        if (edge.getValidTo() > timestamp) {
                                            triangleCounter.getAndIncrement();
                                        }
                                    } catch (NullPointerException ignored) {}
                                }
                            }
                        }
                    }
                }
            }
            out.collect("We found "+triangleCounter.get()+" triangles at timestamp: "+timestamp
            +". This timer took "+(ctx.timerService().currentProcessingTime()-timestamp)+" ms.");
        }
    }

    public static class CountingVertices extends KeyedProcessFunction<Integer, TemporalEdge, String> {

        private transient MapState<GradoopId, HashMap<GradoopId, TemporalEdge>> sortedEdgeList;
        private final Long window;
        private final Long slide;
        private transient ValueState<Long> lastOutput;
        private transient ValueState<Long> lastTimerPull;
        private transient ValueState<Integer> edgesSinceLastTimer;
        private transient MapState<GradoopId, Integer> vertexDegree;

        public CountingVertices(Long window, Long slide) {
            this.window = window;
            this.slide = slide;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<GradoopId, HashMap<GradoopId, TemporalEdge>> ELdescriptor = new MapStateDescriptor<>(
                    "sortedEdgeList",
                    TypeInformation.of(new TypeHint<GradoopId>() {
                    }),
                    TypeInformation.of(new TypeHint<HashMap<GradoopId, TemporalEdge>>() {
                    })
            );
            ELdescriptor.setQueryable("sortedEdgeList");
            sortedEdgeList = getRuntimeContext().getMapState(ELdescriptor);
            ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<Long>(
                    "lastOutputTime", Long.class);
            lastOutput = getRuntimeContext().getState(descriptor);
            ValueStateDescriptor<Long> descriptor1 = new ValueStateDescriptor<Long>(
                    "lastTimerPull", Long.class);
            lastTimerPull = getRuntimeContext().getState(descriptor1);
            ValueStateDescriptor<Integer> descriptor2 = new ValueStateDescriptor<Integer>(
                    "edgesSinceTimer", Integer.class);
            edgesSinceLastTimer = getRuntimeContext().getState(descriptor2);
            MapStateDescriptor<GradoopId, Integer> descriptor3 = new MapStateDescriptor<GradoopId, Integer>(
                    "vertexDegree", GradoopId.class, Integer.class);
            descriptor3.setQueryable("vertexDegree");
            vertexDegree = getRuntimeContext().getMapState(descriptor3);

        }

        @Override
        public void processElement(TemporalEdge edge, Context context, Collector<String> collector) throws Exception {
            if(lastOutput.value() == null) {
                while (!QS.isInitilized()) {
                    Thread.sleep(100);
                }
                long currentTime = context.timerService().currentProcessingTime();
                lastOutput.update(currentTime);
                lastTimerPull.update(currentTime);
                edgesSinceLastTimer.update(0);
                //lastOutput.update(context.timestamp());
                context.timerService().registerProcessingTimeTimer(lastOutput.value() + slide);
                context.timerService().registerProcessingTimeTimer(lastOutput.value() + slide + slide);

            }
            edgesSinceLastTimer.update(edgesSinceLastTimer.value()+1);
            if(edgesSinceLastTimer.value() > 50) {
                lastTimerPull.update(context.timerService().currentProcessingTime());
                edgesSinceLastTimer.update(0);
            }
            long currentTime = lastTimerPull.value();

            while(currentTime>(lastOutput.value()+slide)) {
                lastOutput.update(lastOutput.value()+slide);
                context.timerService().registerProcessingTimeTimer(lastOutput.value()+slide);
                context.timerService().registerProcessingTimeTimer(lastOutput.value()+slide+slide);
            }

            edge.setValidTo(currentTime+window);
            GradoopId source = edge.getSourceId();
            GradoopId target = edge.getTargetId();
            int oldDegree = 0;
            try {
                sortedEdgeList.get(source).put(target, edge);
                oldDegree = vertexDegree.get(source);
            } catch (NullPointerException e) {
                HashMap<GradoopId, TemporalEdge> toPut = new HashMap<>();
                toPut.put(target, edge);
                sortedEdgeList.put(source, toPut);
            }
            vertexDegree.put(source, oldDegree+1);
            oldDegree = 0;
            try {
                sortedEdgeList.get(target).put(source, edge);
                oldDegree = vertexDegree.get(target);
            } catch (NullPointerException e) {
                HashMap<GradoopId, TemporalEdge> toPut = new HashMap<>();
                toPut.put(source, edge);
                sortedEdgeList.put(target, toPut);
            }
            vertexDegree.put(target, oldDegree+1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            List<GradoopId> srcVertices =
                    StreamSupport.stream(vertexDegree.keys().spliterator(), false)
                            .collect(Collectors.toList());
            int currentKey = ctx.getCurrentKey();
            out.collect("Local partition "+currentKey+" had "+ srcVertices.size()+" srcVertices at time: "+timestamp);
            HashSet<GradoopId> distinct = new HashSet<>();
            distinct.addAll(srcVertices);
            for(int key : keys) {
                if(key != currentKey) {
                    int tries = 0;
                    int maxtries = 10;
                    while(tries < maxtries) {
                        try {
                            MapState<GradoopId, Integer> state = QS.getVertexDegree(key);
                            List<GradoopId> externalSrcVertices = StreamSupport.stream(state.keys().spliterator(), false)
                                    .collect(Collectors.toList());
                            out.collect("External partition: "+key+" had: "+externalSrcVertices.size()+" srcVertices at time: "
                                    +timestamp);
                            distinct.addAll(externalSrcVertices);
                            tries = maxtries;
                        } catch (Exception e) {
                            tries++;
                            System.out.println(tries);
                            //Thread.sleep(100);
                        }
                    }
                }
            }
            out.collect("Together the partitions have "+distinct.size()+" distinct vertices.");
        }
    }
    public KeyedStream<TemporalEdge, Integer> getData() {
        return this.input;
    }
}
