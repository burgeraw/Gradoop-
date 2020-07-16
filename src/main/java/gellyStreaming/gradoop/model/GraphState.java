package gellyStreaming.gradoop.model;

import gellyStreaming.gradoop.Experiments;
import gellyStreaming.gradoop.algorithms.Algorithm;
import gellyStreaming.gradoop.util.KeyGen;
import gellyStreaming.gradoop.util.globalCounter;
import gellyStreaming.gradoop.util.makeSimpleTemporalEdgeStream;
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
import org.apache.flink.api.java.tuple.Tuple4;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


public class GraphState implements Serializable {

    //private final transient KeyedStream<TemporalEdge, Integer> input;
    private final Integer[] keys;
    private QueryState QS;
    private final boolean lazyPurging;
    private final int batchSize;
    private final Long windowSize;
    private final Long slide;
    private transient SingleOutputStreamOperator<Tuple4<Integer, Integer[], Long, Long>> decoupledOutput = null;
    private transient SingleOutputStreamOperator<String> algorithmOutput = null;
    private final Algorithm algorithm;
    private final long firstTimestamp;
    public static JobID jobID;
    private final globalCounter myCounter;


    public GraphState(QueryState QS,
                      KeyedStream<TemporalEdge, Integer> input,
                      String strategy,
                      Long windowSize,
                      Long slide,
                      Integer numPartitions,
                      Boolean lazyPurging,
                      int batchSize,
                      Algorithm algorithm) {
        this.QS = QS;
        //this.input = input;
        this.windowSize = windowSize;
        this.slide = slide;
        KeyGen keyGenerator = new KeyGen(numPartitions,
                KeyGroupRangeAssignment.computeDefaultMaxParallelism(numPartitions));
        this.keys = new Integer[numPartitions];
        for (int i = 0; i < numPartitions; i++)
            this.keys[i] = keyGenerator.next(i);
        this.lazyPurging = lazyPurging;
        this.batchSize = batchSize;
        this.algorithm = algorithm;
        this.firstTimestamp = System.currentTimeMillis()+1000L;
        this.myCounter = new globalCounter(Experiments.valueToReach, Experiments.algValueToReach);

        if (algorithm == null) {
            switch (strategy) {
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

    // To perform an algorithm on fully decoupled state. Start with a Tuple4 that the statebuilding part out
    // puts each slide, with {key, allKeys(to locate other remote partitions), from timestamp, to timestamp}
    public DataStream<String> doDecoupledAlg(Algorithm alg) {
        return decoupledOutput.keyBy(new KeySelector<Tuple4<Integer, Integer[], Long, Long>, Integer>() {
            @Override
            public Integer getKey(Tuple4<Integer, Integer[], Long, Long> integerLongLongTuple4) throws Exception {
                return integerLongLongTuple4.f0;
            }
        }).process(new KeyedProcessFunction<Integer, Tuple4<Integer, Integer[], Long, Long>, String>() {
            @Override
            public void processElement(Tuple4<Integer, Integer[], Long, Long> integerLongLongTuple4, Context context, Collector<String> collector) throws Exception {
                System.out.println(integerLongLongTuple4);
                long before = context.timerService().currentProcessingTime();
                collector.collect("In partition "+integerLongLongTuple4.f0+" we got results: "+
                        alg.doAlgorithm(null, QS, integerLongLongTuple4.f0, integerLongLongTuple4.f1,
                        integerLongLongTuple4.f2, integerLongLongTuple4.f3));
                long after = context.timerService().currentProcessingTime();
                collector.collect("This took "+(after-before)+" ms");
                myCounter.incrementAlgResults();
            }
        });
    }

    public QueryState getQS() {
        return QS;
    }


    public void overWriteQS(JobID jobID) {
        QS.initialize(jobID);
        this.jobID = jobID;
    }

    public SingleOutputStreamOperator<Tuple4<Integer, Integer[], Long, Long>> getDecoupledOutput() {
        if(this.decoupledOutput == null) {
            throw new Error("We have only algorithm output, no decoupled one. Set algorithm = null for " +
                    "decoupled output.");
        } else {
            return this.decoupledOutput;
        }
    }

    public DataStream<String> getAlgorithmOutput() {
        if(this.algorithmOutput == null) {
            throw new Error("We have only decoupled output, no algorithm one. Set algorithm != null for " +
                    "algorithm output.");
        } else {
            return this.algorithmOutput;
        }
    }


    // Sorted EL decoupled
    public class SortedELDecoupled extends KeyedProcessFunction<Integer, TemporalEdge, Tuple4<Integer, Integer[], Long, Long>> {

        private transient ValueState<Integer> edgeCountSinceTimestamp;
        private transient ValueState<Long> lastTimestamp;
        private transient MapState<Long, HashMap<GradoopId, List<Tuple2<GradoopId, TemporalEdge>>>> sortedEdgeList;
        private transient ValueState<Long> nextOutputTimestamp;
        private final LinkedList<Long> timestamps = new LinkedList<>();
        private final AtomicLong removalTimeCounter = new AtomicLong(0);

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("Thread \t"+Thread.currentThread().getId()+"\t opens at: \t"+System.currentTimeMillis());
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
        public void close() throws Exception {
            System.out.println("Thread \t"+Thread.currentThread().getId()+"\t closes at: \t"+System.currentTimeMillis());
        }

        @Override
        public void processElement(TemporalEdge edge, Context context, Collector<Tuple4<Integer, Integer[], Long, Long>> collector) throws Exception {

            if(edgeCountSinceTimestamp.value() == null) {
                edgeCountSinceTimestamp.update(0);
            }

            if(lastTimestamp.value() == null) {
                lastTimestamp.update(firstTimestamp);
                if(!lazyPurging && slide != null) {
                    context.timerService().registerProcessingTimeTimer(lastTimestamp.value() + windowSize);
                } else {
                    timestamps.add(lastTimestamp.value());
                }
            }

            if(nextOutputTimestamp.value() == null && slide != null) {
                nextOutputTimestamp.update(firstTimestamp + slide);
                context.timerService().registerProcessingTimeTimer(nextOutputTimestamp.value());
            } else if (nextOutputTimestamp.value()== null && slide == null) {
                nextOutputTimestamp.update(firstTimestamp + 10000L);
                context.timerService().registerProcessingTimeTimer(nextOutputTimestamp.value());
            }

            if(edgeCountSinceTimestamp.value() == batchSize) {
                edgeCountSinceTimestamp.update(0);
                long newtimestamp = context.timerService().currentProcessingTime();
                if(newtimestamp == lastTimestamp.value()) {
                    newtimestamp++;
                }
                lastTimestamp.update(newtimestamp);
                if(!lazyPurging && slide != null) {
                    context.timerService().registerProcessingTimeTimer(lastTimestamp.value() + windowSize);
                } else {
                    timestamps.add(lastTimestamp.value());
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
            myCounter.increment();
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple4<Integer, Integer[], Long, Long>> out) throws Exception {
            if(slide!= null) {
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
                            sortedEdgeList.remove(timestamp1);
                            timestamps.poll();
                        } else {
                            keepRemoving = false;
                        }
                    }
                } else {
                    sortedEdgeList.remove(timestamp);
                }
                removalTimeCounter.getAndAdd((System.currentTimeMillis()-timestamp));
            }

            if (timestamp == nextOutputTimestamp.value()) {
                System.out.println(ctx.getCurrentKey()+"\t :State removal "+
                        "took \t"+removalTimeCounter.get());
                removalTimeCounter.set(0);
                edgeCountSinceTimestamp.update(0);
                long newtimestamp = timestamp;
                if(newtimestamp == lastTimestamp.value()) {
                    newtimestamp++;
                }
                lastTimestamp.update(newtimestamp);
                if(!lazyPurging && slide != null) {
                    ctx.timerService().registerProcessingTimeTimer(lastTimestamp.value() + windowSize);
                } else {
                    timestamps.add(lastTimestamp.value());
                }
                if(slide != null) {
                    nextOutputTimestamp.update(timestamp + slide);
                    ctx.timerService().registerProcessingTimeTimer(timestamp + slide);
                    out.collect(Tuple4.of(ctx.getCurrentKey(), keys, timestamp, timestamp + windowSize));
                } else {
                    if(timestamps.peekLast() < (timestamp-10000L)) {
                        System.out.println("Thread \t"+Thread.currentThread().getId()+"\t its last batchtimestamp was \t"+timestamps.peekLast());
                        out.collect(Tuple4.of(ctx.getCurrentKey(), keys, timestamp, timestamp + windowSize));
                    } else {
                        nextOutputTimestamp.update(timestamp+10000L);
                        ctx.timerService().registerProcessingTimeTimer(nextOutputTimestamp.value());
                    }
                }
            }
        }
    }

    // Sorted EL with Algorithm
    public class SortedELwithAlg extends KeyedProcessFunction<Integer, TemporalEdge, String> {

        private transient ValueState<Integer> edgeCountSinceTimestamp;
        private transient ValueState<Long> lastTimestamp;
        private transient MapState<Long, HashMap<GradoopId, List<Tuple2<GradoopId, TemporalEdge>>>> sortedEdgeList;
        private transient ValueState<Long> nextOutputTimestamp;
        private final LinkedList<Long> timestamps = new LinkedList<>();
        private final AtomicLong removalTimeCounter = new AtomicLong(0);

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("Thread \t"+Thread.currentThread().getId()+"\t opens at: \t"+System.currentTimeMillis());
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
            if (lastTimestamp.value() == null) {
                lastTimestamp.update(firstTimestamp);
                edgeCountSinceTimestamp.update(0);
            }
            if(nextOutputTimestamp.value() == null && slide != null) {
                nextOutputTimestamp.update(firstTimestamp + slide);
                context.timerService().registerProcessingTimeTimer(nextOutputTimestamp.value());
            } else if (nextOutputTimestamp.value()== null && slide == null) {
                nextOutputTimestamp.update(firstTimestamp + 10000L);
                context.timerService().registerProcessingTimeTimer(nextOutputTimestamp.value());
            }
            if (edgeCountSinceTimestamp.value() == batchSize) {
                edgeCountSinceTimestamp.update(0);
                long newtimestamp = context.timerService().currentProcessingTime();
                if(newtimestamp == lastTimestamp.value()) {
                    newtimestamp++;
                }
                lastTimestamp.update(newtimestamp);
            }

            long currentTime = lastTimestamp.value();
            long validTo = currentTime + windowSize;

            if (edgeCountSinceTimestamp.value() == 0) {
                sortedEdgeList.put(validTo, new HashMap<>());
                if(!lazyPurging && slide != null) {
                    context.timerService().registerProcessingTimeTimer(lastTimestamp.value() + windowSize);
                } else {
                    timestamps.add(lastTimestamp.value());
                }
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
            myCounter.increment();
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            if(slide != null) {
                if (lazyPurging) {
                    boolean keepRemoving = true;
                    while (keepRemoving) {
                        long timestamp1;
                        try {
                            timestamp1 = timestamps.peek();
                        } catch (NullPointerException e) {
                            break;
                        }
                        if ((timestamp1 + windowSize) <= timestamp) {
                            sortedEdgeList.remove(timestamp1);
                            timestamps.poll();
                        } else {
                            keepRemoving = false;
                        }
                    }
                } else {
                    sortedEdgeList.remove(timestamp);
                }
                removalTimeCounter.getAndAdd((System.currentTimeMillis()-timestamp));
            }

            if (timestamp == nextOutputTimestamp.value()) {
                System.out.println(ctx.getCurrentKey()+"\t :State removal "+
                        "took \t"+removalTimeCounter.get());
                removalTimeCounter.set(0);
                edgeCountSinceTimestamp.update(0);
                long newtimestamp = timestamp;
                if(newtimestamp == lastTimestamp.value()) {
                    newtimestamp++;
                }
                lastTimestamp.update(newtimestamp);
                if(slide != null) {
                    nextOutputTimestamp.update(timestamp + slide);
                    ctx.timerService().registerProcessingTimeTimer(timestamp + slide);
                    long current = ctx.timerService().currentProcessingTime();
                    out.collect(ctx.getCurrentKey()+"\t :We started the onTimer \t"+(current-timestamp)+ "\t ms too late. If this is big, consider " +
                            "increasing slide, decreasing input rate or using a faster algorithm");
                    out.collect(ctx.getCurrentKey()+"\t :Result at " + timestamp + " : " + algorithm.doAlgorithm(sortedEdgeList, QS,
                            ctx.getCurrentKey(), keys, timestamp, timestamp + windowSize));
                    out.collect(ctx.getCurrentKey()+"\t :This took \t" + (ctx.timerService().currentProcessingTime() - current) );
                } else {
                    if(timestamps.peekLast() < (timestamp-10000L)) {
                        System.out.println("Thread \t"+Thread.currentThread().getId()+"\t its last batchtimestamp was \t"+timestamps.peekLast());
                        out.collect(ctx.getCurrentKey()+"\t :Result at " + timestamp + " : " + algorithm.doAlgorithm(sortedEdgeList, QS,
                                ctx.getCurrentKey(), keys, 0, Long.MAX_VALUE));
                        out.collect(ctx.getCurrentKey()+"\t :This took \t" + (ctx.timerService().currentProcessingTime() - timestamp));
                        myCounter.incrementAlgResults();
                    } else {
                        nextOutputTimestamp.update(timestamp+10000L);
                        ctx.timerService().registerProcessingTimeTimer(nextOutputTimestamp.value());
                    }
                }
            }
        }
    }

    // Edge list decoupled.
    public class ELDecoupled extends KeyedProcessFunction<Integer, TemporalEdge, Tuple4<Integer, Integer[], Long, Long>> {

        private transient ValueState<Integer> edgeCountSinceTimestamp;
        private transient ValueState<Long> lastTimestamp;
        private transient MapState<Long, List<Tuple3<GradoopId, GradoopId, TemporalEdge>>> edgeList;
        private transient ValueState<Long> nextOutputTimestamp;
        private final LinkedList<Long> timestamps = new LinkedList<>();
        private final AtomicLong removalTimeCounter = new AtomicLong(0);

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("Thread \t"+Thread.currentThread().getId()+"\t opened at: \t"+System.currentTimeMillis());
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
        public void close() throws Exception {
            System.out.println("Thread \t"+Thread.currentThread().getId()+"\t closes at: \t"+System.currentTimeMillis());
        }

        @Override
        public void processElement(TemporalEdge edge, Context context, Collector<Tuple4<Integer, Integer[], Long, Long>> collector) throws Exception {

            if(nextOutputTimestamp.value() == null && slide != null) {
                nextOutputTimestamp.update(firstTimestamp + slide);
                context.timerService().registerProcessingTimeTimer(nextOutputTimestamp.value());
            } else if (nextOutputTimestamp.value()== null && slide == null) {
                nextOutputTimestamp.update(firstTimestamp + 10000L);
                context.timerService().registerProcessingTimeTimer(nextOutputTimestamp.value());
            }

            if(lastTimestamp.value() == null) {
                lastTimestamp.update(firstTimestamp);
                edgeCountSinceTimestamp.update(0);
            }

            if(edgeCountSinceTimestamp.value() == batchSize) {
                edgeCountSinceTimestamp.update(0);
                long newtimestamp = context.timerService().currentProcessingTime();
                if(newtimestamp == lastTimestamp.value()) {
                    newtimestamp++;
                }
                lastTimestamp.update(newtimestamp);
            }

            long currentTime = lastTimestamp.value();
            long validTo = currentTime + windowSize;

            if(edgeCountSinceTimestamp.value() == 0) {
                List<Tuple3<GradoopId, GradoopId, TemporalEdge>> list =
                        Collections.synchronizedList(new LinkedList<>());
                edgeList.put(validTo, list);
                if(!lazyPurging && slide != null) {
                    context.timerService().registerProcessingTimeTimer(lastTimestamp.value() + windowSize);
                } else {
                    timestamps.add(lastTimestamp.value());
                }
            }

            GradoopId source = edge.getSourceId();
            GradoopId target = edge.getTargetId();
            edge.setValidTo(validTo);

            edgeList.get(validTo).add(Tuple3.of(source, target, edge));

            edgeCountSinceTimestamp.update(edgeCountSinceTimestamp.value()+1);
            myCounter.increment();
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple4<Integer, Integer[], Long, Long>> out) throws Exception {
            if(slide!= null) {
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
                            edgeList.remove(timestamp1);
                            timestamps.poll();
                        } else {
                            keepRemoving = false;
                        }
                    }
                }else {
                    edgeList.remove(timestamp);
                }
                removalTimeCounter.getAndAdd((System.currentTimeMillis()-timestamp));
            }

            if (timestamp == nextOutputTimestamp.value()) {
                System.out.println(ctx.getCurrentKey()+"\t :State removal "+
                        "took \t"+removalTimeCounter.get());
                removalTimeCounter.set(0);
                edgeCountSinceTimestamp.update(0);
                long newtimestamp = timestamp;
                if(newtimestamp == lastTimestamp.value()) {
                    newtimestamp++;
                }
                lastTimestamp.update(newtimestamp);
                if(slide != null) {
                    nextOutputTimestamp.update(timestamp + slide);
                    ctx.timerService().registerProcessingTimeTimer(timestamp + slide);
                    out.collect(Tuple4.of(ctx.getCurrentKey(), keys, timestamp, timestamp + windowSize));
                } else {
                    if(timestamps.peekLast() < (timestamp-10000L)) {
                        System.out.println("Thread \t"+Thread.currentThread().getId()+"\t its last batchtimestamp was \t"+timestamps.peekLast());
                        out.collect(Tuple4.of(ctx.getCurrentKey(), keys, 0L, Long.MAX_VALUE));
                    } else {
                        nextOutputTimestamp.update(timestamp+10000L);
                        ctx.timerService().registerProcessingTimeTimer(nextOutputTimestamp.value());
                    }
                }
            }
        }
    }

    // Edge list with Algorithm.
    public class ELwithAlg extends KeyedProcessFunction<Integer, TemporalEdge, String> {

        private transient ValueState<Integer> edgeCountSinceTimestamp;
        private transient ValueState<Long> lastTimestamp;
        private transient MapState<Long, List<Tuple3<GradoopId, GradoopId, TemporalEdge>>> edgeList;
        private transient ValueState<Long> nextOutputTimestamp;
        private final LinkedList<Long> timestamps = new LinkedList<>();
        private final AtomicLong removalTimeCounter = new AtomicLong(0);

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("Thread \t"+Thread.currentThread().getId()+"\t opens at: \t"+System.currentTimeMillis());
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
                if(!lazyPurging && slide != null) {
                    context.timerService().registerProcessingTimeTimer(lastTimestamp.value() + windowSize);
                } else {
                    timestamps.add(lastTimestamp.value());
                }
            }

            if(nextOutputTimestamp.value() == null && slide != null) {
                nextOutputTimestamp.update(firstTimestamp + slide);
                context.timerService().registerProcessingTimeTimer(nextOutputTimestamp.value());
            } else if (nextOutputTimestamp.value()== null && slide == null) {
                nextOutputTimestamp.update(firstTimestamp + 10000L);
                context.timerService().registerProcessingTimeTimer(nextOutputTimestamp.value());
            }

            if(edgeCountSinceTimestamp.value() == batchSize) {
                edgeCountSinceTimestamp.update(0);
                long newtimestamp = context.timerService().currentProcessingTime();
                if(newtimestamp == lastTimestamp.value()) {
                    newtimestamp++;
                }
                lastTimestamp.update(newtimestamp);
                if(!lazyPurging && slide != null) {
                    context.timerService().registerProcessingTimeTimer(lastTimestamp.value() + windowSize);
                } else {
                    timestamps.add(lastTimestamp.value());
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
            myCounter.increment();
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            if(slide!= null) {
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
                            edgeList.remove(timestamp1);
                            timestamps.poll();
                        } else {
                            keepRemoving = false;
                        }
                    }
                }else {
                    edgeList.remove(timestamp);
                }
                removalTimeCounter.getAndAdd((System.currentTimeMillis()-timestamp));
            }

            if (timestamp == nextOutputTimestamp.value()) {
                System.out.println(ctx.getCurrentKey()+"\t :State removal "+
                        "took \t"+removalTimeCounter.get());
                removalTimeCounter.set(0);
                edgeCountSinceTimestamp.update(0);
                long newtimestamp = timestamp;
                if(newtimestamp == lastTimestamp.value()) {
                    newtimestamp++;
                }
                lastTimestamp.update(newtimestamp);
                if(!lazyPurging && slide != null) {
                    ctx.timerService().registerProcessingTimeTimer(lastTimestamp.value() + windowSize);
                } else {
                    timestamps.add(lastTimestamp.value());
                }
                if(slide != null) {
                    nextOutputTimestamp.update(timestamp + slide);
                    long current = ctx.timerService().currentProcessingTime();
                    out.collect(ctx.getCurrentKey()+"\t :We started the onTimer \t"+(current-timestamp)+ "\t ms too late. If this is big, consider " +
                            "increasing slide, decreasing input rate or using a faster algorithm");
                    out.collect(ctx.getCurrentKey()+"\t :Result at time " + timestamp + " : " +
                            algorithm.doAlgorithm(edgeList, QS, ctx.getCurrentKey(), keys,
                                    timestamp, timestamp + windowSize));
                    out.collect(ctx.getCurrentKey()+"\t :This took \t" + (ctx.timerService().currentProcessingTime() - current));
                } else {
                    if(timestamps.peekLast() < (timestamp-10000L)) {
                        System.out.println("Thread \t"+Thread.currentThread().getId()+"\t its last batchtimestamp was \t"+timestamps.peekLast());
                        out.collect("Result at time " + timestamp + " : " +
                                algorithm.doAlgorithm(edgeList, QS, ctx.getCurrentKey(), keys,
                                        0, Long.MAX_VALUE));
                        out.collect(ctx.getCurrentKey()+"\t :This took \t" + (ctx.timerService().currentProcessingTime() - timestamp));
                        myCounter.incrementAlgResults();
                    } else {
                        nextOutputTimestamp.update(timestamp+10000L);
                        ctx.timerService().registerProcessingTimeTimer(nextOutputTimestamp.value());
                    }
                }
            }
        }
    }

    // Adjacency List
    public class ALdecoupled extends KeyedProcessFunction<Integer, TemporalEdge, Tuple4<Integer, Integer[], Long, Long>> {

        private transient ValueState<Integer> edgeCountSinceTimestamp;
        private transient ValueState<Long> lastTimestamp;
        private transient MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>> adjacencyList;
        private transient ValueState<Long> nextOutputTimestamp;
        private final LinkedList<Long> timestamps = new LinkedList<>();
        private final AtomicLong removalTimeCounter = new AtomicLong(0);

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("Thread \t"+Thread.currentThread().getId()+"\t opens at: \t"+System.currentTimeMillis());
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
        public void close() throws Exception {
            System.out.println("Thread \t"+Thread.currentThread().getId()+"\t closes at: \t"+System.currentTimeMillis());
        }

        @Override
        public void processElement(TemporalEdge edge, Context context, Collector<Tuple4<Integer, Integer[], Long, Long>> collector) throws Exception {

            if(nextOutputTimestamp.value() == null && slide != null) {
                nextOutputTimestamp.update(firstTimestamp + slide);
                context.timerService().registerProcessingTimeTimer(nextOutputTimestamp.value());
            } else if (nextOutputTimestamp.value()== null && slide == null) {
                nextOutputTimestamp.update(firstTimestamp + 10000L);
                context.timerService().registerProcessingTimeTimer(nextOutputTimestamp.value());
            }

            if(lastTimestamp.value() == null) {
                lastTimestamp.update(firstTimestamp);
                edgeCountSinceTimestamp.update(0);
            }

            if(edgeCountSinceTimestamp.value() == batchSize) {
                edgeCountSinceTimestamp.update(0);
                long newtimestamp = context.timerService().currentProcessingTime();
                if(newtimestamp == lastTimestamp.value()) {
                    newtimestamp++;
                }
                lastTimestamp.update(newtimestamp);
            }

            long currentTime = lastTimestamp.value();
            long validTo = currentTime + windowSize;

            if(edgeCountSinceTimestamp.value() == 0) {
                adjacencyList.put(validTo, new HashMap<>());
                if(!lazyPurging && slide != null) {
                    context.timerService().registerProcessingTimeTimer(lastTimestamp.value() + windowSize);
                } else {
                    timestamps.add(lastTimestamp.value());
                }
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
            myCounter.increment();
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple4<Integer, Integer[], Long, Long>> out) throws Exception {
            if(slide != null) {
                if (lazyPurging) {
                    boolean keepRemoving = true;
                    while (keepRemoving) {
                        long timestamp1;
                        try {
                            timestamp1 = timestamps.peek();
                        } catch (NullPointerException e) {
                            break;
                        }
                        if ((timestamp1 + windowSize) <= timestamp) {
                            adjacencyList.remove(timestamp1);
                            timestamps.poll();
                        } else {
                            keepRemoving = false;
                        }
                    }
                } else {
                    adjacencyList.remove(timestamp);
                }
                removalTimeCounter.getAndAdd((System.currentTimeMillis()-timestamp));
            }

            if(timestamp == nextOutputTimestamp.value()) {
                System.out.println(ctx.getCurrentKey()+"\t :State removal "+
                        "took \t"+removalTimeCounter.get());
                removalTimeCounter.set(0);
                edgeCountSinceTimestamp.update(0);
                long newtimestamp = timestamp;
                if(newtimestamp == lastTimestamp.value()) {
                    newtimestamp++;
                }
                lastTimestamp.update(newtimestamp);
                if(slide != null) {
                    nextOutputTimestamp.update(timestamp + slide);
                    ctx.timerService().registerProcessingTimeTimer(timestamp + slide);
                    out.collect(Tuple4.of(ctx.getCurrentKey(), keys, timestamp, timestamp + windowSize));
                } else {
                    if(timestamps.peekLast() < (timestamp-10000L)) {
                        out.collect(Tuple4.of(ctx.getCurrentKey(), keys, 0L, Long.MAX_VALUE));
                        System.out.println("Thread \t"+Thread.currentThread().getId()+"\t its last batchtimestamp was \t"+timestamps.peekLast());
                    } else {
                        nextOutputTimestamp.update(timestamp+10000L);
                        ctx.timerService().registerProcessingTimeTimer(nextOutputTimestamp.value());
                    }
                }
            }
        }
    }

    // Adjacency List with Algorithm onTimer
    public class ALwithAlg extends KeyedProcessFunction<Integer, TemporalEdge, String> {

        private transient ValueState<Integer> edgeCountSinceTimestamp;
        private transient ValueState<Long> lastTimestamp;
        private transient MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>> adjacencyList;
        private transient ValueState<Long> nextOutputTimestamp;
        private final LinkedList<Long> timestamps = new LinkedList<>();
        private final AtomicLong removalTimeCounter = new AtomicLong(0);


        @Override
        public void open(Configuration parameters) {
            System.out.println("Thread \t"+Thread.currentThread().getId()+"\t opens at: \t"+System.currentTimeMillis());
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

            if(nextOutputTimestamp.value() == null && slide != null) {
                nextOutputTimestamp.update(firstTimestamp + slide);
                context.timerService().registerProcessingTimeTimer(nextOutputTimestamp.value());
            } else if (nextOutputTimestamp.value()== null && slide == null) {
                nextOutputTimestamp.update(firstTimestamp + 10000L);
                context.timerService().registerProcessingTimeTimer(nextOutputTimestamp.value());
            }

            if(lastTimestamp.value() == null) {
                lastTimestamp.update(firstTimestamp);
            }

            if(edgeCountSinceTimestamp.value() == batchSize) {
                edgeCountSinceTimestamp.update(0);
                long newtimestamp = context.timerService().currentProcessingTime();
                if(newtimestamp == lastTimestamp.value()) {
                    newtimestamp++;
                }
                lastTimestamp.update(newtimestamp);
            }

            long currentTime = lastTimestamp.value();
            long validTo = currentTime + windowSize;

            if(edgeCountSinceTimestamp.value() == 0 || !adjacencyList.contains(validTo)) {
                adjacencyList.put(validTo, new HashMap<>());
                if(!lazyPurging && slide != null) {
                    context.timerService().registerProcessingTimeTimer(lastTimestamp.value() + windowSize);
                } else {
                    timestamps.add(lastTimestamp.value());
                }
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
            myCounter.increment();
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) {
            if(slide != null) {
                if (lazyPurging) {
                    boolean keepRemoving = true;
                    while (keepRemoving) {
                        long timestamp1;
                        try {
                            timestamp1 = timestamps.peek();
                        } catch (NullPointerException e) {
                            break;
                        }
                        if ((timestamp1 + windowSize) <= timestamp) {
                            try {
                                adjacencyList.remove(timestamp1);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            timestamps.poll();
                        } else {
                            keepRemoving = false;
                        }
                    }
                } else {
                    try {
                        adjacencyList.remove(timestamp);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                removalTimeCounter.getAndAdd((System.currentTimeMillis()-timestamp));
            }

            try {
                if(timestamp == nextOutputTimestamp.value()) {
                    System.out.println(ctx.getCurrentKey()+"\t :State removal "+
                            "took \t"+removalTimeCounter.get());
                    removalTimeCounter.set(0);
                    edgeCountSinceTimestamp.update(0);
                    long newtimestamp = timestamp;
                    if(newtimestamp == lastTimestamp.value()) {
                        newtimestamp++;
                    }
                    lastTimestamp.update(newtimestamp);


                    if(slide != null) {
                        nextOutputTimestamp.update(timestamp + slide);
                        ctx.timerService().registerProcessingTimeTimer(timestamp + slide);
                        long current = ctx.timerService().currentProcessingTime();
                        out.collect(ctx.getCurrentKey()+"\t :We started the onTimer \t"+(current-timestamp)+ " \t ms too late. If this is big, consider " +
                                "increasing slide, decreasing input rate or using a faster algorithm.");
                        try {
                            //QS.initialize(jobID);
                            out.collect(ctx.getCurrentKey()+"\t :AlgResult at time '" + timestamp + " : " +
                                    algorithm.doAlgorithm(adjacencyList, QS, ctx.getCurrentKey(), keys,
                                            timestamp, timestamp + windowSize));
                        } catch (Exception e) {
                            System.out.println(e);
                        }
                        out.collect(ctx.getCurrentKey()+"\t :Alg took \t" + (ctx.timerService().currentProcessingTime() - current) + "\t ms");

                    } else {
                        if(timestamps.peekLast() < (timestamp-10000L)) {
                            System.out.println("Thread \t"+Thread.currentThread().getId()+"\t its last batchtimestamp was \t"+timestamps.peekLast());
                            //QS.initialize(jobID);
                            out.collect(ctx.getCurrentKey()+"\t :AlgResult at time \t" + timestamp + " \t: " +
                                        algorithm.doAlgorithm(adjacencyList, QS, ctx.getCurrentKey(), keys,
                                                0, Long.MAX_VALUE));
                                    //algorithm.doAlgorithm(adjacencyList, jobID, ctx.getCurrentKey(), keys,
                                     //       0, Long.MAX_VALUE));
                            out.collect(ctx.getCurrentKey()+"\t :Alg took \t" + (ctx.timerService().currentProcessingTime() - timestamp));
                            myCounter.incrementAlgResults();
                        } else {
                            nextOutputTimestamp.update(timestamp+10000L);
                            ctx.timerService().registerProcessingTimeTimer(nextOutputTimestamp.value());
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    //public KeyedStream<TemporalEdge, Integer> getData() {
    //    return input;
   // }
}
