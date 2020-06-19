package gellyStreaming.gradoop.partitioner;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class FennelPartitioner{

    public void edgeListToVertices(DataStream<Edge<Long, String>> input) {
        input.flatMap(new FlatMapFunction<Edge<Long, String>, Tuple2<Long, Long>>() {
            @Override
            public void flatMap(Edge<Long, String> edge, Collector<Tuple2<Long, Long>> collector) throws Exception {
                collector.collect(Tuple2.of(edge.getSource(), edge.getTarget()));
                collector.collect(Tuple2.of(edge.getTarget(), edge.getSource()));
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Long, Long>>() {
            @Override
            public long extractAscendingTimestamp(Tuple2<Long, Long> longLongTuple2) {
                return 1L;
            }
        }).keyBy(new KeySelector<Tuple2<Long, Long>, Object>() {
            @Override
            public Object getKey(Tuple2<Long, Long> longLongTuple2) throws Exception {
                return longLongTuple2.f0;
            }
        })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregateFunction<Tuple2<Long, Long>, Tuple2<Long, List<Long>>,  Tuple2<Long, List<Long>>>() {
                    @Override
                    public Tuple2<Long, List<Long>> createAccumulator() {
                        return Tuple2.of(null, new LinkedList<Long>());
                    }

                    @Override
                    public Tuple2<Long, List<Long>> add(Tuple2<Long, Long> edge, Tuple2<Long, List<Long>> agg) {
                        if (agg.f0 != null && !agg.f0.equals(edge.f0)) {
                            System.out.println("error. agg != edge");
                        }
                        agg.f0 = edge.f0;
                        if (!agg.f1.contains(edge.f1)) {
                            agg.f1.add(edge.f1);
                        }
                        return agg;
                    }

                    @Override
                    public Tuple2<Long, List<Long>> getResult(Tuple2<Long, List<Long>> longListTuple2) {
                        System.out.println("get result");
                        return longListTuple2;
                    }

                    @Override
                    public Tuple2<Long, List<Long>> merge(Tuple2<Long, List<Long>> longListTuple2, Tuple2<Long, List<Long>> acc1) {
                        for (Long toAdd : longListTuple2.f1) {
                            if (!acc1.f1.contains(toAdd)) {
                                acc1.f1.add(toAdd);
                            }
                        }
                        return acc1;
                    }
                })
                .writeAsText("resources/AL/email-Eu-core", FileSystem.WriteMode.OVERWRITE );
    }


    public void makeALlist(DataStream<Edge<Long, String>> input) {
        input.keyBy(new KeySelector<Edge<Long, String>, Long>() {
            @Override
            public Long getKey(Edge<Long, String> edge) throws Exception {
                return edge.getSource();
            }
        }).process(new myProcessFunction())
    .print();
    //.writeAsText("resources/AL/email-Eu-core", FileSystem.WriteMode.OVERWRITE);
    }

    public static class myProcessFunction extends KeyedProcessFunction<Long, Edge<Long, String>, MapState<Long, LinkedList<Long>>> {

        private transient MapState<Long, LinkedList<Long>> state;
        long timestampForOutput = 0L;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<Long, LinkedList<Long>> descriptor = new MapStateDescriptor<Long, LinkedList<Long>>(
                    "state",
                    TypeInformation.of(Long.class),
                    TypeInformation.of(new TypeHint<LinkedList<Long>>() {
                    })
            );
            state = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement(Edge<Long, String> edge, Context context, Collector<MapState<Long, LinkedList<Long>>> collector) throws Exception {
            if (timestampForOutput == 0L) {
                timestampForOutput = context.timerService().currentProcessingTime();
                context.timerService().registerProcessingTimeTimer(timestampForOutput + 20000L);
            }
            long src = edge.getSource();
            long trg = edge.getTarget();
            if (!state.contains(src)) {
                state.put(src, new LinkedList<>());
            }
            if (!state.get(src).contains(trg)) {
                state.get(src).add(trg);
            }
            if (!state.contains(trg)) {
                state.put(trg, new LinkedList<>());
            }
            if (!state.get(trg).contains(src)) {
                state.get(trg).add(src);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<MapState<Long, LinkedList<Long>>> out) throws Exception {
            out.collect(state);
        }
    }

    public void makeAL2(DataStream<Edge<Long, String>> input) {
        input.flatMap(new FlatMapFunction<Edge<Long, String>, Tuple3<Long, LinkedList<Long>, Integer>>() {
            @Override
            public void flatMap(Edge<Long, String> edge, Collector<Tuple3<Long, LinkedList<Long>, Integer>> collector) throws Exception {
                LinkedList<Long> toAdd1 = new LinkedList<>();
                toAdd1.add(edge.getSource());
                collector.collect(Tuple3.of(edge.getTarget(), toAdd1, 1));
                LinkedList<Long> toAdd2 = new LinkedList<>();
                toAdd2.add(edge.getTarget());
                collector.collect(Tuple3.of(edge.getSource(), toAdd2, 1));
            }
        }).keyBy(new KeySelector<Tuple3<Long, LinkedList<Long>, Integer>, Long>() {
            @Override
            public Long getKey(Tuple3<Long, LinkedList<Long>, Integer> longLinkedListTuple2) throws Exception {
                return longLinkedListTuple2.f0;
            }
        }).reduce(new ReduceFunction<Tuple3<Long, LinkedList<Long>, Integer>>() {
            @Override
            public Tuple3<Long, LinkedList<Long>, Integer> reduce(Tuple3<Long, LinkedList<Long>, Integer> t2, Tuple3<Long, LinkedList<Long>, Integer> t1) throws Exception {
                t1.f1.addAll(t2.f1);
                if(!t1.f0.equals(t2.f0)) {
                    System.out.println("error");
                }

                return Tuple3.of(t1.f0, t1.f1, t1.f2+t2.f2);
            }
        }).keyBy(new KeySelector<Tuple3<Long, LinkedList<Long>, Integer>, Long>() {
            @Override
            public Long getKey(Tuple3<Long, LinkedList<Long>, Integer> longLinkedListIntegerTuple3) throws Exception {
                return longLinkedListIntegerTuple3.f0;
            }
        }).maxBy(2)
                .map(new MapFunction<Tuple3<Long, LinkedList<Long>, Integer>, Tuple2<Long, LinkedList<Long>>>() {
                    @Override
                    public Tuple2<Long, LinkedList<Long>> map(Tuple3<Long, LinkedList<Long>, Integer> longLinkedListIntegerTuple3) throws Exception {
                        return Tuple2.of(longLinkedListIntegerTuple3.f0, longLinkedListIntegerTuple3.f1);
                    }
                })
           .writeAsText("resources/AL/email-Eu-core", FileSystem.WriteMode.OVERWRITE);
    }

    public void AL3(DataStream<Edge<Long, String>> input) {
        input.keyBy(new KeySelector<Edge<Long, String>, Long>() {
            @Override
            public Long getKey(Edge<Long, String> edge) throws Exception {
                return edge.getSource();
            }
        }).process(new KeyedProcessFunction<Long, Edge<Long, String>, MapState<Long, HashSet<Long>>>() {
            MapState<Long, HashSet<Long>> state;

            @Override
            public void open(Configuration parameters) throws Exception {
                MapStateDescriptor<Long, HashSet<Long>> descriptor = new MapStateDescriptor<Long, HashSet<Long>>(
                        "state",
                        TypeInformation.of(Long.class),
                        TypeInformation.of(new TypeHint<HashSet<Long>>() {
                        })
                );
                state = getRuntimeContext().getMapState(descriptor);
            }

            @Override
            public void processElement(Edge<Long, String> edge, Context context, Collector<MapState<Long, HashSet<Long>>> collector) throws Exception {
                long src = edge.getSource();
                long trg = edge.getTarget();
                if(!state.contains(src)) {
                    state.put(src, new HashSet<>());
                }
                state.get(src).add(trg);
                if(!state.contains(trg)) {
                    state.put(trg, new HashSet<>());
                }
                state.get(trg).add(src);
                collector.collect(state);
            }
        }).keyBy(new KeySelector<MapState<Long, HashSet<Long>>, MapState<Long, HashSet<Long>>>() {

            @Override
            public MapState<Long, HashSet<Long>> getKey(MapState<Long, HashSet<Long>> longHashSetMapState) throws Exception {
                return longHashSetMapState;
            }
        }).reduce(new ReduceFunction<MapState<Long, HashSet<Long>>>() {
            @Override
            public MapState<Long, HashSet<Long>> reduce(MapState<Long, HashSet<Long>> t0, MapState<Long, HashSet<Long>> t1) throws Exception {
                return t1;
            }
        }).flatMap(new FlatMapFunction<MapState<Long, HashSet<Long>>, Tuple2<Long, List<Long>>>() {
            @Override
            public void flatMap(MapState<Long, HashSet<Long>> longHashSetMapState, Collector<Tuple2<Long, List<Long>>> collector) throws Exception {
                for(Long src : longHashSetMapState.keys()) {
                    List<Long> toOutput = new ArrayList<>(longHashSetMapState.get(src));
                    collector.collect(Tuple2.of(src, toOutput));
                }
            }
        }).writeAsText("resources/AL/email-Eu-core", FileSystem.WriteMode.OVERWRITE);
    }

    public HashMap<Long, HashSet<Long>> AL4(DataStream<Edge<Long, String>> input) throws InterruptedException {
        HelpState toReturn = new HelpState();
        AtomicInteger counter = new AtomicInteger(0);
        while(counter.get() != 	25571) {
            input.process(new ProcessFunction<Edge<Long, String>, Object>() {
                @Override
                public void processElement(Edge<Long, String> edge, Context context, Collector<Object> collector) throws Exception {
                    toReturn.addEdge(edge.getSource(), edge.getTarget());
                    counter.getAndIncrement();
                }
            });
        }
        while(counter.get()!= 25571) {
            wait();
        }
        return toReturn.returnState();
    }


}
