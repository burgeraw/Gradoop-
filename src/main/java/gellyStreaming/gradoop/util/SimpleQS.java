package gellyStreaming.gradoop.util;

import gellyStreaming.gradoop.model.QueryState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.Random;

public class SimpleQS {

    private final QueryState QS;
    private final static int[] partitionvalues = new int[]{1,2,4,9};

    private SimpleQS() throws Exception {
        this.QS = new QueryState();
        int numberOfPartitions = 4;
        Configuration config = new Configuration();
        config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(numberOfPartitions, config);
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(numberOfPartitions);
        DataStream<Tuple2<Integer, Integer>> stream = env.addSource(new MySource());
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        stream.keyBy(new KeySelector<Tuple2<Integer, Integer>, Integer>() {
            @Override
            public Integer getKey(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {
                return integerIntegerTuple2.f0;
            }
        }).process(new KeyedProcessFunction<Integer, Tuple2<Integer, Integer>, String>() {

            private transient MapState<Integer, Integer> state;
            private transient ValueState<Long> lastOutput;
            private transient ValueState<Long> slowestRetrieval;


            @Override
            public void open(Configuration parameters) {
                MapStateDescriptor<Integer, Integer> descriptor = new MapStateDescriptor<Integer, Integer>(
                        "state",
                        TypeInformation.of(Integer.class),
                        TypeInformation.of(Integer.class));
                descriptor.setQueryable("state");
                state = getRuntimeContext().getMapState(descriptor);
                ValueStateDescriptor<Long> descriptor2 = new ValueStateDescriptor<Long>(
                        "lastOutputTime", Long.class);
                lastOutput = getRuntimeContext().getState(descriptor2);
                ValueStateDescriptor<Long> descriptor3 = new ValueStateDescriptor<Long>(
                        "slowestRetrieval", Long.class);
                slowestRetrieval = getRuntimeContext().getState(descriptor3);
            }

            @Override
            public void processElement(Tuple2<Integer, Integer> tuple, Context context, Collector<String> collector) throws Exception {
                while(!QS.isInitilized()) {
                    Thread.sleep(100);
                    System.out.println("QS not initialized");
                }
                state.put(tuple.f0, tuple.f1);
                if(lastOutput.value() == null) {
                    lastOutput.update(context.timerService().currentProcessingTime());
                    context.timerService().registerProcessingTimeTimer(lastOutput.value()+10000);
                }
                if((lastOutput.value()+10000)<context.timerService().currentProcessingTime()) {
                    lastOutput.update(lastOutput.value()+10000);
                    context.timerService().registerProcessingTimeTimer(lastOutput.value()+10000);

                }

            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) {
                int currentkey = ctx.getCurrentKey();
                for (int key : partitionvalues) {
                    if (key != currentkey) {
                        int tries = 0;
                        while (tries < 10) {
                            try {
                                MapState<Integer, Integer> otherstate = QS.getState3(key);
                                out.collect("we collected state: " + otherstate.keys().toString() + otherstate.values().toString());
                                long currenttime = ctx.timerService().currentProcessingTime();
                                out.collect("In partition " + currentkey + " we retrieve state from partition "
                                        + key + " for timer set for: " + timestamp + " at time: " + currenttime +
                                        ". This took " + tries + " tries.");
                                if (slowestRetrieval.value() == null ||
                                        slowestRetrieval.value() < (currenttime - timestamp)) {
                                    slowestRetrieval.update(currenttime - timestamp);
                                    System.out.println("Slowest retrieval: " + slowestRetrieval.value());
                                }
                                break;
                            } catch (Exception e) {
                                tries++;
                                if (tries == 10) {
                                    System.out.println("We failed to get state");
                                }
                            }
                        }

                    }
                }
            }

        }).print();
        JobClient results = env.executeAsync();
        QS.initialize2(results.getJobID());
    }

    public static void main(String[] args) throws Exception {
        final SimpleQS qs = new SimpleQS();
    }

    private class MyProcessFunction extends KeyedProcessFunction<Integer, Tuple2<Integer, Integer>, String> implements Serializable {

        private transient MapState<Integer, Integer> state;
        private transient ValueState<Long> lastOutput;
        private transient ValueState<Long> slowestRetrieval;


        @Override
        public void open(Configuration parameters) {
            MapStateDescriptor<Integer, Integer> descriptor = new MapStateDescriptor<Integer, Integer>(
                    "state",
                    TypeInformation.of(Integer.class),
                    TypeInformation.of(Integer.class));
            descriptor.setQueryable("state");
            state = getRuntimeContext().getMapState(descriptor);
            ValueStateDescriptor<Long> descriptor2 = new ValueStateDescriptor<Long>(
                    "lastOutputTime", Long.class);
            lastOutput = getRuntimeContext().getState(descriptor2);
            ValueStateDescriptor<Long> descriptor3 = new ValueStateDescriptor<Long>(
                    "slowestRetrieval", Long.class);
            slowestRetrieval = getRuntimeContext().getState(descriptor3);
        }

        @Override
        public void processElement(Tuple2<Integer, Integer> tuple, Context context, Collector<String> collector) throws Exception {
            while(!QS.isInitilized()) {
                Thread.sleep(100);
            }
            state.put(tuple.f0, tuple.f1);
            if(lastOutput.value() == null) {
                lastOutput.update(context.timerService().currentProcessingTime());
                context.timerService().registerProcessingTimeTimer(lastOutput.value()+10000);
            }
            if((lastOutput.value()+10000)<context.timerService().currentProcessingTime()) {
                lastOutput.update(lastOutput.value()+10000);
                context.timerService().registerProcessingTimeTimer(lastOutput.value()+10000);

            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            int currentkey = ctx.getCurrentKey();
            for(int key: partitionvalues) {
                if(key != currentkey) {
                    int tries = 0;
                    while(tries < 10) {
                        try{
                            MapState<Integer, Integer> otherstate = QS.getState3(key);
                            out.collect("we collected state: "+otherstate.keys().toString()+otherstate.values().toString());
                            long currenttime = ctx.timerService().currentProcessingTime();
                            out.collect("In partition "+currentkey+" we retrieve state from partition "
                                    + key+" for timer set for: "+timestamp+" at time: "+ currenttime +
                                    ". This took "+tries+" tries.");
                            if(slowestRetrieval.value()== null ||
                                    slowestRetrieval.value()< (currenttime-timestamp)) {
                                slowestRetrieval.update(currenttime-timestamp);
                                System.out.println("Slowest retrieval: " +slowestRetrieval.value());
                            }
                            tries = 10;
                        }
                        catch (Exception e) {
                            tries++;
                        }
                    }
                    if(tries == 9) {
                        System.out.println("We failed to get state");
                    }

                }
            }

        }
    }

    private class MySource implements SourceFunction<Tuple2<Integer, Integer>>{

        final int[] partitionvalues = new int[]{1,2,4,9};
        final Random random = new Random();
        @Override
        public void run(SourceContext<Tuple2<Integer, Integer>> sourceContext) throws Exception {
            while(true) {
                int index = random.nextInt(4);
                sourceContext.collect(new Tuple2<>(partitionvalues[index], 1));
                Thread.sleep(1);
            }
        }

        @Override
        public void cancel() {

        }
    }
}