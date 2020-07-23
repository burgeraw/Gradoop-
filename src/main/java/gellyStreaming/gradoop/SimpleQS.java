package gellyStreaming.gradoop;


import gellyStreaming.gradoop.model.QueryState;
import org.apache.flink.api.common.JobID;
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
import org.gradoop.common.model.impl.id.GradoopId;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Random;

public class SimpleQS {
    private transient static QueryState QS;
    private final static QueryState QS2 = new QueryState();
    private final static int[] partitionvalues = new int[]{1,2,4,9};
    private final static JobID[] jobid = new JobID[1];

    public static void main(String[] args) {
        int numberOfPartitions = 4;
        Configuration config = new Configuration();
        config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(numberOfPartitions, config);
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(numberOfPartitions);
        DataStream<Tuple2<Integer, Integer>> stream = env.addSource(new MySource());
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        QS = new QueryState();
        final QueryState QS1 = QS;
        stream.keyBy(new KeySelector<Tuple2<Integer, Integer>, Integer>() {
            @Override
            public Integer getKey(Tuple2<Integer, Integer> integerIntegerTuple2) {
                return integerIntegerTuple2.f0;
            }
        }).process(new MyProcessFunction()).print();
        try {
            JobClient results = env.executeAsync();
            JobID jobID = results.getJobID();
            jobid[0] = jobID;
            QS2.initialize2(jobID);
            QS1.initialize2(jobID);
            QS.initialize(jobID);
        } catch (Exception e) {
            e.printStackTrace();
        }
        QS = QS1;
    }

    private static class MyProcessFunction extends KeyedProcessFunction<Integer, Tuple2<Integer, Integer>, String> {

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
            lastOutput = getRuntimeContext().getState(descriptor2);
            ValueStateDescriptor<Long> descriptor3 = new ValueStateDescriptor<Long>(
                    "slowestRetrieval", Long.class);
            slowestRetrieval = getRuntimeContext().getState(descriptor3);
            int counter = 0;
            while(!QS2.isInitilized()&& counter < 30) {
                System.out.println("QS2 not initialized");
                try {
                    if(jobid[0] != null) {
                        QS2.initialize(jobid[0]);
                    } else {
                        System.out.println("jobid still null;");
                        counter++;
                        Thread.sleep(100);
                    }
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                } catch (InterruptedException ignored) { }
            }
            if(QS2.isInitilized()) {
                System.out.println("QS2 initialized");
            }
            counter = 0;
            while(!QS.isInitilized()&& counter < 30) {
                System.out.println("QS not initialized");
                try {
                    if(jobid[0] != null) {
                        QS.initialize(jobid[0]);
                    } else {
                        System.out.println("jobid still null;");
                        counter++;
                        Thread.sleep(100);
                    }
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                } catch (InterruptedException ignored) { }
            }
            if(QS.isInitilized()) {
                System.out.println("QS initialzed");
            }
        }

        @Override
        public void processElement(Tuple2<Integer, Integer> tuple, Context context, Collector<String> collector) throws Exception {
            state.put(tuple.f0, tuple.f1);
                if (lastOutput == null) {
                    lastOutput.update(context.timerService().currentProcessingTime());
                    context.timerService().registerProcessingTimeTimer(lastOutput.value() + 10000);
                }
                if ((lastOutput.value() + 10000) < System.currentTimeMillis()) {
                    lastOutput.update(lastOutput.value() + 10000);
                    context.timerService().registerProcessingTimeTimer(lastOutput.value() + 10000);

                }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) {
            int currentkey = ctx.getCurrentKey();
            for(int key: partitionvalues) {
                if(key != currentkey) {
                    int tries = 0;
                    while(tries < 10) {
                        try{
                            MapState<GradoopId, HashSet<GradoopId>> otherstate = QS2.getState2(key);
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
                            if(tries == 10) {
                                System.out.println("We failed to get state");
                                e.printStackTrace();
                            }
                        }
                    }


                }
            }

        }
    }

    private static class MySource implements SourceFunction<Tuple2<Integer, Integer>>{

        int[] partitionvalues = new int[]{1,2,4,9};
        Random random = new Random();
        int counter = 0;
        @Override
        public void run(SourceContext<Tuple2<Integer, Integer>> sourceContext) throws InterruptedException {
            //while(counter < 10000) {
            while(true) {
                int index = random.nextInt(4);
                sourceContext.collect(new Tuple2<>(partitionvalues[index], 1));
                Thread.sleep(10);
                counter++;
            }
        }

        @Override
        public void cancel() {

        }
    }
}