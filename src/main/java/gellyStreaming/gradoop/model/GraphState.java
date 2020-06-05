package gellyStreaming.gradoop.model;

import akka.dispatch.ExecutorServiceConfigurator;
import akka.dispatch.ExecutorServiceFactory;
import akka.dispatch.ExecutorServiceFactoryProvider;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.library.clustering.directed.TriangleListing;
import org.apache.flink.hadoop.shaded.org.apache.http.HttpResponse;
import org.apache.flink.hadoop.shaded.org.apache.http.client.HttpClient;
import org.apache.flink.hadoop.shaded.org.apache.http.client.methods.HttpGet;
import org.apache.flink.hadoop.shaded.org.apache.http.impl.client.CloseableHttpClient;
import org.apache.flink.hadoop.shaded.org.apache.http.impl.client.HttpClients;
import org.apache.flink.runtime.dispatcher.SingleJobJobGraphStore;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.webmonitor.WebMonitorEndpoint;
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
import scala.Int;

import java.io.IOException;
import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class GraphState implements Serializable {

    private final KeyedStream<TemporalEdge, Integer> input;
    private static StreamExecutionEnvironment env;
    private static int[] keys;
    private static QueryState QS;
    private static JobID jobID;


    public GraphState(KeyedStream<TemporalEdge, Integer> input, String strategy) {
        this.input = input;
        switch (strategy) {
            case "EL": input.map(new createEdgeList()).writeAsText("out", FileSystem.WriteMode.OVERWRITE);
            case "EL2" : input.process(new createEdgeList2()).print();
            case "triangles" : input.process(new CountTrianglesWithinPu())
                    .writeAsText("out", FileSystem.WriteMode.OVERWRITE);

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
    // USING
    public GraphState(QueryState QS,
                      //StreamExecutionEnvironment env,
                      KeyedStream<TemporalEdge, Integer> input,
                      String strategy,
                      org.apache.flink.streaming.api.windowing.time.Time windowSize,
                      org.apache.flink.streaming.api.windowing.time.Time slide,
                      Integer numPartitions) throws IOException, InterruptedException {
        this.input = input;
        this.QS = QS;

        KeyGen keyGenerator = new KeyGen(numPartitions,
                KeyGroupRangeAssignment.computeDefaultMaxParallelism(numPartitions));
        keys = new int[numPartitions];
        for (int i = 0; i < numPartitions; i++)
            keys[i] = keyGenerator.next(i);

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



    public GraphState(QueryState QS,
                      KeyedStream<TemporalEdge, Integer> input,
                      String strategy,
                      Long windowSize,
                      Long slide,
                      Integer numPartitions) throws InterruptedException {
        this.input = input;
        this.QS = QS;
        KeyGen keyGenerator = new KeyGen(numPartitions,
                KeyGroupRangeAssignment.computeDefaultMaxParallelism(numPartitions));
        keys = new int[numPartitions];
        for (int i = 0; i < numPartitions; i++)
            keys[i] = keyGenerator.next(i);


        switch (strategy) {
            case "EL2" : input.process(new createEdgeList3(windowSize, slide))
                    .writeAsText("out", FileSystem.WriteMode.OVERWRITE);
            case "TTL" : input.process(new createEdgeList4(windowSize, slide))
                    .writeAsText("out", FileSystem.WriteMode.OVERWRITE);
            case "triangle" : input.process(new TriangleCounting(windowSize, slide))
                    .writeAsText("out", FileSystem.WriteMode.OVERWRITE);
            case "vertices" : input.process(new CountingVertices(windowSize, slide))
                    .print();
                    //.writeAsText("out", FileSystem.WriteMode.OVERWRITE);
        }
    }

    public GraphState(QueryState QS,
                      KeyedStream<TemporalEdge, Integer> input,
                      String strategy,
                      Integer numPartitions) throws InterruptedException {
        this.input = input;
        this.QS = QS;
        KeyGen keyGenerator = new KeyGen(numPartitions,
                KeyGroupRangeAssignment.computeDefaultMaxParallelism(numPartitions));
        keys = new int[numPartitions];
        for (int i = 0; i < numPartitions; i++)
            keys[i] = keyGenerator.next(i);
        //input.process(new CountTrianglesStream()).print();

        switch (strategy) {
            case "triangles" : input.process(new CountTrianglesStream()).print();
                //.writeAsText("out", FileSystem.WriteMode.OVERWRITE);
        }
    }

    public void overWriteQS(JobID jobID) throws UnknownHostException {
        this.QS.initialize(jobID);
    }

    public static class CountTrianglesWithinPu extends KeyedProcessFunction<Integer, TemporalEdge, String> {
        private MapState<GradoopId, HashSet<GradoopId>> adjacencyList;
        private ValueState<Integer> triangleCount;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<GradoopId, HashSet<GradoopId>> descriptor = new MapStateDescriptor<GradoopId, HashSet<GradoopId>>(
                    "adjacencyList",
                    TypeInformation.of(new TypeHint<GradoopId>() {}),
                    TypeInformation.of(new TypeHint<HashSet<GradoopId>>() {}));
            //descriptor.setQueryable("adjacencyList");
            adjacencyList = getRuntimeContext().getMapState(descriptor);
            ValueStateDescriptor<Integer> descriptor1 = new ValueStateDescriptor<Integer>(
                    "triangleCount",
                    Integer.class);
            triangleCount = getRuntimeContext().getState(descriptor1);
        }

        @Override
        public void processElement(TemporalEdge edge, Context context, Collector<String> collector) throws Exception {
            if (triangleCount.value() == null) {
                triangleCount.update(0);
            }

            GradoopId src = edge.getSourceId();
            GradoopId trg = edge.getTargetId();
            Boolean processEdge = true;
            try {
                if (adjacencyList.get(src).contains(trg)) {
                    //processEdge = false;
                }
                //if (adjacencyList.get(trg).contains(src)) {
                //    processEdge = false;
                //}
            } catch (NullPointerException ignored) {}
            if(processEdge){
                    try {
                        adjacencyList.get(src).add(trg);
                    } catch (NullPointerException e) {
                        HashSet<GradoopId> toPut = new HashSet<GradoopId>();
                        toPut.add(trg);
                        adjacencyList.put(src, toPut);
                    }

                    try {
                        adjacencyList.get(trg).add(src);
                    } catch (NullPointerException e) {
                        HashSet<GradoopId> toPut = new HashSet<GradoopId>();
                        toPut.add(src);
                        adjacencyList.put(trg, toPut);
                    }

                    HashSet<GradoopId> neighboursSrc = adjacencyList.get(src);
                    HashSet<GradoopId> neighboursTrg = adjacencyList.get(trg);

                    AtomicInteger triangles = new AtomicInteger(0);
                    if (neighboursSrc.size() < neighboursTrg.size()) {
                        for (GradoopId id : neighboursSrc) {
                            if (neighboursTrg.contains(id) && id != src && id != trg) {
                                triangles.getAndIncrement();
                            }
                        }
                    } else {
                        for (GradoopId id : neighboursTrg) {
                            if (neighboursSrc.contains(id) && id != src && id != trg) {
                                triangles.getAndIncrement();
                            }
                        }
                    }
                    triangleCount.update(triangleCount.value() + triangles.get());
                    collector.collect("We found " + triangles.get() + " new triangles, making the total " + triangleCount.value());
            }
        }
    }

    public static class CountTrianglesStream extends KeyedProcessFunction<Integer, TemporalEdge, String> {
        private MapState<GradoopId, HashSet<GradoopId>> adjacencyList;
        private ValueState<Integer> triangleCount;


        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<GradoopId, HashSet<GradoopId>> descriptor = new MapStateDescriptor<GradoopId, HashSet<GradoopId>>(
                    "adjacencyList",
                    TypeInformation.of(new TypeHint<GradoopId>() {}),
                    TypeInformation.of(new TypeHint<HashSet<GradoopId>>() {}));
            descriptor.setQueryable("adjacencyList");
            adjacencyList = getRuntimeContext().getMapState(descriptor);
            ValueStateDescriptor<Integer> descriptor1 = new ValueStateDescriptor<Integer>(
                    "triangleCount",
                    Integer.class);
            triangleCount = getRuntimeContext().getState(descriptor1);
        }

        @Override
        public void processElement(TemporalEdge edge, Context context, Collector<String> collector) throws Exception {
            if(triangleCount.value() == null) {
                triangleCount.update(0);
            }
            while(!QS.isInitilized()) {
                Thread.sleep(100);
            }
            GradoopId src = edge.getSourceId();
            GradoopId trg = edge.getTargetId();

            try{
                adjacencyList.get(src).add(trg);
            } catch (NullPointerException e) {
                HashSet<GradoopId> toPut = new HashSet<GradoopId>();
                toPut.add(trg);
                adjacencyList.put(src, toPut);
            }

            try{
                adjacencyList.get(trg).add(src);
            } catch (NullPointerException e) {
                HashSet<GradoopId> toPut = new HashSet<GradoopId>();
                toPut.add(src);
                adjacencyList.put(trg, toPut);
            }



            HashSet<GradoopId> neighboursSrc = adjacencyList.get(src);
            HashSet<GradoopId> neighboursTrg = adjacencyList.get(trg);
            int currentKey = context.getCurrentKey();
            for(int key : keys) {
                if (key != currentKey) {
                    boolean retry = true;
                    int numRetries = 0;
                    while (retry && numRetries < 10) {
                        try {
                            try {
                                neighboursSrc.addAll(QS.getState2(key).get(src));
                            } catch (NullPointerException ignored) {
                            }
                            try {
                                neighboursTrg.addAll(QS.getState2(key).get(trg));
                            } catch (NullPointerException ignored) {
                            }
                            retry = false;
                        } catch (Exception e) {
                            numRetries++;
                            if (numRetries == 10) {
                                System.out.println("We failed to get state after 10 tries for key: " + key + " and srcVertex: " +
                                        edge.getSourceId() + " and trgVertex: " + edge.getTargetId());
                            }
                        }
                    }
                }
            }
            AtomicInteger triangles = new AtomicInteger(0);
            if(neighboursSrc.size() < neighboursTrg.size()) {
                for(GradoopId id : neighboursSrc) {
                    if(neighboursTrg.contains(id) && id != src && id != trg) {
                        triangles.getAndIncrement();
                    }
                }
            } else {
                for(GradoopId id : neighboursTrg) {
                    if(neighboursSrc.contains(id) && id != src && id != trg) {
                        triangles.getAndIncrement();
                    }
                }
            }
            triangleCount.update(triangleCount.value()+triangles.get());
            collector.collect("We found "+triangles.get()+" new triangles, making the total "+triangleCount.value());
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

    public static class TriangleCounting extends KeyedProcessFunction<Integer, TemporalEdge, String> {
        private transient MapState<GradoopId, HashMap<GradoopId, TemporalEdge>> sortedEdgeList;
        private final Long window;
        private final Long slide;
        private transient ValueState<Long> lastOutput;
        private transient ValueState<Integer> triangleCount;
        private transient ValueState<Integer> edgeCountSinceTimestamp;
        private transient ValueState<Long> lastTimestamp;

        public TriangleCounting(Long window, Long slide) {
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
            // Make first timer, and trianglecount & make sure Query state is properly initilized
            if(lastTimestamp.value() == null) {
                lastTimestamp.update(context.timerService().currentProcessingTime());
            }
            long currentTime = lastTimestamp.value();
            int currentKey = context.getCurrentKey();
            if(lastOutput.value() == null) {
                while(!QS.isInitilized()) {
                    Thread.sleep(100);
                }
                /*
                boolean wait = true;
                while(wait) {
                    try{
                        for(int key: keys) {
                            QS.getState(key);
                        }
                        wait = false;
                    } catch (Exception ignored) {}
                }
                 */
                // Helps to ensure all thread have initialized state.
                //Thread.sleep(1000);
                //System.out.println("All QS working");
                lastOutput.update(currentTime);
                //lastOutput.update(context.timestamp());
                context.timerService().registerProcessingTimeTimer(lastOutput.value()+slide);
                context.timerService().registerProcessingTimeTimer(lastOutput.value()+slide+slide);
            }
            if(triangleCount.value() == null) {
                triangleCount.update(0);
            }
            if(edgeCountSinceTimestamp.value() == null) {
                edgeCountSinceTimestamp.update(0);
            }

            // Make new timer if last has expired.
            // prev. current processing time
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
                lastTimestamp.update(context.timerService().currentProcessingTime());
            }

            // Process edge, put in state, in both directions
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

            //Thread.sleep(100);
            // Check if edge forms triangle
            //int currentKey = context.getCurrentKey();
            HashMap<GradoopId, TemporalEdge> neighboursSrc = sortedEdgeList.get(edge.getSourceId());
            HashMap<GradoopId, TemporalEdge> neighboursTrg = sortedEdgeList.get(edge.getTargetId());
            for(int key : keys) {
                if(key != currentKey) {
                    boolean retry = true;
                    int numRetries = 0;
                    while(retry && numRetries<10) {
                        try {
                            try {
                                neighboursSrc.putAll(QS.getSrcVertex(key, edge.getSourceId()));
                            } catch (NullPointerException ignored) {}
                            try {
                                neighboursTrg.putAll(QS.getSrcVertex(key, edge.getTargetId()));
                            } catch (NullPointerException ignored) {}
                            retry = false;
                        }
                        catch (Exception e) {
                            System.out.println("In triangle count for key: "+key+ " and srcVertex: "+
                                    edge.getSourceId()+" and trgVertex: "+edge.getTargetId()+" we have: "+ e);
                            numRetries++;
                            if(numRetries==10) {
                                System.out.println("We failed to get state after 10 tries for key: "+key+ " and srcVertex: "+
                                        edge.getSourceId()+" and trgVertex: "+edge.getTargetId());
                            }
                        }
                    }
                }
            }
            //long currentTime = context.timerService().currentProcessingTime();
            AtomicInteger newTriangles = new AtomicInteger(0);
            if(neighboursSrc.size() < neighboursTrg.size()) {
                for(Map.Entry<GradoopId, TemporalEdge> neighbour : neighboursSrc.entrySet()) {
                    if(neighbour.getValue().getValidTo() > currentTime &&
                            neighboursTrg.containsKey(neighbour.getKey()) &&
                            neighboursTrg.get(neighbour.getKey()).getValidTo() > currentTime) {
                        //triangleCount.update(triangleCount.value() + 1);
                        newTriangles.getAndIncrement();
                    }
                }
            } else {
                for(Map.Entry<GradoopId, TemporalEdge> neighbour : neighboursTrg.entrySet()) {
                    if(neighbour.getValue().getValidTo() > currentTime &&
                            neighboursSrc.containsKey(neighbour.getKey()) &&
                            neighboursSrc.get(neighbour.getKey()).getValidTo() > currentTime) {
                        //triangleCount.update(triangleCount.value() + 1);
                        newTriangles.getAndIncrement();
                    }
                }
            }
            //int oldvalue = triangleCount.value();
            //int newvalue = oldvalue+newTriangles.get();
            triangleCount.update(triangleCount.value()+newTriangles.get());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect("At time: "+timestamp+" we have found a total of "+triangleCount.value()+" triangles");
            for(GradoopId srcId : sortedEdgeList.keys()) {
                for(GradoopId trgId : sortedEdgeList.get(srcId).keySet()) {
                    if(sortedEdgeList.get(srcId).get(trgId).getValidTo() < timestamp) {
                        sortedEdgeList.get(srcId).remove(trgId);
                    }
                    if(sortedEdgeList.get(srcId).isEmpty()) {
                        sortedEdgeList.remove(srcId);
                    }
                }
            }
            out.collect("The timer took " +(ctx.timerService().currentProcessingTime()-timestamp)+" ms to execute");
        }
    }

    public static class createEdgeList4 extends KeyedProcessFunction<Integer, TemporalEdge, String> {
        private transient MapState<GradoopId, HashMap<GradoopId, TemporalEdge>> edgeList;
        private final Long window;
        private final Long slide;
        private transient ValueState<Long> lastOutput;
        //private transient ValueState<Integer> totalEdges;
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
                    "sortedEdgeList",
                    TypeInformation.of(new TypeHint<GradoopId>() {
                    }),
                    TypeInformation.of(new TypeHint<HashMap<GradoopId, TemporalEdge>>() {
                    })
            );
            //ELdescriptor.enableTimeToLive(ttlConfig);
            ELdescriptor.setQueryable("sortedEdgeList");
            edgeList = getRuntimeContext().getMapState(ELdescriptor);
            ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<Long>(
                    "lastOutputTime", Long.class);
            lastOutput = getRuntimeContext().getState(descriptor);
            //ValueStateDescriptor<Integer> descriptor1 = new ValueStateDescriptor<Integer>(
            //        "total edges", Integer.class
            //);
            //totalEdges = getRuntimeContext().getState(descriptor1);

        }

        @Override
        public void processElement(TemporalEdge edge, Context context, Collector<String> collector) throws Exception {
            if(lastOutput.value() == null) {
                while(!QS.isInitilized()) {
                    Thread.sleep(100);
                }
                lastOutput.update(context.timerService().currentProcessingTime());
                context.timerService().registerProcessingTimeTimer(lastOutput.value()+slide);
            }
            while(context.timerService().currentProcessingTime()>(lastOutput.value()+slide)) {
                lastOutput.update(lastOutput.value()+slide);
                context.timerService().registerProcessingTimeTimer(lastOutput.value()+slide);
            }
                /*
                //Thread.sleep(10000);
                AtomicInteger uniqueVerices = new AtomicInteger(0);
                AtomicInteger duplicates = new AtomicInteger(0);
                List<GradoopId> srcVertices =
                        StreamSupport.stream(edgeList.keys().spliterator(), false)
                                .collect(Collectors.toList());
                int currentKey = context.getCurrentKey();
                for(int key : keys) {
                    if(key != currentKey) {
                        int tries = 0;
                        int maxtries = 10;
                        while(tries < maxtries) {
                            try {
                                MapState<GradoopId, HashMap<GradoopId, TemporalEdge>> state = QS.getState(key);
                                List<GradoopId> externalSrcVertices = StreamSupport.stream(state.keys().spliterator(), false)
                                        .collect(Collectors.toList());
                                collector.collect("Partition: "+key+" had: "+externalSrcVertices.size()+" srcVertices at time: "
                                        +context.timerService().currentProcessingTime());
                                break;
                            } catch (Exception e) {
                                tries++;
                                Thread.sleep(100);
                            }
                        }

                    }
                }
                //lastOutput.update(lastOutput.value()+slide);
                //context.timerService().registerProcessingTimeTimer(lastOutput.value()+slide);
            }

                 */
            try {
                edgeList.get(edge.getSourceId()).put(edge.getTargetId(), edge);
                //Thread.sleep(100);
            } catch (NullPointerException e) {
                HashMap<GradoopId, TemporalEdge> toPut = new HashMap<>();
                toPut.put(edge.getTargetId(), edge);
                edgeList.put(edge.getSourceId(), toPut);
                //Thread.sleep(100);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            AtomicInteger uniqueVerices = new AtomicInteger(0);
            AtomicInteger duplicates = new AtomicInteger(0);
            long beginWindow = timestamp - window;
            //List<Long> processingTimes = new LinkedList<>();
            List<GradoopId> srcVertices =
                    StreamSupport.stream(edgeList.keys().spliterator(), false)
                            .collect(Collectors.toList());
            int currentKey = ctx.getCurrentKey();
            out.collect("Local partition "+currentKey+" had "+ srcVertices.size()+" srcVertices at time: "+timestamp);
            for(int key : keys) {
                if(key != currentKey) {
                    int tries = 0;
                    int maxtries = 10;
                    while(tries < maxtries) {
                        try {
                            MapState<GradoopId, HashMap<GradoopId, TemporalEdge>> state = QS.getState(key);
                            List<GradoopId> externalSrcVertices = StreamSupport.stream(state.keys().spliterator(), false)
                                    .collect(Collectors.toList());
                            out.collect("External partition: "+key+" had: "+externalSrcVertices.size()+" srcVertices at time: "
                            +timestamp);
                            srcVertices.addAll(externalSrcVertices);
                            break;
                        } catch (Exception e) {
                            tries++;
                            System.out.println(tries);
                            //Thread.sleep(100);
                        }
                    }

                }
            }
            HashSet<GradoopId> distinct = new HashSet<>();
            for(GradoopId id : srcVertices) {
                distinct.add(id);
            }
            out.collect("Together the partitions have "+distinct.size()+" distinct vertices.");
            /*
            for(GradoopId srcVertex : srcVertices) {
                boolean isUnique = true;
                for(int key : keys) {
                    if(key != currentKey && isUnique) {
                        int tries = 0;
                        int maxtries = 10;
                        while(tries < maxtries && isUnique) {
                            try {
                                HashMap<GradoopId, TemporalEdge> answer = QS.getSrcVertex(key, srcVertex);
                                if (answer == null) {
                                    tries = maxtries;
                                } else {
                                    duplicates.getAndIncrement();
                                    System.out.println("We have a map for :" + srcVertex + " of size: " + answer.size());
                                    tries = maxtries;
                                    isUnique = false;
                                }
                                System.out.println("In GS:"+answer);
                            } catch (Exception e) {
                                //System.out.println(e);
                                tries++;
                            }
                        }
                    }
                }
                if(isUnique) {
                    uniqueVerices.getAndIncrement();
                }
                //try {
                //    Set<GradoopId> trgkeys = sortedEdgeList.get(key).keySet();
                //    for (GradoopId key2 : trgkeys) {
                //        //out.collect(Tuple2.of(key2, ctx.getCurrentKey()));

                 //       counter.getAndIncrement();
                 //   }
                //} catch (NullPointerException ignored) {}
            }
            //for (GradoopId srcId : keys) {
            //    counter++;
                //try {
                //    for(GradoopId trgId : sortedEdgeList.get(srcId).keySet()) {
                //        processingTimes.add(sortedEdgeList.get(srcId).get(trgId).getTxFrom());
                //    }
                //} catch (NullPointerException ignored) {}
            //}
            //int oldCounter = 0;
            //try{
            //        oldCounter = (int)totalEdges.value();}
            //catch (NullPointerException ignored) {}
            //int newValue = oldCounter + counter.intValue();
            //totalEdges.update(newValue);

             */
            //out.collect("The window " + beginWindow + " until " + timestamp + " contained " + uniqueVerices.get() +
            //        " unique SrcVertices & "+ duplicates.get() +" duplicate SrcVertices");
            //out.collect("Total edges so far: "+newValue);

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
                        "sortedEdgeList",
                        TypeInformation.of(new TypeHint<GradoopId>() {}),
                        TypeInformation.of(new TypeHint<HashMap<GradoopId, TemporalEdge>>() {})
                );
                ELdescriptor.setQueryable("sortedEdgeList");
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
                AtomicInteger unique = new AtomicInteger(0);
                AtomicInteger duplicates = new AtomicInteger(0);

                AtomicInteger counter = new AtomicInteger(0);
                while(!QS.isInitilized()) {
                    Thread.sleep(100);
                    counter.getAndIncrement();
                }
                if(counter.get()>0) {
                    System.out.println("Waited " + (counter.get() * 100) + " milliseconds");
                }
                Thread.sleep(100);


                for(GradoopId srcId : sortedEdgeList.keys()) {
                    boolean uniqueVertex = true;
                    for(java.lang.Integer otherkey : keys) {
                        if(otherkey != key) {
                            try {
                                HashMap<GradoopId, TemporalEdge> answer = QS.getState(otherkey).get(srcId);
                                if (answer != null) {
                                    duplicates.getAndIncrement();
                                    uniqueVertex = false;
                                    break;
                                }
                            } catch (Exception e) {
                                System.out.println("We failed to get state for key: "+key+" and srcVertex: "+srcId+" in GS. Exception: "+e);
                            }
                        }
                    }
                    if(uniqueVertex) {
                        unique.getAndIncrement();
                    }
                }
                String output = "We found "+unique.get()+" unique sourceVertices in partition " + key +
                        " and we found "+duplicates+" srcVertices that are also in other partitions."+
                        "This was at maxwindow: " + context.window().maxTimestamp();
                System.out.println(output);
                //collector.collect("We found "+unique.get()+" unique sourceVertices in partition " + key +
                //        " and we found "+duplicates+" srcVertices that are also in other partitions."+
                //        "This was at maxwindow: " + context.window().maxTimestamp());
                collector.collect(output);
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
