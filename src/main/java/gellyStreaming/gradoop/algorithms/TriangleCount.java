package gellyStreaming.gradoop.algorithms;


import it.unimi.dsi.fastutil.ints.Int2BooleanOpenHashMap;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.IntegerTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.util.concurrent.AtomicDouble;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import scala.Int;

import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;

public class TriangleCount {

    private final static Random random = new Random();
    private static AtomicInteger indexForOldestEdge = new AtomicInteger(0);

    private static AtomicInteger sizeOfWaitingRoom;
    private static AtomicInteger sizeOfReservoir;

    private static final AtomicLong curOfWaitingRoom = new AtomicLong(0); // number of edges that can be in the waiting room
    private static final AtomicLong curOfReservoir = new AtomicLong(0); // number of edges that can be in the reservoir (can be larger than sizeOfWaitingRoom)

    private static final AtomicIntegerArray waitingRoom = new AtomicIntegerArray(sizeOfWaitingRoom.get()*2);
    private static final AtomicIntegerArray reservoir = new AtomicIntegerArray(sizeOfReservoir.get()*2);

    private static transient MapState<Integer, HashMap<Integer, Boolean>> srcToTrgs;
    private static transient MapState<Integer, Double> nodeToTriangles;
    private static AtomicDouble C = new AtomicDouble(0);

    static DataStream<Tuple2<Integer, Integer>> input;

    public TriangleCount(int k, double alpha, DataStream<Tuple2<Integer, Integer>> input) {
        sizeOfWaitingRoom = new AtomicInteger((int)(k * alpha));
        sizeOfReservoir = new AtomicInteger(k - sizeOfWaitingRoom.get());
        this.input = input;
        //waitingRoom = new AtomicIntegerArray(sizeOfWaitingRoom.get()*2);
        //reservoir = new AtomicIntegerArray(sizeOfReservoir.get()*2);
    }

    public DataStream<Double> getGlobalTriangles() {
        return input
                .keyBy(0, 1)
                .process(new KeyedProcessFunction<Tuple, Tuple2<Integer, Integer>, Double>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<Integer, HashMap<Integer, Boolean>> descriptor =
                                new MapStateDescriptor<Integer, HashMap<Integer, Boolean>>(
                                        "srcToDsts",
                                        TypeInformation.of(Integer.class),
                                        TypeInformation.of(new TypeHint<HashMap<Integer, Boolean>>() {
                                        })
                                );
                        srcToTrgs = getRuntimeContext().getMapState(descriptor);
                    }

                    @Override
                    public void processElement(Tuple2<Integer, Integer> edge, Context context, Collector<Double> collector) throws Exception {
                        int src = edge.f0;
                        int trg = edge.f1;
                        if (src == trg) {
                            return;
                        }

                        if (!srcToTrgs.contains(src) || !srcToTrgs.contains(trg)) {
                            return;
                        }

                        HashMap<Integer, Boolean> srcMap = srcToTrgs.get(src);
                        HashMap<Integer, Boolean> trgMap = srcToTrgs.get(trg);
                        if (srcMap.size() > trgMap.size()) {
                            HashMap<Integer, Boolean> temp = srcMap;
                            srcMap = trgMap;
                            trgMap = temp;
                        }
                        double countSum = 0;

                        for (int neighbor : srcMap.keySet()) {
                            if (trgMap.containsKey(neighbor)) {
                                boolean srcFlag = srcMap.get(neighbor);
                                boolean dstFlag = trgMap.get(neighbor);
                                double count = 1;
                                if (srcFlag == false && dstFlag == false) {
                                    count = Math.max((curOfReservoir.get() + 0.0) / sizeOfReservoir.get() * (curOfReservoir.get() - 1.0) / (sizeOfReservoir.get() - 1.0), 1);
                                } else if (srcFlag == false || dstFlag == false) {
                                    count = Math.max((curOfReservoir.get() + 0.0) / sizeOfReservoir.get(), 1);
                                }
                                countSum += count;
                            }
                        }
                        if (countSum > 0) {
                            C.getAndAdd(countSum);
                        }

                        boolean isInWaitingRoom = true; // whether the new edge should be stored in the waiting room

                        if (curOfReservoir.get() < sizeOfReservoir.get()) { //reservoir
                            reservoir.getAndSet((int) curOfReservoir.get(), src);
                            reservoir.getAndSet((int) (curOfReservoir.get()+sizeOfReservoir.get()),trg);
                            isInWaitingRoom = false;
                            curOfReservoir.getAndIncrement();
                        } else if (curOfWaitingRoom.get() < sizeOfWaitingRoom.get()) {
                            reservoir.getAndSet((int) curOfReservoir.get(), src);
                            reservoir.getAndSet((int) (curOfReservoir.get()+sizeOfReservoir.get()),trg);
                            curOfWaitingRoom.getAndIncrement();
                        } else {
                            int toMoveSrc = waitingRoom.get(indexForOldestEdge.get()); //edge popped from the waiting room
                            int toMoveTrg = waitingRoom.get(indexForOldestEdge.get()+sizeOfWaitingRoom.get());
                            waitingRoom.getAndSet(indexForOldestEdge.get(),src);
                            waitingRoom.getAndSet(indexForOldestEdge.get()+sizeOfWaitingRoom.get(),trg);
                            indexForOldestEdge.set((indexForOldestEdge.get() + 1) % sizeOfWaitingRoom.get());
                            curOfReservoir.getAndIncrement();

                            if (random.nextDouble() < (sizeOfReservoir.get() + 0.0) / curOfReservoir.get()) { // popped edge is sampled
                                srcToTrgs.get(toMoveSrc).put(toMoveTrg, false);
                                srcToTrgs.get(toMoveTrg).put(toMoveSrc, false);

                                int indexForSamplesProb = random.nextInt(sizeOfReservoir.get()); // choose a random index
                                int toRemoveSrc = reservoir.get(indexForSamplesProb);
                                int toRemoveTrg = reservoir.get(indexForSamplesProb+sizeOfReservoir.get());
                                HashMap<Integer, Boolean> map = srcToTrgs.get(toRemoveSrc);
                                map.remove(toRemoveTrg);
                                if (map.isEmpty()) {
                                    srcToTrgs.remove(toRemoveSrc);
                                }
                                map = srcToTrgs.get(toRemoveTrg);
                                map.remove(toRemoveTrg);
                                if (map.isEmpty()) {
                                    srcToTrgs.remove(toRemoveTrg);
                                }
                                reservoir.getAndSet(indexForSamplesProb,toMoveSrc);
                                reservoir.getAndSet(indexForSamplesProb+sizeOfReservoir.get(),toMoveTrg);
                            } else {
                                HashMap<Integer, Boolean> map = srcToTrgs.get(toMoveSrc);
                                map.remove(toMoveTrg);
                                if (map.isEmpty()) {
                                    srcToTrgs.remove(toMoveSrc);
                                }
                                map = srcToTrgs.get(toMoveTrg);
                                map.remove(toMoveTrg);
                                if (map.isEmpty()) {
                                    srcToTrgs.remove(toMoveTrg);
                                }
                            }
                        }

                        if (!srcToTrgs.contains(src)) {
                            srcToTrgs.put(src, new HashMap<Integer, Boolean>());
                        }
                        srcToTrgs.get(src).put(trg, isInWaitingRoom);

                        if (!srcToTrgs.contains(trg)) {
                            srcToTrgs.put(trg, new HashMap<Integer, Boolean>());
                        }
                        srcToTrgs.get(trg).put(src, isInWaitingRoom);
                        collector.collect(C.get());
                    }

                });
    }

    public DataStream<Tuple2<Integer, Double>> getLocalTriangles() {
        return null;
    }

    public DataStream<Tuple2<Integer, Double>> getGlobalAndLocalTriangles() {
        return null;
    }

}
