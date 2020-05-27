package gellyStreaming.gradoop.algorithms;

import com.google.common.util.concurrent.AtomicDouble;
import it.unimi.dsi.fastutil.ints.Int2BooleanOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Random;

public class TriangleEstimator implements Serializable {

    private final Int2ObjectOpenHashMap<Int2BooleanOpenHashMap> srcToDsts = new Int2ObjectOpenHashMap(); // graph composed of the sampled edges
    private final Int2DoubleOpenHashMap nodeToTriangles = new Int2DoubleOpenHashMap(); // local triangle counts
    private AtomicDouble C = new AtomicDouble(0); // global triangles
    //private double C = 0;
    private final int sizeOfWaitingRoom; // size of the waiting room
    private final int sizeOfReservoir; // size of the reservoir

    private long curOfWaitingRoom = 0; // number of edges that can be in the waiting room
    private long curOfReservoir = 0; // number of edges that can be in the reservoir (can be larger than sizeOfWaitingRoom)

    private final int[][] waitingRoom;
    private final int[][] reservoir;

    private int indexForOldestEdge = 0;
    private final Random random;

    /**
     * create an instance for WRS
     *
     * @param k maximum number of samples
     * @param alpha relative size of the waiting room (between 0 and 1)
     * @param randomSeed random seed
     */
    public TriangleEstimator(int k, double alpha, int randomSeed) {
        random = new Random(randomSeed);
        sizeOfWaitingRoom = (int)(k * alpha);
        sizeOfReservoir = k - sizeOfWaitingRoom;
        waitingRoom = new int[2][sizeOfWaitingRoom];
        reservoir = new int[2][sizeOfReservoir];
        nodeToTriangles.defaultReturnValue(0);
    }

    /**
     * process an edge
     */
    public void processElement(Tuple2<Integer, Integer> input) {
        int src = input.f0;
        int dst = input.f1;

        if(src == dst) { //ignore self loop
            return;
        }

        count(src, dst); //count triangles involved

        boolean isInWaitingRoom = true; // whether the new edge should be stored in the waiting room

        if(curOfReservoir < sizeOfReservoir) { //reservoir
            reservoir[0][(int) curOfReservoir] = src;
            reservoir[1][(int) curOfReservoir] = dst;
            isInWaitingRoom = false;
            curOfReservoir++;
        }
        else if (curOfWaitingRoom < sizeOfWaitingRoom){
            waitingRoom[0][(int) curOfWaitingRoom] = src;
            waitingRoom[1][(int) curOfWaitingRoom] = dst;
            curOfWaitingRoom++;
        }
        else {
            int toMoveSrc = waitingRoom[0][indexForOldestEdge]; //edge popped from the waiting room
            int toMoveDst = waitingRoom[1][indexForOldestEdge];
            waitingRoom[0][indexForOldestEdge] = src;
            waitingRoom[1][indexForOldestEdge] = dst;
            indexForOldestEdge = (indexForOldestEdge + 1) % sizeOfWaitingRoom;
            curOfReservoir++;

            if(random.nextDouble() <  (sizeOfReservoir + 0.0) / curOfReservoir) { // popped edge is sampled

                srcToDsts.get(toMoveSrc).put(toMoveDst, false);
                srcToDsts.get(toMoveDst).put(toMoveSrc, false);

                int indexForSamplesProb = chooseIndex(sizeOfReservoir); // choose a random index
                deleteEdge(reservoir[0][indexForSamplesProb], reservoir[1][indexForSamplesProb]);
                reservoir[0][indexForSamplesProb] = toMoveSrc;
                reservoir[1][indexForSamplesProb] = toMoveDst;
            }
            else { // popped edge is not sampled
                deleteEdge(toMoveSrc, toMoveDst);
            }
        }

        if(!srcToDsts.containsKey(src)) {
            srcToDsts.put(src, new Int2BooleanOpenHashMap());
        }
        srcToDsts.get(src).put(dst, isInWaitingRoom);

        if(!srcToDsts.containsKey(dst)) {
            srcToDsts.put(dst, new Int2BooleanOpenHashMap());
        }
        srcToDsts.get(dst).put(src, isInWaitingRoom);

    }

    /**
     * choose an index randomly
     * @param n maximum index
     * @return
     */
    private int chooseIndex(int n) {
        return random.nextInt(n);
    }


    /**
     * delete the given edge from the sampled graph
     * @param src source of the edge to be removed
     * @param dst destination of the edge to be removed
     */
    private void deleteEdge(int src, int dst) {
        Int2BooleanOpenHashMap map = srcToDsts.get(src);
        map.remove(dst);
        if(map.isEmpty()) {
            srcToDsts.remove(src);
        }
        map = srcToDsts.get(dst);
        map.remove(src);
        if(map.isEmpty()) {
            srcToDsts.remove(dst);
        }
    }

    /**
     * get estimated global triangle count
     * @return estimate of global triangle count
     */
    public double getGlobalTriangle() {
        return C.get();
    }

    /**
     * get estimated local triangle counts
     * @return map from nodes to counts
     */
    public Int2DoubleMap getLocalTriangle() {
        return nodeToTriangles;
    }

    /**
     * counts triangles with the given edge
     * @param src the source node of the given edge
     * @param dst the destination node of the given edge
     */
    private void count(int src, int dst) {

        // if this edge has a new node, there cannot be any triangles
        if(!srcToDsts.containsKey(src) || !srcToDsts.containsKey(dst)) {
            return;
        }

        // source node to neighbors
        Int2BooleanOpenHashMap srcMap = srcToDsts.get(src);

        // destination node to neighbors
        Int2BooleanOpenHashMap dstMap = srcToDsts.get(dst);

        if(srcMap.size() > dstMap.size()) {
            Int2BooleanOpenHashMap temp = srcMap;
            srcMap = dstMap;
            dstMap = temp;
        }

        // the sum of counts increased
        double countSum = 0;

        for(int neighbor : srcMap.keySet()) {
            if (dstMap.containsKey(neighbor)) {
                boolean srcFlag = srcMap.get(neighbor);
                boolean dstFlag = dstMap.get(neighbor);
                double count = 1;
                if (srcFlag == false && dstFlag == false) {
                    count = Math.max((curOfReservoir + 0.0) / sizeOfReservoir * (curOfReservoir - 1.0) / (sizeOfReservoir - 1.0), 1);
                } else if (srcFlag == false || dstFlag == false) {
                    count = Math.max((curOfReservoir + 0.0) / sizeOfReservoir, 1);
                }
                countSum += count;
                nodeToTriangles.addTo(neighbor, count); // update the local triangle count of the common neighbor

            }
        }

        if(countSum > 0) {
            nodeToTriangles.addTo(src, countSum); // update the local triangle count of the source node
            nodeToTriangles.addTo(dst, countSum); // update the local triangle count of the destination node
            // update the global triangle count
            //C=+countSum;
            C.getAndAdd(countSum);
        }

    }

}