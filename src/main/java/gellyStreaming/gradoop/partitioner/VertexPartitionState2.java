package gellyStreaming.gradoop.partitioner;

import gellyStreaming.gradoop.util.KeyGen;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class VertexPartitionState2 implements Serializable {

    int[] keys;
    HashMap<Long, Integer> state;
    AtomicInteger[] sizePartitions;

    public VertexPartitionState2(int numPartitions) {
        KeyGen keyGenerator = new KeyGen(numPartitions,
                KeyGroupRangeAssignment.computeDefaultMaxParallelism(numPartitions));
        keys = new int[numPartitions];
        for (int i = 0; i < numPartitions ; i++) {
            keys[i] = keyGenerator.next(i);
        }
        state = new HashMap<>();
        sizePartitions = new AtomicInteger[numPartitions];
        for(int i = 0; i < numPartitions; i++) {
            sizePartitions[i] = new AtomicInteger(0);
        }

    }

    public int[] getPartition(long sourceVertex, long targetVertex) throws Exception {
        int partition1 = -1;
        if(!state.containsKey(sourceVertex)) {
            int smallest = Integer.MAX_VALUE;
            for(int i = 0; i < sizePartitions.length; i++) {
                if(sizePartitions[i].get() < smallest) {
                    smallest = sizePartitions[i].get();
                    partition1 = i;
                }
            }
            if(partition1 == -1) {
                throw new Exception();
            }
            state.put(sourceVertex, partition1);
        } else {
            partition1 = state.get(sourceVertex);
        }
        sizePartitions[partition1].getAndIncrement();

        int partition2 = -1;
        if(!state.containsKey(targetVertex)) {
            int smallest = Integer.MAX_VALUE;
            for(int i = 0; i < sizePartitions.length; i++) {
                if(sizePartitions[i].get() < smallest) {
                    smallest = sizePartitions[i].get();
                    partition2 = i;
                }
            }
            if(sizePartitions[partition2].get()*1.0/sizePartitions[partition1].get()>0.5) {
                partition2 = partition1;
            }
            state.put(targetVertex, partition2);
        } else {
            partition2 = state.get(targetVertex);
        }
        if(partition1 != partition2) {
            sizePartitions[partition2].getAndIncrement();
            return new int[]{keys[partition1], keys[partition2]};
        } else {
            return new int[]{keys[partition1]};
        }
    }
}

