package gellyStreaming.gradoop.util;

import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.util.MathUtils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

/*
Code to generate keys to use for the partitioner. Use simple 0 to x causes keys to be grouped together
in key groups when using the keyBy on the stream, therefor the partitionIDs/keys need to be so that
the build-in Flink hash does not group them together.
0-7 can be represented using: [1, 2, 4, 6, 9, 10, 11, 22]
from: m@ki at http://apache-flink-user-mailing-list-archive.2336050.n4.nabble.com/Use-keyBy-to-deterministically-hash-each-record-to-a-processor-task-slot-td16483.html

 */

public class KeyGen
{
    private int partitions;
    private int maxPartitions;
    private HashMap<Integer, Queue<Integer>> cache = new HashMap<Integer, Queue<Integer>>();
    private int lastPosition = 1;

    public KeyGen(final int partitions, final int maxPartitions)
    {
        this.partitions    = partitions;
        this.maxPartitions = maxPartitions;
    }

    public KeyGen(final int partitions)
    {
        this.partitions    = partitions;
        this.maxPartitions = 128;
    }

    public Integer next(int targetPartition)
    {
        Queue<Integer> queue;
        if (cache.containsKey(targetPartition))
            queue = cache.get(targetPartition);
        else
            queue = new LinkedList<Integer>();

        // Check if queue is empty
        if (queue.size() == 0)
        {
            boolean found = false;
            while (!found)
            {
                for (int id = lastPosition ; id < Integer.MAX_VALUE ; id++)
                {
                    //System.out.println("Hey " + id);

                    int partition = (MathUtils.murmurHash(id) %
                            maxPartitions) * partitions / maxPartitions;

                    if (cache.containsKey(partition))
                        queue = cache.get(partition);
                    else
                        queue = new LinkedList<Integer>();
                    // Add element to the queue
                    queue.add(id);

                    if (partition == targetPartition) {
                        found = true;
                        break; // break the for loop
                    }
                }
            }
        }

        return queue.poll(); // Return the first elements and deletes it -->similar to dequeue of scala's mutable.Queue
    }

    public int[] getKeys(int numPartitions) {
        KeyGen keyGenerator = new KeyGen(numPartitions,
                KeyGroupRangeAssignment.computeDefaultMaxParallelism(numPartitions));
        int[] procID = new int[numPartitions];

        for (int i = 0; i < numPartitions ; i++) {
            procID[i] = keyGenerator.next(i);
        }
        return procID;
    }

    public static void main(String[] args) throws Exception
    {
        //Generate intermediate keys
        final int p = 400;
        int numPartitions = p;
        int numKeys       = p;
        int parallelism   = p;

        KeyGen keyGenerator = new KeyGen(numPartitions,
                KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism));

        FileWriter fr = new FileWriter("generatedKeys");
        BufferedWriter bf = new BufferedWriter(fr);
        for(int i = 0; i < numKeys; i++) {
            int elem = keyGenerator.next(i);
            bf.write(""+elem);
            bf.newLine();
            System.out.println(elem);
        }
        bf.close();
        fr.close();

/*
        int[] procID = new int[numKeys];

        for (int i = 0; i < numKeys ; i++)
            procID[i] = keyGenerator.next(i);

        for (int elem : procID)
            System.out.println(elem);

 */
    }
}
