package gellyStreaming.gradoop.partitioner;

import gellyStreaming.gradoop.util.KeyGen;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

import java.util.Random;

/**
 * Created by zainababbas on 21/02/2017.
 */
public class DBHPartitioner<K, V> implements Partitioner<K> {
    private static final long serialVersionUID = 1L;
    private final CustomKeySelector<K, V> keySelector;
    private final int k;
    private final StoredVertexPartitionState currentState;
    private static final int MAX_SHRINK = 100;
    private final double seed;
    private final int shrink;
    private int[] keys;

    public DBHPartitioner(CustomKeySelector<K, V> keySelector, int k)
    {
        this.keySelector = keySelector;
        this.k= k;
        this.currentState = new StoredVertexPartitionState(k);
        seed = Math.random();
        Random r = new Random();
        shrink = r.nextInt(MAX_SHRINK);
        KeyGen keyGenerator = new KeyGen(k,
                KeyGroupRangeAssignment.computeDefaultMaxParallelism(k));
        keys = new int[k];
        for (int i = 0; i < k ; i++)
            keys[i] = keyGenerator.next(i);
    }

    @Override
    public int partition(K key, int numPartitions) {

        long target = 0L;
        try {
            Object target2 = keySelector.getValue(key);
            if(target2 != null) {
                target = (long) target2;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        long source = (long) key;

        int machine_id = -1;

        StoredVertex first_vertex = currentState.getRecord(source);
        StoredVertex second_vertex = currentState.getRecord(target);

        int shard_u = Math.abs((int) ( (int) source*seed*shrink) % k);
        int shard_v = Math.abs((int) ( (int) target*seed*shrink) % k);

        int degree_u = first_vertex.getDegree() +1;
        int degree_v = second_vertex.getDegree() +1;

        if (degree_v<degree_u){
            machine_id = shard_v;
        }
        else if (degree_u<degree_v){
            machine_id = shard_u;
        }
        else{ //RANDOM CHOICE
            //*** PICK A RANDOM ELEMENT FROM CANDIDATES
            Random r = new Random();
            int choice = r.nextInt(2);
            if (choice == 0){
                machine_id = shard_u;
            }
            else if (choice == 1){
                machine_id = shard_v;
            }
            else{
                System.out.println("ERROR IN RANDOM CHOICE DBH");
                System.exit(-1);
            }
        }
        //UPDATE EDGES
        currentState.incrementMachineLoad(machine_id);

        //UPDATE RECORDS
        if (currentState.getClass() == StoredVertexPartitionState.class){
            StoredVertexPartitionState cord_state = currentState;
            //NEW UPDATE RECORDS RULE TO UPFDATE THE SIZE OF THE PARTITIONS EXPRESSED AS THE NUMBER OF VERTICES THEY CONTAINS
            if (!first_vertex.hasReplicaInPartition(machine_id)){ first_vertex.addPartition(machine_id); cord_state.incrementMachineLoadVertices(machine_id);}
            if (!second_vertex.hasReplicaInPartition(machine_id)){ second_vertex.addPartition(machine_id); cord_state.incrementMachineLoadVertices(machine_id);}
        } else {
            //1-UPDATE RECORDS
            if (!first_vertex.hasReplicaInPartition(machine_id)){
                first_vertex.addPartition(machine_id);
            }
            if (!second_vertex.hasReplicaInPartition(machine_id)){
                second_vertex.addPartition(machine_id);
            }
        }

        //3-UPDATE DEGREES

        //System.out.println("source:"+source+" target:"+target+" machineid:"+machine_id);
        first_vertex.incrementDegree();
        second_vertex.incrementDegree();

        /*
        Mind that this uses custom keys to ensure that all partitions get keyed to different keygroups
        and different partitions. If using more partitions, ensure to generate more keys using KeyGen.java
         */

        return keys[machine_id];
    }



}



