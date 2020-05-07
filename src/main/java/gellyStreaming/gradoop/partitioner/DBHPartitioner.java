package gellyStreaming.gradoop.partitioner;

import org.apache.flink.api.common.functions.Partitioner;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import java.util.Random;

public class DBHPartitioner<GradoopId> implements Partitioner<GradoopId> {

        private static final long serialVersionUID = 1L;
        TempEdgeKeySelector<GradoopId> keySelector;

        private final int k;
        StoredVertexPartitionState currentState;
        private static final int MAX_SHRINK = 100;
        private final double seed;
        private final int shrink;

        public DBHPartitioner(TempEdgeKeySelector<GradoopId> keySelector, int numPartitions)
        {
            this.keySelector = keySelector;
            this.k = numPartitions;
            this.currentState = new StoredVertexPartitionState(numPartitions);
            this.seed = Math.random();
            Random r = new Random();
            this.shrink = r.nextInt(MAX_SHRINK);
        }

        @Override
        public int partition(GradoopId key, int numPartitions) {

            GradoopId target = null;
            try {
                target = (GradoopId) keySelector.getValue((GradoopId) key);
            } catch (Exception e) {
                e.printStackTrace();
            }
            GradoopId source = (GradoopId) key;
            long source2 = Long.parseLong(source.toString(),16);
            long target2 = 0L;
            if(target!=null) {
                target2 = Long.parseLong(target.toString(),16);
            }
            int machine_id = -1;

            StoredVertex first_vertex = currentState.getRecord(source2);
            StoredVertex second_vertex = currentState.getRecord(target2);

            int shard_u = Math.abs((int) ( (int) source2*seed*shrink) % this.k);
            int shard_v = Math.abs((int) ( (int) target2*seed*shrink) % this.k);

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
            // Seems unnecessary edge creation?
            //TemporalEdge e = new TemporalEdge(GradoopId.get(), null, source, target, null, null, null, null);
            //currentState.incrementMachineLoad(machine_id,e);
            currentState.incrementMachineLoad(machine_id);

            //UPDATE RECORDS
            if (currentState.getClass() == StoredVertexPartitionState.class) {
                StoredVertexPartitionState cord_state = this.currentState;
                //NEW UPDATE RECORDS RULE TO UPDATE THE SIZE OF THE PARTITIONS EXPRESSED AS THE NUMBER OF VERTICES THEY CONTAIN
                if (!first_vertex.hasReplicaInPartition(machine_id)){
                    first_vertex.addPartition(machine_id); cord_state.incrementMachineLoadVertices(machine_id);
                }
                if (!second_vertex.hasReplicaInPartition(machine_id)){
                    second_vertex.addPartition(machine_id); cord_state.incrementMachineLoadVertices(machine_id);
                }
            }
            else{
                //1-UPDATE RECORDS
                if (!first_vertex.hasReplicaInPartition(machine_id)){ first_vertex.addPartition(machine_id);}
                if (!second_vertex.hasReplicaInPartition(machine_id)){ second_vertex.addPartition(machine_id);}
            }

            //3-UPDATE DEGREES

            System.out.print("source"+source);
            System.out.println("target"+target);
            System.out.println("machineid"+machine_id);
            first_vertex.incrementDegree();
            second_vertex.incrementDegree();

            return machine_id;
        }
}
