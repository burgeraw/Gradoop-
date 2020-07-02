package gellyStreaming.gradoop.algorithms;

import gellyStreaming.gradoop.model.QueryState;
import org.apache.flink.api.common.state.MapState;
import org.apache.zookeeper.server.ExitCode;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import java.util.HashMap;

import static gellyStreaming.gradoop.model.Experiments.myLogWriter;
import static gellyStreaming.gradoop.model.GraphState.globalCounter;

public class CheckNumberOfElementsAL implements Algorithm<String, MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>>> {

        long numberToCheck;

        public CheckNumberOfElementsAL(long numberToCheck) {
               this.numberToCheck = numberToCheck;
        }
        @Override
        public String doAlgorithm(MapState<Long, HashMap<GradoopId, HashMap<GradoopId, TemporalEdge>>> localState, QueryState QS, Integer localKey, Integer[] allKeys, long from, long maxValidTo) throws Exception {
                if(numberToCheck<=globalCounter.get()) {
                        myLogWriter.appendLine("Partition "+localKey+" is done at: "+System.currentTimeMillis());
                        myLogWriter.closeWriter();
                        System.exit(20);
                        return "done";
                } else {
                        return "notyet";
                }
        }
}
