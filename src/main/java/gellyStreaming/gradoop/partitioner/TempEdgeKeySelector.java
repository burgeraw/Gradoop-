package gellyStreaming.gradoop.partitioner;

import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

import java.util.HashMap;

//TODO: fix unchecked cast?

public class TempEdgeKeySelector<GradoopId> implements KeySelector<TemporalEdge, GradoopId> {

    private static final HashMap<org.gradoop.common.model.impl.id.GradoopId, org.gradoop.common.model.impl.id.GradoopId> keyMap = new HashMap<org.gradoop.common.model.impl.id.GradoopId, org.gradoop.common.model.impl.id.GradoopId>();
    int k;

    public TempEdgeKeySelector(int k) {
        this.k = k;
    }

    @Override
    public GradoopId getKey(TemporalEdge temporalEdge) throws Exception {
        keyMap.put(temporalEdge.getSourceId(), temporalEdge.getTargetId());
        return (GradoopId) temporalEdge.getSourceId();
    }

    public org.gradoop.common.model.impl.id.GradoopId getValue(GradoopId key) throws Exception{
        org.gradoop.common.model.impl.id.GradoopId target =  keyMap.get(key);
        keyMap.clear();
        return target;
    }
}
