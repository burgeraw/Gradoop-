package gellyStreaming.gradoop.model;

import org.gradoop.common.model.impl.id.GradoopId;

public class GradoopIdUtil {
    public static Boolean getModulo(GradoopId gradoopId, Integer currentKey, Integer[] allKeys) {
        int mod = (gradoopId.hashCode())%(allKeys.length);
        return allKeys[mod].equals(currentKey);
    }
}
