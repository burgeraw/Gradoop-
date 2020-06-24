package gellyStreaming.gradoop.model;

import org.gradoop.common.model.impl.id.GradoopId;

public class GradoopIdUtil {
    public static Boolean getModulo(GradoopId gradoopId, Integer currentKey, Integer[] allKeys) {
        int mod = (gradoopId.hashCode())%(allKeys.length);
        return allKeys[mod].equals(currentKey);
    }

    public static Long getLong(GradoopId gradoopId) {
        String s = gradoopId.toString();
        // example: 00000000ffffff0000000000
        String substring = s.substring(8,14);
        return Long.parseLong(substring, 16);
    }
}
