package gellyStreaming.gradoop.util;

import org.gradoop.common.model.impl.id.GradoopId;

public class GradoopIdUtil {
    public static Boolean getModulo(GradoopId gradoopId, Integer currentKey, Integer[] allKeys) {
        int mod = (gradoopId.hashCode())%(allKeys.length);
        if(mod<0) {
            mod = allKeys.length-1+mod;
        }
        return allKeys[mod].equals(currentKey);
    }

    public static Long getLong(GradoopId gradoopId) {
        // for example: 7fffffff0000000000000000 = 2147483647 = max vertex number allowed.
        String s = gradoopId.toString();
        String substring = s.substring(0,8);
        return Long.parseLong(substring, 16);
    }
}
