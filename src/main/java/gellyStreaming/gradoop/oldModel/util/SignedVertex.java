package gellyStreaming.gradoop.oldModel.util;

import org.apache.flink.api.java.tuple.Tuple2;

public class SignedVertex extends Tuple2<Long, Boolean> {

public SignedVertex() {}

public SignedVertex(long vertex, boolean sign) {
        super(vertex, sign);
        }

public long getVertex() {
        return this.f0;
        }

public boolean getSign() {
        return this.f1;
        }

public SignedVertex reverse() {
        return new SignedVertex(this.getVertex(), !this.getSign());
        }
}