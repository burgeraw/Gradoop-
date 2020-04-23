package gellyStreaming.gradoop.StatefulFunctions.streamingGradoop;

final class GraphMessages {

    static final class MyInputMessage {
        private final String srcId;
        private final String trgId;

        MyInputMessage(String srcId, String trgId) {
            this.srcId = srcId;
            this.trgId = trgId;
        }

        String getSrcId() {
            return srcId;
        }

        String getTrgId() {
            return trgId;
        }
    }

    static final class MyOutputMessage {
        private final String vertexId;
        private final String degree;

        MyOutputMessage(String vertexId, String degree) {
            this.vertexId = vertexId;
            this.degree = degree;
        }

        String getVertexId() {
            return vertexId;
        }

        String getDegree() {
            return degree;
        }

        @Override
        public String toString() {
            return String.format("MyOutputMessage(%s, %s)", getVertexId(), getDegree());
        }
    }
}
