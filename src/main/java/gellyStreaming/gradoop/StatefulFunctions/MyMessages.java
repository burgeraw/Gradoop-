package gellyStreaming.gradoop.StatefulFunctions;

final class MyMessages {

    static final class MyInputMessage {
        private final String userId;
        private final String message;

        MyInputMessage(String userId, String message) {
            this.userId = userId;
            this.message = message;
        }

        String getUserId() {
            return userId;
        }

        String getMessage() {
            return message;
        }
    }

    static final class MyOutputMessage {
        private final String userId;
        private final String content;

        MyOutputMessage(String userId, String content) {
            this.userId = userId;
            this.content = content;
        }

        String getUserId() {
            return userId;
        }

        String getContent() {
            return content;
        }

        @Override
        public String toString() {
            return String.format("MyOutputMessage(%s, %s)", getUserId(), getContent());
        }
    }
}