package gellyStreaming.gradoop.util;

import java.io.FileWriter;
import java.io.IOException;

public class logWriter {

    private final FileWriter log;

    public logWriter(String filename) throws IOException {
        log = new FileWriter(filename);
    }

    public void appendLine(String line) throws IOException {
        log.write(line+"\n");
    }

    public void closeWriter() throws IOException {
        log.close();
    }


}
