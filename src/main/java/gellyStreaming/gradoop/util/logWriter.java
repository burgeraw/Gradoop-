package gellyStreaming.gradoop.util;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.FileWriter;
import java.io.IOException;

public class logWriter {

    private final FileWriter log;
    private final String filename;

    public logWriter(String filename) throws IOException {
        log = new FileWriter(filename);
        this.filename = filename;
    }

    public void appendLine(String line) throws IOException {
        log.write(line+"\n");
    }

    public void closeWriter() throws IOException {
        log.close();
    }

    public void appendLines(DataStream<String> lines) {
        lines.writeAsText(filename, FileSystem.WriteMode.NO_OVERWRITE);
    }

}
