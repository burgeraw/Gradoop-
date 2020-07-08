package gellyStreaming.gradoop.util;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

public class ELtoAL {
    public static void main(String[] args) throws IOException {
        String inputEL = args[0];
        String outputAL = args[1];
        String makeUndirected = args[2];
        HelpState state = new HelpState(Boolean.parseBoolean(makeUndirected));
        FileReader fr = new FileReader(inputEL);
        BufferedReader br = new BufferedReader(fr);
        String line;
        while ((line = br.readLine()) != null) {
            String[] fields = line.split("\\s");
            long src = Long.parseLong(fields[0]);
            long trg = Long.parseLong(fields[1]);
            state.addEdge(src, trg);
        }
        br.close();
        fr.close();

        HashMap<Long, HashSet<Long>> stateFinal = state.returnState();

        File output = new File(outputAL);
        try (BufferedWriter bf = new BufferedWriter(new FileWriter(output))) {
            for (long src : stateFinal.keySet()) {
                bf.write(src + ":" + Arrays.toString(stateFinal.get(src).toArray()));
                bf.newLine();
            }
            bf.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("number of vertices: \t"+state.getNumberVertices());
        System.out.println("number of edges: \t"+state.getNumberEdges());
    }
}
