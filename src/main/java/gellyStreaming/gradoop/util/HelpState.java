package gellyStreaming.gradoop.util;

import java.util.HashMap;
import java.util.HashSet;

public class HelpState {

    HashMap<Long, HashSet<Long>> state;
    Boolean makeUndirected;

    public HelpState(Boolean makeUndirected) {
        state = new HashMap<>();
        this.makeUndirected = makeUndirected;
    }

    public void addEdge(long src, long trg) {
        if(!state.containsKey(src)) {
            state.put(src, new HashSet<>());
        }
        state.get(src).add(trg);

        if(makeUndirected) {
            if (!state.containsKey(trg)) {
                state.put(trg, new HashSet<>());
            }
            state.get(trg).add(src);
        }
    }

    public int getNumberVertices() {
        return state.keySet().size();
    }

    public int getNumberEdges() {
        int size = 0;
        for(long vertex : state.keySet()) {
            size = size + state.get(vertex).size();
        }
        return size;
    }

    public HashMap<Long, HashSet<Long>> returnState() {
        return state;
    }
}
