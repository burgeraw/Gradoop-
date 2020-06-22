package gellyStreaming.gradoop.partitioner;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;

public class HelpState implements Serializable {

    HashMap<Long, HashSet<Long>> state;

    public HelpState() {
        state = new HashMap<>();
    }

    public void addEdge(long src, long trg) {
        if(!state.containsKey(src)) {
            state.put(src, new HashSet<>());
        }
        state.get(src).add(trg);
        //if(!state.containsKey(trg)) {
        //    state.put(trg, new HashSet<>());
        //}
        //state.get(trg).add(src);
    }

    public HashMap<Long, HashSet<Long>> returnState() {
        return state;
    }
}
