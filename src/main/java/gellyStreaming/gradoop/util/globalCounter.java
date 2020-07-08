package gellyStreaming.gradoop.util;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.util.concurrent.atomic.AtomicLong;

public class globalCounter {

    private final long valueToReach;
    private final AtomicLong counter;

    public globalCounter(long valueToReach) {
        this.valueToReach = valueToReach;
        counter = new AtomicLong(0);
    }

    public void increment() {
        if(valueToReach > 0 && counter.incrementAndGet() >= valueToReach) {
            System.out.println("Done putting all elements in state at: \t"+System.currentTimeMillis());
            makeSimpleTemporalEdgeStream.stopInfiniteStream();
        }

    }

}
