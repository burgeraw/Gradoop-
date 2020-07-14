package gellyStreaming.gradoop.util;

import gellyStreaming.gradoop.Experiments;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class globalCounter implements Serializable {

    private final long valueToReach;
    private final AtomicLong counter;
    private final AtomicInteger counter2;
    private final int algValueToReach;

    public globalCounter(long valueToReach, int algValueToReach) {
        this.valueToReach = valueToReach;
        counter = new AtomicLong(0);
        counter2 = new AtomicInteger(0);
        this.algValueToReach = algValueToReach;
    }

    public void increment() throws Exception {
        if(valueToReach > 0 && counter.incrementAndGet() >= valueToReach) {
            System.out.println("Done putting all elements in state at: \t"+System.currentTimeMillis());
            Experiments.stream.stopInfiniteStream();
            //throw new Exception("We are done running. ");
        }
    }

    public void incrementAlgResults() throws Exception {
        if(algValueToReach > 0 && counter2.incrementAndGet() >= algValueToReach) {
            System.out.println("Done with all algorithms at: \t"+System.currentTimeMillis());
            Experiments.stream.stopInfiniteStream();
            //throw new Exception("We are done running. ");
        }
    }

}
