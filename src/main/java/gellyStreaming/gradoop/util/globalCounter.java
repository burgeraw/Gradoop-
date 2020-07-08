package gellyStreaming.gradoop.model;

import java.util.concurrent.atomic.AtomicLong;

public class globalCounter {

    private final long valueToReach;
    private final AtomicLong counter;

    public globalCounter(long valueToReach) {
        this.valueToReach = valueToReach;
        counter = new AtomicLong(0);
    }

    public void getAndIncrement() {
        if(counter.getAndIncrement() >= valueToReach) {
            System.exit(28);
        }
    }

}
