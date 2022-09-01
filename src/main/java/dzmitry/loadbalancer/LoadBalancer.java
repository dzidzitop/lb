package dzmitry.loadbalancer;

import java.util.Objects;
import java.util.Random;

public class LoadBalancer
{
    private static final int MAX_SIZE = 10;
    
    private interface Selector
    {
        public int select();
    }
    
    private class RandomSelector implements Selector
    {
        private final Random rnd = new Random(System.nanoTime());
        
        @Override
        public int select()
        {
            final int[] activeIdxs = activeNodes;
            final int n = activeIdxs.length;
            if (n == 0) {
                throw new IllegalStateException("No active instances.");
            }
            return activeIdxs[rnd.nextInt(n)];
        }
    }
    
    private class RoundRobinSelector implements Selector
    {
        private int cnt;
        
        public RoundRobinSelector()
        {
            cnt = 0;
        }
        
        public int select()
        {
            final int[] activeIdxs = activeNodes;
            final int n = activeIdxs.length;
            if (n == 0) {
                throw new IllegalStateException("No active instances.");
            }
            int val;
            synchronized (this) {
                val = cnt;
                if (val >= n) {
                    /* Some nodes were excluded between two calls and
                     * this is causing overflow. Resetting the counter
                     * to the beginning of the active node list.
                     */
                    val = 0;
                }
                int next = val + 1;
                if (next == n) {
                    next = 0;
                }
                cnt = next;
            }
            return val;
        }
    }
    
    private final Provider[] instances;
    private final Selector selector;
    /* Contains indices of instances in the {@code instances} array
     * which are considered active (alive).
     * 
     * It is replaced each time the list of active nodes is modified.
     * So, after the array reference is read it can be used freely
     * as long as it is a consistent snapshot of active nodes which
     * was valid at the time this array is read.
     */
    private volatile int[] activeNodes;
    
    public LoadBalancer(final Provider[] instances)
    {
        this(instances, SelectorType.RANDOM);
    }
    
    public LoadBalancer(final Provider[] instances, final SelectorType selectorType)
    {
        Objects.requireNonNull(selectorType);
        final int n = instances.length;
        if (n == 0) {
            throw new IllegalArgumentException("No instances.");
        }
        if (n > MAX_SIZE) {
            throw new IllegalArgumentException(
                    "Too many instances. Max allowed: " + MAX_SIZE +
                    ", provided: " + n);
        }
        final Provider[] copy = new Provider[n];
        for (int i = 0; i < n; ++i) {
            final Provider p = instances[i];
            Objects.requireNonNull(p, "null provider");
            copy[i] = p;
        }
        this.instances = copy;
        final int[] activeIdxs = new int[n];
        for (int i = 0; i < n; ++i) {
            activeIdxs[i] = i;
        }
        activeNodes = activeIdxs;
        
        switch (selectorType) {
        case RANDOM:
            selector = new RandomSelector();
            break;
        case ROUND_ROBIN:
            selector = new RoundRobinSelector();
            break;
        default:
            throw new AssertionError(
                    "Unsupported selector type: " + selectorType);
        }
    }
    
    public String get()
    {
        final int idx = selector.select();
        final Provider provider = instances[idx];
        return provider.get();
    }
}
