package dzmitry.loadbalancer;

import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

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
            return rnd.nextInt(instances.length);
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
            final int n = instances.length;
            final int val;
            synchronized (this) {
                val = cnt;
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
