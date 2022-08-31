package dzmitry.loadbalancer;

import java.util.Objects;
import java.util.Random;

public class LoadBalancer
{
    private static final int MAX_SIZE = 10;
    
    private final Provider[] instances;
    private final Random rnd;
    
    public LoadBalancer(final Provider[] instances)
    {
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
        rnd = new Random(System.nanoTime());
    }
    
    public String get()
    {
        final Provider provider;
        synchronized (this) {
            final int idx = rnd.nextInt(instances.length);
            provider = instances[idx];
        }
        return provider.get();
    }
}
