package dzmitry.loadbalancer;

import java.util.Objects;
import java.util.Random;

public class LoadBalancer
{
    private static final int MAX_SIZE = 10;
    
    private final Provider[] instances;
    private int size;
    private final Random rnd;
    
    public LoadBalancer()
    {
        instances = new Provider[MAX_SIZE];
        rnd = new Random(System.nanoTime());
    }
    
    public synchronized void register(final Provider instance)
    {
        Objects.requireNonNull(instance);
        if (size == MAX_SIZE) {
            throw new IllegalStateException("Max size already reached.");
        }
        instances[size++] = instance;
    }
    
    public String get()
    {
        final Provider provider;
        synchronized (this) {
            final int idx = rnd.nextInt(size);
            provider = instances[idx];
        }
        return provider.get();
    }
}
