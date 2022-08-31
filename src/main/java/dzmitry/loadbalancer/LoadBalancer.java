package dzmitry.loadbalancer;

import java.util.Objects;

public class LoadBalancer<T>
{
    private static final int MAX_SIZE = 10;
    
    private final T[] instances;
    private int size; // eventually consistent with instances.
    
    public LoadBalancer()
    {
        instances = (T[]) new Object[MAX_SIZE];
    }
    
    public synchronized void register(final T instance)
    {
        Objects.requireNonNull(instance);
        if (size == MAX_SIZE) {
            throw new IllegalStateException("Max size already reached.");
        }
        instances[size++] = instance;
    }
}
