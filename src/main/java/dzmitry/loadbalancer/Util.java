package dzmitry.loadbalancer;

import java.util.concurrent.atomic.AtomicInteger;

public class Util
{
    private Util()
    {
    }
    
    // Perfect lock-free modulo increment counter.
    public static int moduloIncrement(final AtomicInteger cnt, final int n)
    {
        int max = n - 1;
        for (;;) {
            int prev = cnt.get();
            int next = prev == max ? 0 : prev + 1;
            if (cnt.weakCompareAndSetPlain(prev, next)) {
                return prev;
            }
        }
    }
}
