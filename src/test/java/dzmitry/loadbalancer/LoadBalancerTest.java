package dzmitry.loadbalancer;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class LoadBalancerTest
{
    @Test
    public void testNoProviders()
    {
         assertThrows(IllegalArgumentException.class,
                 () -> new LoadBalancer(new Provider[0], 3));
    }
    
    @Test
    public void testSingleProvider()
    {
        final Provider p1 = provider("p1", "val1");
        
        final LoadBalancer balancer = new LoadBalancer(new Provider[]{p1}, 3);
        
        assertEquals("val1", balancer.get());
        assertEquals("val1", balancer.get());
        
        balancer.excludeNodes("p1");
        
        assertThrows(IllegalStateException.class, () -> balancer.get());
        
        balancer.includeNodes("p1");
        
        assertEquals("val1", balancer.get());
    }
    
    @Test
    public void testTwoProviders()
    {
        final Provider p1 = provider("p1", "val1");
        final Provider p2 = provider("p2", "val2");
        
        final HashSet<String> values = new HashSet<>(Arrays.asList("val1", "val2"));
        
        final LoadBalancer balancer = new LoadBalancer(new Provider[]{p1, p2}, 3);
        
        assertTrue(values.contains(balancer.get()));
        assertTrue(values.contains(balancer.get()));
        
        balancer.excludeNodes("p1");
        
        assertEquals("val2", balancer.get());
        
        balancer.includeNodes("p1");
        balancer.excludeNodes("p2");
        
        assertEquals("val1", balancer.get());
        
        balancer.excludeNodes("p1");
        
        assertThrows(IllegalStateException.class, () -> balancer.get());
        
        balancer.includeNodes("p2");
        
        assertEquals("val2", balancer.get());
    }
    
    @Test
    public void testNoProviders_RoundRobin()
    {
         assertThrows(IllegalArgumentException.class,
                 () -> new LoadBalancer(new Provider[0], SelectorType.ROUND_ROBIN, 3));
    }
    
    @Test
    public void testSingleProvider_RoundRobin()
    {
        final Provider p1 = provider("p1", "val1");
        
        final LoadBalancer balancer = new LoadBalancer(
                new Provider[]{p1}, SelectorType.ROUND_ROBIN, 3);
        
        assertEquals("val1", balancer.get());
        assertEquals("val1", balancer.get());
        
        balancer.excludeNodes("p1");
        
        assertThrows(IllegalStateException.class, () -> balancer.get());
        
        balancer.includeNodes("p1");
        
        assertEquals("val1", balancer.get());
    }
    
    @Test
    public void testTwoProviders_RoundRobin()
    {
        final Provider p1 = provider("p1", "val1");
        final Provider p2 = provider("p2", "val2");
        
        final LoadBalancer balancer = new LoadBalancer(
                new Provider[]{p1, p2}, SelectorType.ROUND_ROBIN, 3);
        
        assertEquals("val1", balancer.get());
        assertEquals("val2", balancer.get());
        assertEquals("val1", balancer.get());
        assertEquals("val2", balancer.get());
        
        balancer.excludeNodes("p1");
        
        assertEquals("val2", balancer.get());
        
        balancer.includeNodes("p1");
        balancer.excludeNodes("p2");
        
        assertEquals("val1", balancer.get());
        
        balancer.excludeNodes("p1");
        
        assertThrows(IllegalStateException.class, () -> balancer.get());
        
        balancer.includeNodes("p2");
        
        assertEquals("val2", balancer.get());
    }
    
    @Test
    public void testTooManyRequests() throws Exception
    {
        final AtomicBoolean asyncFailure = new AtomicBoolean();
        final int rqsPerNode = 3;
        final int maxRqs = rqsPerNode * 2;
        final CountDownLatch getLatch = new CountDownLatch(1);
        final CountDownLatch testLatch = new CountDownLatch(maxRqs);
        final Provider p1 = provider("p1", () -> {
            try {
                testLatch.countDown();
                getLatch.await();
                return "val1";
            }
            catch (Throwable ex) {
                asyncFailure.set(true);
                throw new RuntimeException(ex);
            }
        });
        final Provider p2 = provider("p2", () -> {
            try {
                testLatch.countDown();
                getLatch.await();
                return "val2";
            }
            catch (Throwable ex) {
                asyncFailure.set(true);
                throw new RuntimeException(ex);
            }
        });
        
        final LoadBalancer balancer = new LoadBalancer(
                new Provider[]{p1, p2}, rqsPerNode);
        
        final Thread[] threads = new Thread[maxRqs];
        
        for (int i = 0; i < maxRqs; ++i) {
            final Thread t = new Thread(() -> {
                testLatch.countDown();
                balancer.get();
            });
            t.setDaemon(true);
            t.start();
            threads[i] = t;
        }
        
        try {
            testLatch.await(10, TimeUnit.SECONDS);
            
            // Request limit reached. Forcing overload.
            assertThrows(IllegalStateException.class, () -> balancer.get());
            
            getLatch.countDown();
        }
        finally {
            for (int i = 0; i < maxRqs; ++i) {
                threads[i].join(10_000);
            }
        }
        
        assertFalse(asyncFailure.get());
        
        final HashSet<String> values = new HashSet<>(Arrays.asList("val1", "val2"));
        
        assertTrue(values.contains(balancer.get()));
    }
    
    private static Provider provider(final String uuid, final String val)
    {
        final Provider result = Mockito.mock(Provider.class);
        Mockito.when(result.getUuid()).thenReturn(uuid);
        Mockito.when(result.get()).thenReturn(val);
        return result;
    }
    
    private static Provider provider(final String uuid,
            final Supplier<String> getImpl)
    {
        final Provider result = Mockito.mock(Provider.class);
        Mockito.when(result.getUuid()).thenReturn(uuid);
        Mockito.when(result.get()).thenAnswer(inv -> {
            return getImpl.get();
        });
        return result;
    }
}
