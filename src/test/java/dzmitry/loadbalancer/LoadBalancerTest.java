package dzmitry.loadbalancer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
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
    public void testTooManyNoProviders()
    {
        final Provider[] providers = new Provider[11];
        for (int i = 0; i < providers.length; ++i) {
            providers[i] = Mockito.mock(Provider.class);
        }
        assertThrows(IllegalArgumentException.class,
                 () -> new LoadBalancer(providers, 3));
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
    public void testTwoProviders_RoundRobin_RepeatedActivationAndDeactivation()
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
        
        balancer.excludeNodes("p2");
        
        assertEquals("val1", balancer.get());
        
        balancer.excludeNodes("p1");
        
        assertThrows(IllegalStateException.class, () -> balancer.get());
        
        balancer.includeNodes("p2");
        
        assertEquals("val2", balancer.get());
        
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
    
    @Test
    public void testTooManyRequests_WithInactiveProviders() throws Exception
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
                throw new RuntimeException(ex);
            }
        });
        final Provider p3 = provider("p3", "val3");
        
        final LoadBalancer balancer = new LoadBalancer(
                new Provider[]{p1, p2, p3}, rqsPerNode);
        
        balancer.excludeNodes("p3");
        
        final Thread[] threads = new Thread[maxRqs];
        
        for (int i = 0; i < maxRqs; ++i) {
            final Thread t = new Thread(() -> {
                try {
                    balancer.get();
                }
                catch (Throwable ex) {
                    asyncFailure.set(true);
                    throw new RuntimeException(ex);
                }
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
    
    @Test
    public void testHeartbeatChecking_NoHeartbeatChecker()
    {
        final Provider p1 = provider("p1", "val1");
        
        final LoadBalancer balancer = new LoadBalancer(new Provider[]{p1}, 3);
        
        balancer.startHeartbeatChecking();
        balancer.close();
    }
    
    @Test
    public void testHeartbeatChecking_WithHeartbeatChecker()
    {
        final HeartbeatChecker checker = Mockito.mock(HeartbeatChecker.class);
        final HeartbeatCheckResultHandler handler =
                Mockito.mock(HeartbeatCheckResultHandler.class);
        final Provider p1 = provider("p1", "val1");
        final Provider p2 = provider("p2", "val2");
        
        final List<BooleanSupplier> checkCallbacks = new ArrayList<>();
        final List<Consumer<Boolean>> handleCallbacks = new ArrayList<>();
        
        Mockito.doAnswer(inv -> {
            checkCallbacks.add(inv.getArgument(0));
            handleCallbacks.add(inv.getArgument(1));
            return null;
        }).when(checker).registerChecker(any(), any(), anyLong(), anyLong());
        
        final LoadBalancer balancer = new LoadBalancer(
                new Provider[]{p1, p2}, SelectorType.RANDOM, 3,
                checker, handler, 123, 456);
        
        balancer.startHeartbeatChecking();
        
        Mockito.verify(checker, Mockito.times(2)).registerChecker(
                notNull(), notNull(), eq(123L), eq(456L));
        
        Mockito.verify(p1, Mockito.never()).check();
        Mockito.verify(p2, Mockito.never()).check();
        
        checkCallbacks.forEach(cb -> cb.getAsBoolean());
        Mockito.verify(p1).check();
        Mockito.verify(p2).check();
        
        Mockito.verify(handler, Mockito.never()).handle(anyBoolean(), any(), any());
        
        handleCallbacks.forEach(cb -> cb.accept(Boolean.TRUE));
        Mockito.verify(handler).handle(true, balancer, p1);
        Mockito.verify(handler).handle(true, balancer, p2);
        Mockito.verifyNoMoreInteractions(handler);
        
        Mockito.verify(checker, Mockito.never()).close();
        balancer.close();
        Mockito.verify(checker).close();
    }
    
    @Test
    public void testStartHeartbeatChecking_WithHeartbeatChecker_RepeatedCall()
    {
        final HeartbeatChecker checker = Mockito.mock(HeartbeatChecker.class);
        final HeartbeatCheckResultHandler handler =
                Mockito.mock(HeartbeatCheckResultHandler.class);
        final Provider p1 = provider("p1", "val1");
        final Provider p2 = provider("p2", "val2");
        
        final LoadBalancer balancer = new LoadBalancer(
                new Provider[]{p1, p2}, SelectorType.RANDOM, 3,
                checker, handler, 123, 456);
        
        balancer.startHeartbeatChecking();
        
        Mockito.verify(checker, Mockito.times(2)).registerChecker(
                notNull(), notNull(), eq(123L), eq(456L));
        
        balancer.startHeartbeatChecking();
        
        Mockito.verify(checker, Mockito.times(2)).registerChecker(
                notNull(), notNull(), eq(123L), eq(456L));
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
