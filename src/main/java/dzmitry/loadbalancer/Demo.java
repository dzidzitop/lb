package dzmitry.loadbalancer;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class Demo
{
    public static void main(String[] args) throws Exception
    {
        final SelectorType selector = SelectorType.ROUND_ROBIN;
        final int nodeCount = 10; // number of providers (10 max as per tech specification).
        final int maxLoadPerNode = 9; // max number of simultaneous requests per node.
        final long heartbeatRateMs = 50; // heartbeat rate.
        final long heartbeatTimeoutMs = 18; // timeout for check(). 
        final int threadCount = 40; // number of clients.
        final int providerGetRandomSleepMs = 20; // 0-(n-1) - random sleep time in get().
        final int providerCheckRandomSleepMs = 20; // 0-(n-1) - random sleep time in check().
        final int providerCheckSuccessPercent = 95; // 0-(n-1) - chance for check() to succeed.
        final int clientProcessingRandomTimeMs = 0; // 0-(n-1_ - client random delay between calls.
        final int clientCallCount = 1000; // number of calls per client during demo.
        
        final Provider[] providers = new Provider[nodeCount];
        for (int i = 0; i < nodeCount; ++i) {
            providers[i] = new Provider() {
                @Override
                public String get()
                {
                    try {
                        if (providerGetRandomSleepMs > 0) {
                            final Random rnd = ThreadLocalRandom.current();
                            Thread.sleep(rnd.nextInt(providerGetRandomSleepMs));
                        }
                        return super.get();
                    }
                    catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(ex);
                    }
                }
                
                @Override
                public boolean check()
                {
                    try {
                        final Random rnd = ThreadLocalRandom.current();
                        if (providerCheckRandomSleepMs > 0) {
                            Thread.sleep(rnd.nextInt(providerCheckRandomSleepMs));
                        }
                        return (rnd.nextInt(100) + 1) <= providerCheckSuccessPercent;
                    }
                    catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(ex);
                    }
                }
            };
        }
        
        final HeartbeatChecker heartbeatChecker = new HeartbeatChecker(providers.length);
        final HeartbeatCheckResultHandler handler = new HeartbeatCheckResultHandlerImpl();
        
        final ConcurrentHashMap<String, AtomicInteger> hist =
                new ConcurrentHashMap<>();
        for (final Provider p : providers) {
            hist.put(p.getUuid(), new AtomicInteger());
        }
        hist.put("outOfService", new AtomicInteger());
        
        try (final LoadBalancer balancer = new LoadBalancer(
                providers, selector, maxLoadPerNode,
                heartbeatChecker, handler, heartbeatRateMs, heartbeatTimeoutMs)) {
            
            balancer.startHeartbeatChecking();
            
            final Thread[] threads = new Thread[threadCount];
            for (int i = 0; i < threads.length; ++i) {
                threads[i] = new Thread(() -> {
                    for (int j = 0; j < clientCallCount; ++j) {
                        try {
                            final String data = balancer.get();
                            hist.get(data).incrementAndGet();
                            if (clientProcessingRandomTimeMs > 0) {
                                Thread.sleep(ThreadLocalRandom.current().nextLong(
                                        clientProcessingRandomTimeMs));
                            }
                        }
                        catch (RuntimeException ex) {
                            hist.get("outOfService").incrementAndGet();
                        }
                        catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(ex);
                        }
                    }
                });
                threads[i].start();
            }
            
            for (int i = 0; i < threads.length; ++i) {
                threads[i].join();
            }
        }
        
        System.out.println("Call distribution: ");
        hist.entrySet().forEach(e -> System.out.println(e));
    }
}
