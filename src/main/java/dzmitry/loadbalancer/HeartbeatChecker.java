package dzmitry.loadbalancer;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

public class HeartbeatChecker implements AutoCloseable
{
    private final ScheduledExecutorService scheduledExecutor;
    /* Variable-size pool. Have to use it separately because
     * ScheduledExecutorService is a fixed-size pool so parallelism
     * is limited and can get stuck.
     */
    private final ExecutorService handlingExecutor;
    
    public HeartbeatChecker(final int schedulingThreadPoolSize)
    {
        final ThreadFactory threadFactory = runnable -> {
            final Thread t = new Thread(runnable);
            t.setDaemon(true);
            return t;
        };
        scheduledExecutor = Executors.newScheduledThreadPool(
                schedulingThreadPoolSize, threadFactory);
        boolean success = false;
        try {
            handlingExecutor = Executors.newCachedThreadPool(threadFactory);
            success = true;
        } finally {
            // Gracefully tearing down in case of exceptions during init.
            if (!success) {
                scheduledExecutor.shutdownNow();
            }
        }
    }
    
    public Future<?> registerChecker(final BooleanSupplier checker,
            final Consumer<Boolean> checkResultHandler,
            final long checkRateMs, final long timeoutMs)
    {
        /* A periodic task is started that spawns async heartbeat check
         * and then the task checks if the given asyn routine managed
         * to finish in time. If heartbeat check gets stalled
         * then the task would be able to detect it this way.
         */
        return scheduledExecutor.scheduleAtFixedRate(() -> {
            /* Running checking to support continuing on timeout even if
             * the checker function gets stuck.
             */
            final Future<Boolean> f = handlingExecutor.submit(
                    () -> checker.getAsBoolean());
            Boolean result;
            try {
                result = f.get(timeoutMs, TimeUnit.MILLISECONDS);
            }
            catch (TimeoutException | ExecutionException ex) {
                result = Boolean.FALSE;
            }
            catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(ex);
            }
            checkResultHandler.accept(result);
        }, 0, checkRateMs, TimeUnit.MILLISECONDS);
    }
    
    @Override
    public void close()
    {
        try {
            handlingExecutor.shutdownNow();
        }
        finally {
            scheduledExecutor.shutdownNow();
        }
    }
}
