package dzmitry.loadbalancer;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

public class HeartbeatChecker implements AutoCloseable
{
    private final ScheduledExecutorService executor;
    
    public HeartbeatChecker()
    {
        executor = Executors.newScheduledThreadPool(0, runnable -> {
            final Thread t = new Thread(runnable);
            t.setDaemon(true);
            return t;
        });
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
        return executor.scheduleAtFixedRate(() -> {
            final Future<Boolean> f = executor.submit(
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
        executor.shutdownNow();
    }
}
