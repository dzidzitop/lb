package dzmitry.loadbalancer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class HeartbeatCheckerTest
{
    @Test
    public void testSingleChecker() throws Exception
    {
        final BooleanSupplier checkFunction = Mockito.mock(BooleanSupplier.class);
        final Consumer<Boolean> resultHandler = Mockito.mock(Consumer.class);
        
        final List<Boolean> checkResults =
                List.of(true, false, false, true);
        final List<Boolean> results = Collections.synchronizedList(
                new ArrayList<>());
        final AtomicInteger counter = new AtomicInteger();
        
        Mockito.when(checkFunction.getAsBoolean()).thenAnswer(inv -> {
            final int val = counter.getAndIncrement();
            if (val == checkResults.size()) {
                throw new RuntimeException();
            } else if (val > checkResults.size()) {
                return Boolean.TRUE;
            } else {
                return checkResults.get(val);
            }
        });
        Mockito.doAnswer(inv -> {
            results.add(inv.getArgument(0));
            return null;
        }).when(resultHandler).accept(any());
        
        try (final HeartbeatChecker checker = new HeartbeatChecker(10)) {
            final Future<?> fut = checker.registerChecker(
                    checkFunction, resultHandler, 10, 5);
            
            assertNotNull(fut);
            /* At least one element is added after an exception to
             * check processing is not stopped after it.
             */
            while (results.size() <= checkResults.size() + 1) {
                Thread.sleep(20);
            }
            fut.cancel(true);
            final ArrayList<Boolean> expectedDeterministicResults = new ArrayList<>();
            expectedDeterministicResults.addAll(checkResults);
            expectedDeterministicResults.add(Boolean.FALSE); // for exception
            expectedDeterministicResults.add(Boolean.TRUE); // for at least one tailing element.
            /* Cancelled task can still push results here.
             * Synchronising to make this deterministic.
             */
            synchronized (results) {
                assertEquals(expectedDeterministicResults,
                        results.subList(0, expectedDeterministicResults.size()));
            }
            for (int i = expectedDeterministicResults.size(),
                    n = results.size(); i < n; ++i) {
                assertTrue(results.get(i));
            }
        }
    }
}
