package dzmitry.loadbalancer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

public class LoadBalancer implements AutoCloseable
{
    private static final int MAX_SIZE = 10;
    
    private interface Selector
    {
        public int select();
    }
    
    private class RandomSelector implements Selector
    {
        private final Random rnd = new Random(System.nanoTime());
        
        @Override
        public int select()
        {
            final int[] activeIdxs = activeNodes;
            final int n = activeIdxs.length;
            if (n == 0) {
                throw new IllegalStateException("No active instances.");
            }
            return activeIdxs[rnd.nextInt(n)];
        }
    }
    
    private class RoundRobinSelector implements Selector
    {
        private int cnt;
        
        public RoundRobinSelector()
        {
            cnt = 0;
        }
        
        public int select()
        {
            final int[] activeIdxs = activeNodes;
            final int n = activeIdxs.length;
            if (n == 0) {
                throw new IllegalStateException("No active instances.");
            }
            int val;
            synchronized (this) {
                val = cnt;
                if (val >= n) {
                    /* Some nodes were excluded between two calls and
                     * this is causing overflow. Resetting the counter
                     * to the beginning of the active node list.
                     */
                    val = 0;
                }
                int next = val + 1;
                if (next == n) {
                    next = 0;
                }
                cnt = next;
            }
            return activeIdxs[val];
        }
    }
    
    /** All instances this load balancer manages. Indices of active
     * instances are stored in {@code activeNodes}. */
    private final Provider[] instances;
    
    /** Strategy to distribute load used by this load balancer. */
    private final Selector selector;
    
    /**
     * The maximum number of simultaneous requests allowed
     * for a single node.
     */
    private final int maxLoadPerNode;
    /** Holds number of requests that are currently being processed. */
    private long requestCounter;
    /** This lock is used to count active requests. */
    private final Object requestCounterLock;
    
    /**
     * Contains indices of instances in the {@code instances} array
     * which are considered active (alive).
     * 
     * It is replaced each time the list of active nodes is modified.
     * So, after the array reference is read it can be used freely
     * as long as it is a consistent snapshot of active nodes which
     * was valid at the time this array is read.
     */
    private volatile int[] activeNodes;
    
    /** Used to synchronise upon to activate/deactivate nodes. */
    private final Object activeNodeLock;
    
    private final HeartbeatChecker heartbeatChecker;
    private final HeartbeatCheckResultHandler heartbeatHandler;
    /**
     * Time interval of issuing another heartbeat check after the time
     * when the previous check should be issued. Constant rate of
     * issuing requests is followed regardless of how responsive
     * nodes are.
     */
    private final long heartbeatCheckRateMs;
    /**
     * Timeout in milliseconds for waiting a response from a
     * heartbeat check call.
     */
    private final long heartbeatCheckTimeoutMs;
    private boolean heartbeatCheckStarted;
    
    /**
     * Mapping from node UUID and its index in {@code instances}.
     * Useful to simplify and speedup algorithms that change
     * set of active nodes.
     */
    private final Map<String, Integer> uuidToIdx;
    
    public LoadBalancer(final Provider[] instances, final int maxLoadPerNode)
    {
        this(instances, SelectorType.RANDOM, maxLoadPerNode);
    }
    
    public LoadBalancer(final Provider[] instances,
            final SelectorType selectorType, final int maxLoadPerNode)
    {
        this(instances, selectorType, maxLoadPerNode, null, null, -1, -1);
    }
    
    public LoadBalancer(final Provider[] instances,
            final SelectorType selectorType, final int maxLoadPerNode,
            final HeartbeatChecker heartbeatChecker,
            final HeartbeatCheckResultHandler heartbeatHandler,
            final long heartbeatCheckRateMs,
            final long heartbeatCheckTimeoutMs)
    {
        Objects.requireNonNull(selectorType);
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
        final HashMap<String, Integer> uuidToIdx = new HashMap<>();
        for (int i = 0; i < n; ++i) {
            final Provider p = instances[i];
            Objects.requireNonNull(p, "null provider");
            copy[i] = p;
            uuidToIdx.put(p.getUuid(), i);
        }
        this.instances = copy;
        this.uuidToIdx = uuidToIdx;
        
        final int[] activeIdxs = new int[n];
        for (int i = 0; i < n; ++i) {
            activeIdxs[i] = i;
        }
        activeNodes = activeIdxs;
        activeNodeLock = new Object();
        
        switch (selectorType) {
        case RANDOM:
            selector = new RandomSelector();
            break;
        case ROUND_ROBIN:
            selector = new RoundRobinSelector();
            break;
        default:
            throw new AssertionError(
                    "Unsupported selector type: " + selectorType);
        }
        
        this.maxLoadPerNode = maxLoadPerNode;
        requestCounter = 0;
        requestCounterLock = new Object();
        
        this.heartbeatChecker = heartbeatChecker;
        this.heartbeatHandler = heartbeatHandler;
        this.heartbeatCheckRateMs = heartbeatCheckRateMs;
        this.heartbeatCheckTimeoutMs = heartbeatCheckTimeoutMs;
    }
    
    public String get()
    {
        boolean accepted = false;
        final long maxRequests = maxLoadPerNode * activeNodes.length;
        synchronized (requestCounterLock) {
            if (requestCounter < maxRequests) {
                ++requestCounter;
                accepted = true;
            }
        }
        if (accepted) {
            try {
                final int idx = selector.select();
                final Provider provider = instances[idx];
                return provider.get();
            }
            finally {
                synchronized (requestCounterLock) {
                    --requestCounter;
                }
            }
        } else {
            throw new IllegalStateException(
                    "Max number of simultaneous requests reached.");
        }
    }
    
    public void excludeNode(final String uuid)
    {
        /* 
         * It costs O(activeNodes.length) which can be too expensive
         * for large instance pools. If it is the case and if active
         * nodes updated frequently then adapt data structures to support
         * complexity closer to the number of nodes to exclude.
         */
        
        final int nodeIdx = getNodeIdx(uuid);
        
        /* Checking if it is already excluded (to increase parallelism
         * and produce less memory garbage).
         */
        quickCheck: {
            final int[] activeIdxs = activeNodes;
            for (int activeIdx : activeIdxs) {
                if (nodeIdx == activeIdx) {
                    break quickCheck;
                }
            }
            /* It is already inactive. No need to change any supplementary
             * structure. (Of course this can become obsolete a moment
             * after taking a snapshot of active nodes).
             */
            return;
        }
        
        synchronized (activeNodeLock) {
            final int[] activeIdxs = activeNodes;
            final int[] newActiveIdxs = new int[activeIdxs.length];
            int newCount = 0;
            for (final int activeIdx : activeIdxs) {
                if (nodeIdx != activeIdx) {
                    newActiveIdxs[newCount++] = activeIdx;
                }
            }
            replaceActiveNodes(newActiveIdxs, newCount);
        }
    }
    
    public void includeNode(final String uuid)
    {
        /* 
         * It costs O(instances.length) which can be too expensive
         * for large instance pools. If it is the case and if active
         * nodes updated frequently then adapt data structures to support
         * complexity closer to the number of nodes to exclude.
         */
        
        final int nodeIdx = getNodeIdx(uuid);
        
        /* Checking if it is already included (to increase parallelism
         * and produce less memory garbage).
         */
        {
            final int[] activeIdxs = activeNodes;
            for (int activeIdx : activeIdxs) {
                if (nodeIdx == activeIdx) {
                    /* Already active. Nothing to do. (Of course this can
                     * become obsolete a moment after taking a snapshot of
                     * active nodes).
                     */
                    return;
                }
            }
        }
        
        final HashSet<String> activatedUuids = new HashSet<>();
        synchronized (activeNodeLock) {
            final int[] activeIdxs = activeNodes;
            for (final int idx : activeIdxs) {
                final Provider instance = instances[idx];
                activatedUuids.add(instance.getUuid());
            }
            final int[] newActiveIdxs =
                    Arrays.copyOf(activeIdxs, instances.length);
            int newCount = activeIdxs.length;
            if (activatedUuids.add(uuid)) {
                /* It is not an already activated UUID.
                 * Processing further.
                 */
                final Integer idx = uuidToIdx.get(uuid);
                if (idx == null) {
                    throw new IllegalArgumentException(
                            "Unknown UUID: " + uuid);
                }
                newActiveIdxs[newCount++] = idx.intValue();
            }
            replaceActiveNodes(newActiveIdxs, newCount);
        }
    }
    
    // Atomically replacing old active nodes with updated ones.
    private void replaceActiveNodes(final int[] newActiveNodes,
            final int nodeCount)
    {
        assert Thread.holdsLock(activeNodeLock);
        activeNodes = Arrays.copyOf(newActiveNodes, nodeCount);
    }
    
    private int getNodeIdx(final String uuid)
    {
        final Integer uuidIdx = uuidToIdx.get(uuid);
        if (uuidIdx == null) {
            throw new IllegalStateException("Unknown UUID.");
        }
        return uuidIdx.intValue();
    }
    
    /* It is not inside a constructor to avoid exposing
     * a reference to an unconstructed object.
     */
    public void startHeartbeatChecking()
    {
        if (heartbeatChecker != null) {
            synchronized (this) {
                if (!heartbeatCheckStarted) {
                    for (final Provider node : instances) {
                        heartbeatChecker.registerChecker(
                                () -> node.check(),
                                result -> heartbeatHandler.handle(
                                        result, this, node),
                                heartbeatCheckRateMs, heartbeatCheckTimeoutMs);
                    }
                    heartbeatCheckStarted = true;
                }
            }
        }
    }
    
    @Override
    public void close()
    {
        if (heartbeatChecker != null) {
            heartbeatChecker.close();
        }
    }
}
