package dzmitry.loadbalancer;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class HeartbeatCheckResultHandlerImpl
        implements HeartbeatCheckResultHandler
{
    private static final int MAX_RELEVANT_SUCCESSFUL_CHECKS = 2;
    
    /* Used to store individual instances regardless of whether
     * or not they are equal. In addition, this handler synchronises
     * on this wrapper to ensure sequential consistency or
     * processing of multiple check results that can come for
     * the same item.
     */
    private static class NodeWrapper
    {
        private final Provider node;
        private int lastSuccessfulChecks; // <= MAX_RELEVANT_SUCCESSFUL_CHECKS
        
        public NodeWrapper(final Provider node)
        {
            this.node = node;
        }
        
        @Override
        public boolean equals(final Object o)
        {
            if (o == null || o.getClass() != NodeWrapper.class) {
                return false;
            }
            return node == ((NodeWrapper) o).node;
        }
        
        @Override
        public int hashCode()
        {
            return System.identityHashCode(node);
        }
    }
    
    private static ConcurrentHashMap<NodeWrapper, NodeWrapper> nodes =
            new ConcurrentHashMap<>();
    
    @Override
    public void handle(final boolean checkResult, final LoadBalancer balancer,
            final Provider node)
    {
        Objects.requireNonNull(node);
        /* Reusing the existing wrapper or creating a new one
         * if there is no one already.
         */
        final NodeWrapper key = new NodeWrapper(node);
        final NodeWrapper wrapper = nodes.computeIfAbsent(
                key, val -> val == null ? key : val);
        synchronized (wrapper) {
            /* Not trying to guess the current state of the node before
             * activating/deactivating it. The load balancer should just
             * ensure it is in the needed state. The result of surrounding
             * checks can expire immediately and lead to intermittent
             * state inconsistencies between this heartbeat checker and
             * the load balancer.
             */
            if (checkResult == false) {
                wrapper.lastSuccessfulChecks = 0;
                balancer.excludeNode(node.getUuid());
            } else {
                int lastSuccChecks = wrapper.lastSuccessfulChecks;
                if (lastSuccChecks < MAX_RELEVANT_SUCCESSFUL_CHECKS) {
                    ++lastSuccChecks;
                    // Issuing write only when data is really changed.
                    wrapper.lastSuccessfulChecks = lastSuccChecks;
                    
                    if (lastSuccChecks == MAX_RELEVANT_SUCCESSFUL_CHECKS) {
                        balancer.includeNode(node.getUuid());
                    }
                }
            }
        }
    }
}
