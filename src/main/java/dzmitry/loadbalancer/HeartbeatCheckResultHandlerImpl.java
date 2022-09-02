package dzmitry.loadbalancer;

public class HeartbeatCheckResultHandlerImpl
        implements HeartbeatCheckResultHandler
{
    @Override
    public void handle(final boolean checkResult, final LoadBalancer balancer,
            final Provider node)
    {
        if (checkResult == false) {
            balancer.excludeNodes(node.getUuid());
        }
    }
}
