package dzmitry.loadbalancer;

public interface HeartbeatCheckResultHandler
{
    void handle(boolean checkResult, LoadBalancer balancer, Provider node);
}
