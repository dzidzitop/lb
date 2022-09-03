package dzmitry.loadbalancer;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class HeartbeatCheckResultHandlerImplTest
{
    @Test
    public void testSimpleLifecycle()
    {
        final LoadBalancer balancer = Mockito.mock(LoadBalancer.class);
        final Provider node = Mockito.mock(Provider.class);
        Mockito.when(node.getUuid()).thenReturn("p1");
        
        final HeartbeatCheckResultHandlerImpl handler =
                new HeartbeatCheckResultHandlerImpl();
        
        Mockito.verifyNoInteractions(balancer);
        
        handler.handle(true, balancer, node);
        
        Mockito.verifyNoInteractions(balancer);
        
        handler.handle(true, balancer, node);
        
        Mockito.verify(balancer).includeNode("p1");
        Mockito.verifyNoMoreInteractions(balancer);
        
        handler.handle(true, balancer, node);
        
        Mockito.verify(balancer, Mockito.times(2)).includeNode("p1");
        Mockito.verifyNoMoreInteractions(balancer);
        
        handler.handle(false, balancer, node);
        
        Mockito.verify(balancer).excludeNode("p1");
        Mockito.verifyNoMoreInteractions(balancer);
        
        handler.handle(true, balancer, node);
        
        Mockito.verifyNoMoreInteractions(balancer);
        
        handler.handle(false, balancer, node);
        
        Mockito.verify(balancer, Mockito.times(2)).excludeNode("p1");
        Mockito.verifyNoMoreInteractions(balancer);
        
        handler.handle(true, balancer, node);
        
        Mockito.verifyNoMoreInteractions(balancer);
        
        handler.handle(true, balancer, node);
        
        Mockito.verify(balancer, Mockito.times(3)).includeNode("p1");
        Mockito.verifyNoMoreInteractions(balancer);
    }
}
