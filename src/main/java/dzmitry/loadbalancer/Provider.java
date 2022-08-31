package dzmitry.loadbalancer;

import java.util.UUID;

public class Provider
{
    private final String uuid;
    
    public Provider()
    {
        uuid = UUID.randomUUID().toString();
    }
    
    public String get()
    {
        return uuid;
    }
}
