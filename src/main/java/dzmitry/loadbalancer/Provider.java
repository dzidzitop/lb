package dzmitry.loadbalancer;

import java.util.UUID;

public class Provider
{
    private final String uuid;
    
    public Provider()
    {
        uuid = UUID.randomUUID().toString();
    }
    
    public String getUuid()
    {
        return uuid;
    }
    
    public String get()
    {
        return uuid;
    }
    
    public boolean check()
    {
        return true;
    }
}
