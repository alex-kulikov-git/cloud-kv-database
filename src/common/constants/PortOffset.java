/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package common.constants;

public interface PortOffset {
    
    final int ECS_OFFSET = 100;
    final int GOSSIP_OFFSET = 200;
    
    public static int getEcsPort(int port) {
        int ecsPort = port + ECS_OFFSET;
        if(ecsPort > 65535)
            return ecsPort - 65535 + 1023;
        return ecsPort;
    }
    
    public static int getGossipPort(int port) {
        int gossipPort = port + GOSSIP_OFFSET;
        if(gossipPort > 65535)
            return gossipPort - 65535 + 1023;
        return gossipPort;
    }
}
