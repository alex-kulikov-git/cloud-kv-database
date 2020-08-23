/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package app_kvServer;

import common.constants.PortOffset;
import common.logger.Constants;
import common.messages.MetaData;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import manager.CacheManager;
import manager.SubscriptionManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Has a server socket running all the time which waits for another server
 * to connect to send us some information. 
 * If a connection attempt is detected, a new Runnable (GossipHeartJulia) is 
 * started to process the communication. 
 */
public class PingListener extends Thread {
    private static final Logger LOGGER = LogManager.getLogger(Constants.SERVER_NAME);
    
    private final ExecutorService executor;
    private ServerSocket listener;
    private int gossipPort;
    private final CacheManager manager;
    private final SubscriptionManager subManager;
    
    /**
     * CONSTRUCTOR
     * @param mainServerPort the port of the server itself. we calculate the gossipPort based on it. 
     * @param metaData the meta data object of the current server
     * @param manager the cache manager of the current server
     * @param subManager the subscription manager of the current server
     */
    public PingListener(int mainServerPort, MetaData metaData, CacheManager manager, SubscriptionManager subManager) { 
        this.gossipPort = PortOffset.getGossipPort(mainServerPort);
        this.subManager = subManager;
        
        try {
            this.listener = new ServerSocket(gossipPort);            
            LOGGER.info("Launched Ping Listener on port: " + PortOffset.getGossipPort(mainServerPort));
        } 
        catch(IOException e) {
            LOGGER.error("Unable to create ServerSocket to receive connections in PingListener class");
        }
        
        this.executor = Executors.newCachedThreadPool();
        this.manager = manager;
    }
    
    /**
     * Gets called automatically by start() because this is a thread. 
     */
    @Override
    public void run() {
        while(true) {
            try{ 
                executor.execute(new GossipHeartJulia(listener.accept(), manager, subManager));
                
            } catch(IOException io) {
                LOGGER.error("Unable to receive incoming connection from a server in PingListener class");
                System.err.println("Unable to receive incoming connection a server in PingListener class");
            }
        }   
    }
    
}
