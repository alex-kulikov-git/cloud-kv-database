/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package app_kvServer;

import common.logger.Constants;
import common.messages.AdminMessage;
import common.messages.MetaData;
import common.messages.MetaDataEntry;
import manager.CacheManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Executes commands that were received from the ecs. 
 */
public class AdminCommands {    
    private final CacheManager manager;
    private MetaData metaData;
    private final Logger logger = LogManager.getLogger(Constants.SERVER_NAME);
    private ErrorManager errorManager;
    
    public AdminCommands(CacheManager manager, MetaData metaData) {
        this.manager = manager;
        this.metaData = metaData;
    }
    
    
    /**
     * Starts the KVServer, all client requests and all ECS requests are
     * processed.
     * @return 
     */
    public boolean start() {
        manager.start();
        logger.info("started server.");
        return true;
    }
    
    /**
     * Stops the KVServer, all client requests are rejected and only
     * ECS requests are processed.
     * @return 
     */
    public boolean stop() {
        manager.stop();
        logger.info("stopped server");
        return true;
    }
    
    /**
     * Lock the KVServer for write operations.
     * @return 
     */
    public boolean lockWrite() {
        manager.lockWrite();
        logger.info("locked server");
        return true;
    }
    
    /**
     * Unlock the KVServer for write operations.
     * @return 
     */
    public boolean unLockWrite() {
        manager.unLockWrite();
        logger.info("unlocked server");
        return true;
    }
    
    /**
     * Exits the KVServer application.
     * @return 
     */
    public boolean shutDown() {
        lockWrite();
        stop();
        logger.warn("shutting down server");
        manager.shutDown();
        return true;
    }
    
    
    /**
     * Transfer a subset (range) of the KVServer’s data to another
     * KVServer (reallocation before removing this server or adding a
     * new KVServer to the ring); send a notification to the ECS, if data
     * transfer is completed. Delete data after the transfer.
     * @param payload
     * @return whether it was possible to connect to the requested server
     */
    public boolean moveData(byte[] payload) {
        MetaDataEntry entry = new MetaDataEntry(payload);
        boolean result = manager.moveData(entry.getRange(), entry.getIP(), entry.getPort(), true); 
        if(!result){
            if(errorManager == null) 
                errorManager = new ErrorManager();
            errorManager.sendServerDown(entry.getIP(), entry.getPort());            
        }
        return true; // if we send an error message to the ecs anyway, it doesnt neeed to receive another error from HandleEcs
    }
    
    /**
     * Transfer a subset (range) of the KVServer’s data to another
     * KVServer (reallocation before removing this server or adding a
     * new KVServer to the ring); send a notification to the ECS, if data
     * transfer is completed. Do not delete data after the transfer.
     * @param payload
     * @return whether it was possible to connect to the requested server
     */
    public boolean replicateData(byte[] payload) {
        MetaDataEntry entry = new MetaDataEntry(payload);
        boolean result = manager.moveData(entry.getRange(), entry.getIP(), entry.getPort(), false); 
        if(!result){
            if(errorManager == null) 
                errorManager = new ErrorManager();
            errorManager.sendServerDown(entry.getIP(), entry.getPort());            
        }
        return true; // if we send an error message to the ecs anyway, it doesnt neeed to receive another error from HandleEcs
    } 
    
    /**
     * Delete all KV-tuples in a given range
     * @param payload
     * @return whether or not the operation was successful
     */
    public boolean deleteData(byte[] payload) {
        MetaDataEntry entry = new MetaDataEntry(payload);
        return manager.deleteData(entry.getRange()); 
    }    
    
    /**
     * Update the meta-data repository of this server
     * @param metadata 
     * @return  
     */
    public boolean update(byte[] metadata) {
        System.out.println("extracting meta data of size "+metadata.length);
        this.metaData.extractMetadata(metadata);
        System.out.println("data: " + this.metaData.getFirst().getIP() + this.metaData.getFirst().getPort());
        logger.warn("updating meta data");
        return true;
    }
    
    /**
     * Crash the server with System.exit() for testing purposes
     */
    public boolean crash(){
        logger.warn("SERVER CRASHING!");
        manager.shutDown();
        return true;
    }
    
    
    public boolean execute(AdminMessage adminMessage) {
        System.out.println("executing admin command: "+adminMessage.getStatus());
        switch(adminMessage.getStatus()) {
            case META_DATA: return update(adminMessage.getPayload());
            case START: return start();
            case STOP: return stop();
            case SHUT_DOWN: return shutDown();
            case LOCK_WRITE: return lockWrite();
            case UNLOCK_WRITE: return unLockWrite();
            case MOVE_DATA: return moveData(adminMessage.getPayload());            
            case CRASH: return crash(); // in fact, nothing will be returned here, as the server exits (crashes)
            case REPLICATE_DATA: return replicateData(adminMessage.getPayload());
            case DELETE_DATA: return deleteData(adminMessage.getPayload());
            case PING: return true;
            default: 
                throw new RuntimeException("You should have checked, if the message was valid."); // programmers fault
        }
    }   
}
