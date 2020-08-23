/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package common.messages;

/**
 * Interface for the class AdminMessage
 */
public interface KVAdminMessage {
    public enum AdminType {
        /* commands 21 - 40 */
        META_DATA, 
        START, 
        STOP, 
        SHUT_DOWN, 
        LOCK_WRITE, 
        UNLOCK_WRITE, 
        MOVE_DATA,
        PING, 
        CRASH,
        REPLICATE_DATA,
        DELETE_DATA,
        
        /* confirmation/error 41 - 60 */
        RECEIVED_AND_EXECUTED, // received an adminMessage in valid format && executed it
        AN_ERROR_OCCURED, // received a message on the port, but did not have valid format
        SERVER_DOWN // server is not reachable
    }
    
    /**
     * @return the status 
     */
    public AdminType getStatus();
}
