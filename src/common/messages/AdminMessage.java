/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package common.messages;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * New Message format for ecs interaction.
 * @author kajo
 */
public class AdminMessage implements KVAdminMessage {
    private byte[] byteMessage;
    private boolean valid;
    
    /*
    
    Available Flags:
    META_DATA
    START
    STOP
    SHUT_DOWN
    LOCK_WRITE
    UNLOCK_WRITE
    MOVE_DATA
    PING
    CRASH
    (AUTH)
    
    RECEIVED_AND_EXECUTED
    AN_ERROR_OCCURED
    SERVER_DOWN 
    
    Message format short:   status
    Message format long:    status | length_b1 | length_b2 | length_b3 | length_b4 | payload
    
    */
    
    /**
     * CONSTRUCTOR
     * created an admin message that only consists of a status byte
     * 
     * @param status
     */
    public AdminMessage(AdminType status){
        this.valid = validStatus(status);
        this.byteMessage = createByteMessage(status);
    }
    
    
    /**
     * CONSTRUCTOR
     * creates an admin message with a given status byte and payload
     * 
     * @param status
     * @param payload
     */    
    public AdminMessage(AdminType status, byte[] payload) {
        this.valid = validStatus(status);
        this.byteMessage = createByteMessage(status, payload);
    }
    
    
    /**
     * CONSTRUCTOR
     * creates an admin message from a given byte array
     * 
     * @param byteMessage
     */
    public AdminMessage(byte[] byteMessage){
        this.valid = true;
        this.byteMessage = byteMessage;
        if(byteMessage.length == 1) {
            this.valid &= validStatus(decodeStatus(byteMessage[0]));
            if(valid && !messageIsShort(decodeStatus(byteMessage[0]))) // valid status, but message is not of type short
                this.valid &= false;
        } else {
            this.valid &= validStatus(decodeStatus(byteMessage[0]));
            if(valid && !messageIsShort(decodeStatus(byteMessage[0]))) { // valid status and message is is of type long --> decode
                this.valid &= mathIsFine(byteMessage);
            } else {
                this.valid &= false;
            }
        }       
    }    
    
    
    /**
     * Answers according to an already set status flag
     * 
     * @return no meta data - true; with payload - false
     */
    private boolean messageIsShort(AdminType status) {  // only call, if status is initialized                                           
        switch(status) {
            case START: 
            case STOP: 
            case SHUT_DOWN: 
            case LOCK_WRITE: 
            case UNLOCK_WRITE: 
            case PING: 
            case CRASH:
                
            case RECEIVED_AND_EXECUTED:
            case AN_ERROR_OCCURED:
                return true;
            
            default: 
                return false; // hence, META_DATA and MOVE_DATA also delivers false
        }
    }
    
    // maybe split into validConfirmation and validCommand
    private boolean validStatus(AdminType status) {
        switch(status) {
            case META_DATA:  
            case START: 
            case STOP: 
            case SHUT_DOWN:  
            case LOCK_WRITE:  
            case UNLOCK_WRITE:  
            case MOVE_DATA: 
            case PING: 
            case CRASH:
            case REPLICATE_DATA:
            case DELETE_DATA:
            
            case RECEIVED_AND_EXECUTED:
            case AN_ERROR_OCCURED:
            case SERVER_DOWN:
                return true;
            default: 
                return false;
        }
    }
    
    private boolean mathIsFine(byte[] byteMessage) {
        if(byteMessage.length < 5) // status | 4b length | payload
            return false;
        byte[] payloadLength = new byte[4];
        System.arraycopy(byteMessage, 1, payloadLength, 0, 4);
        return (byteMessage.length - 1 - 4 - ByteBuffer.wrap(payloadLength).getInt()) == 0;
    }
    
    /**
     * 
     * @param statusByte
     * @return 
     */
    private AdminType decodeStatus(byte statusByte) {
        switch((int) statusByte) {
            case 21: return AdminType.START;
            case 22: return AdminType.STOP;
            case 23: return AdminType.SHUT_DOWN;
            case 24: return AdminType.META_DATA;
            case 25: return AdminType.LOCK_WRITE;
            case 26: return AdminType.UNLOCK_WRITE;
            case 27: return AdminType.MOVE_DATA;
            case 28: return AdminType.PING;
            case 29: return AdminType.CRASH;
            case 30: return AdminType.REPLICATE_DATA;
            case 31: return AdminType.DELETE_DATA;
            
            case 41: return AdminType.RECEIVED_AND_EXECUTED;
            case 42: return AdminType.AN_ERROR_OCCURED;
            case 43: return AdminType.SERVER_DOWN;
            default: 
                this.valid = false;
                return null;
        }
    }
    
    /**
     * Parsing status to status byte.
     * @param status - given status
     * @return status byte
     */
    private byte statusToByte(AdminType status) {
        switch(status) {
            case START: return (byte) 21;
            case STOP: return (byte) 22;
            case SHUT_DOWN: return (byte) 23;
            case META_DATA: return (byte) 24;
            case LOCK_WRITE: return (byte) 25;
            case UNLOCK_WRITE: return (byte) 26; 
            case MOVE_DATA: return (byte) 27;
            case PING: return (byte) 28;
            case CRASH: return (byte) 29;
            case REPLICATE_DATA: return (byte) 30;
            case DELETE_DATA: return (byte) 31;
            
            case RECEIVED_AND_EXECUTED: return (byte) 41;
            case AN_ERROR_OCCURED: return (byte) 42;
            case SERVER_DOWN: return (byte) 43;
            
            default: throw new RuntimeException("status byte not valid"); // programmers fault
        }
    }
    
    private byte[] createByteMessage(AdminType status) {
        byteMessage = new byte[1];
        byteMessage[0] = statusToByte(status);
        return byteMessage;
    }
    
    private byte[] createByteMessage(AdminType status, byte[] payload) {
        byte statusByte = statusToByte(status);
        byte[] lengthBytes;
        
        lengthBytes = ByteBuffer.allocate(4).putInt(payload.length).array();
        
        byte[] byteMessage = new byte[1 + 4 + payload.length];
        byteMessage[0] = statusByte;
        System.arraycopy(lengthBytes, 0, byteMessage, 1, 4);
        System.arraycopy(payload, 0, byteMessage, 5, payload.length);
        
        return byteMessage;
    }
       
    
    private byte[] extractPayload(byte[] byteMessage) {
        if(byteMessage.length < 6) { // minimum of 6 bytes to have a payload.
            this.valid = false;
            return null;
        }
        
        byte[] lengthBytes = new byte[4];
        byte[] payloadBytes = new byte[byteMessage.length - 5];
        
        System.arraycopy(byteMessage, 1, lengthBytes, 0, 4);
        System.arraycopy(byteMessage, 5, payloadBytes, 0, byteMessage.length - 5);
        
        int length = java.nio.ByteBuffer.wrap(lengthBytes).getInt(); // BigEndian
        
        if(length != payloadBytes.length) {
            this.valid = false;
            return null;
        }
        
        return payloadBytes;
    }
    
 
    public AdminType getStatus(){
        return decodeStatus(byteMessage[0]);
    }
    
    public boolean getValid(){
        return this.valid;
    }
    
    public byte[] getByteMessage() {
        return this.byteMessage;
    }
    
    public byte[] getPayload() {
        return extractPayload(byteMessage);
    }
}
