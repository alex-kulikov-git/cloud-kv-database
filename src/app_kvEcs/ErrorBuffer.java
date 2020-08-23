/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package app_kvEcs;

import common.logger.Constants;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Represents Error Buffer
 */
public class ErrorBuffer {
    // Lock
    private ReentrantLock lock;
    // List of ErrorMessages
    private ArrayList<BufferEntry> errorMessages;
    private static final Logger LOGGER = LogManager.getLogger(Constants.SERVER_NAME);
    
    public ErrorBuffer() {
        this.errorMessages = new ArrayList<BufferEntry>();
        this.lock = new ReentrantLock();
    }
    
    /**
     * Add an entry to the buffer in byte format
     * @param byteMessage the message to be added
     */
    public void add(byte[] byteMessage) {
        errorMessages.add(new BufferEntry(byteMessage));
	LOGGER.info("added error message" + errorMessages.get(errorMessages.size() - 1).port);
    }
    
    /**
     * remove a server from the error buffer
     * @param ip IP of the server
     * @param port Port of the server
     */
    public void remove(String ip, int port) { 
        for(int i = 0; i < errorMessages.size(); i++) {
            BufferEntry inQuestion = errorMessages.get(i);
            if(inQuestion.ip.equals(ip) && inQuestion.port == port)
            errorMessages.remove(inQuestion);
        }
    }
    
    /**
     * 
     * @return the IP of the first server in the buffer
     */
    public String getFirstIp() {
        return errorMessages.get(0).ip;
    }
    
    /**
     *
     * @return the Port of the first server in the buffer
     */
    public int getFirstPort() {
        return errorMessages.get(0).port;
    }
    
    /**
     * lock the buffer
     */
    public void lock() {
        lock.lock();
    }
    
    /**
     * unlock the buffer
     */
    public void unlock() {
        lock.unlock();
    }
    
    /**
     *
     * @return if the buffer is empty
     */
    public boolean isEmpty() {
        return errorMessages.size() == 0;
    }
    
    private class BufferEntry {
        String ip;
        int port;

        public BufferEntry(byte[] payload) {
            /*
            payload:
            4 byte ip | 4 byte port
            ip_b1 | ip_b2 | ip_b3 | ip_b4 | p_b1 | p_b2 | p_b3 | p_b4
            */
            
            byte[] byteIp = new byte[4];
            byte[] bytePort = new byte[4];
            
            System.arraycopy(payload, 0, byteIp, 0, byteIp.length);
            System.arraycopy(payload, byteIp.length, bytePort, 0, bytePort.length);
            
            this.ip = byteIp[0] + "." + byteIp[1] + "." + byteIp[2] + "." + byteIp[3];
            this.port = ByteBuffer.wrap(bytePort).getInt();
        }
        
    }
    
}
