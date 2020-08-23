/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package app_kvEcs;

import common.logger.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
/**
 * Reads Error from Buffer, deletes Error - then, if the server is still in the list of available servers, 
 * calls method in CommandManager
 */
public class HandleError extends Thread {
    private ErrorBuffer buffer;
    private CommandManager commander;
    private static final Logger LOGGER = LogManager.getLogger(Constants.SERVER_NAME);
    private boolean running;
    
    public HandleError(CommandManager commander, ErrorBuffer buffer) {
        this.buffer = buffer;
        this.commander = commander;
    }
    
    public void stopRunning() {
        running = false;
    }
    
    @Override
    public void run() {
        running = true;
        
        while(running) {
            buffer.lock(); // Aquire Lock on Buffer
            
            if(!buffer.isEmpty()) { // Is there something in the Buffer?
                
                String ip = buffer.getFirstIp();
                int port =  buffer.getFirstPort();
                //LOGGER.debug("HandleError: buffer is not empty, first is "+ip+":"+port);
                
                commander.lock();
                try {
                    commander.removeCrashedServer(ip, port); // commandManager needs crashed list
                }
                finally {
                    commander.unlock();
                }
                
                buffer.remove(ip, port); // remove all elements with ip and port out of Q
            }
  
            buffer.unlock(); // unlock Buffer
                
            try{
                Thread.sleep(3000); // sleep for 10 sec - less for testing
            } catch (InterruptedException ex) {
                // well?
            }
        }
                    
    }
}
