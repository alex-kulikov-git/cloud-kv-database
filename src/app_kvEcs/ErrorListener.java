/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package app_kvEcs;


/**
 * This thing waits for incoming connections
 */
import common.logger.Constants;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ErrorListener extends Thread {
    private ExecutorService executor;
    private ServerSocket ecsServer;
    private ErrorBuffer buffer;
    private static final Logger LOGGER = LogManager.getLogger(Constants.SERVER_NAME);
    private boolean running;
    
    public ErrorListener(int port, ErrorBuffer buffer) {
        this.executor = Executors.newCachedThreadPool();
        this.buffer = buffer;
        try{
            ecsServer = new ServerSocket(port);
        } catch(IOException io) {
            io.printStackTrace();
        }
    }
       
    private ServerSocket getListener() {
        return this.ecsServer;
    }
    
    public void stopRunning() {
        running = false;
    }
    
    @Override
    public void run() {
        running = true;
        
        while(running) {
            try{ // passes ingoing connections to thread pool
                executor.execute(new AcknowledgeError(getListener().accept(), buffer));
            } catch(IOException io) {
                System.out.println("nothing...");
            }
        }   
    }
     
}
