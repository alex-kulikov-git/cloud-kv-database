/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package app_kvEcs;

import common.constants.PortOffset;
import common.messages.AdminMessage;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * Runnable that sends command to server and waits for confirmation.
 */
public class SendingCommand extends ReceiveConfirmation implements Runnable {
    private AdminMessage adminMessage;
    private String ip;
    private int port;
    private boolean[] confirmationArray;
    private int myPos;    
    
    /**
     * CONSTRUCTOR
     * 
     * @param adminMessage
     * @param ip
     * @param port
     * @param confirmationArray
     * @param myPos 
     */
    public SendingCommand(AdminMessage adminMessage, String ip, int port, boolean[] confirmationArray, int myPos) {
        this.adminMessage = adminMessage;
        this.ip = ip;
        this.port = PortOffset.getEcsPort(port); // ecs connections have their own port
        this.confirmationArray = confirmationArray;
        this.myPos = myPos;
    }
    
    
    /* public ----------------------------------------------------------------*/
    @Override
    public void run() {
        Socket ecsClient = null;
        InputStream in = null;
        OutputStream out = null;        
        
        try{
            //send command to server
            ecsClient = new Socket(ip, port);
            in = ecsClient.getInputStream();
            out = ecsClient.getOutputStream();
            
            try {
                Thread.sleep(1000);
            }
            catch (Exception e ) {
                
            }
            
            sendCommand(adminMessage, out);
            
            //receive reply from the server
            confirmationArray[myPos] = receiveConfirmation(in);
            
        } catch(IOException io) {
            // what to do?
            io.printStackTrace();
            confirmationArray[myPos] = false;
            // System.out.println("Error in SendingCommand. "); // for debugging
            io.printStackTrace();
        }
        
        
        try{
            // Thread.sleep(2000);
            in.close();
            out.close();
            ecsClient.close();
        } catch(Exception e) {
            // just try
        }
        
        
        // thread should die here
    }
    
    
    
}
