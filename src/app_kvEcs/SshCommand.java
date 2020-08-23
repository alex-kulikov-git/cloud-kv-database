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
 * Runnable that executes an sh script and then waits for confirmation of the launched server.
 * @author kajo
 */
public class SshCommand extends ReceiveConfirmation implements Runnable {
    private String[] script;
    private AdminMessage adminMessage;
    private String ip;
    private int port;
    private boolean[] confirmationArray;
    private int myPos;
    
    
    public SshCommand(String[] script, AdminMessage adminMessage, String ip, int port, boolean[] confirmationArray, int myPos) {
        this.script = script;
        this.adminMessage = adminMessage;
        this.ip = ip;
        this.port = PortOffset.getEcsPort(port); // ecs connections have their own port
        this.confirmationArray = confirmationArray;
        this.myPos = myPos;
    }

    /* private ---------------------------------------------------------------*/
    /**
     * Runs script
     * @throws IOException 
     */
    private void runSsh() throws IOException {
        Process proc;
        Runtime run = Runtime.getRuntime(); 
        proc = run.exec(script); // TODO find out, if the process dies by itself
    }
    
    /* public ----------------------------------------------------------------*/
    @Override
    public void run() {
        Socket ecsClient = null;
        InputStream in = null;
        OutputStream out = null;
        
        try{
            runSsh();
            Thread.sleep(5000); // give the server  second to 
            
            ecsClient = new Socket(ip, port);
            in = ecsClient.getInputStream();
            out = ecsClient.getOutputStream();
            
            sendCommand(adminMessage, out);
            
            confirmationArray[myPos] = receiveConfirmation(in);
            
        } catch(IOException | InterruptedException io_rupt) {
            // what to do?
            io_rupt.printStackTrace();
            confirmationArray[myPos] = false;
            // System.out.println("Error in SshCommand."); // for debugging
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
