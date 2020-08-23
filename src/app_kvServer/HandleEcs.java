/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package app_kvServer;

import common.constants.PortOffset;
import common.messages.AdminMessage;
import common.messages.MetaData;
import common.reader.UniversalReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import manager.CacheManager;

/**
 * Handles the communication between the server and the ecs on a seperate port.
 */
public class HandleEcs extends Thread {    
    private int port;
    private CacheManager manager;
    private AdminCommands adminCommands;
    
    public HandleEcs(CacheManager manager, int port, MetaData metaData) {
        this.port = PortOffset.getEcsPort(port); // ecs connections have their own port
        this.manager = manager;
        this.adminCommands = new AdminCommands(manager, metaData);
    }
    
    /**
     * Informs ecs that something went wrong.
     * @param out - outputstream
     * @throws IOException 
     */
    private void sendValid(OutputStream out) throws IOException {
        out.write((byte) 41);
        out.flush();
    }
    
    /**
     * Informs ecs that something went wrong.
     * @param out
     * @throws IOException 
     */
    private void sendInvalid(OutputStream out) throws IOException {
        out.write((byte) 42);
        out.flush();
    }
    
    @Override
    public void run() {
        ServerSocket ecsServer = null;
        try {
            ecsServer = new ServerSocket(port);
        } catch (IOException ex) {
            // what now?
            // server shutdown?
            manager.shutDown();
        }
        while(true) {
            try{
                Socket ecsServerClient = ecsServer.accept();            // waits for the ecs client to connect
                InputStream in = ecsServerClient.getInputStream();      // create input stream
                OutputStream out = ecsServerClient.getOutputStream();   // create output stream

                UniversalReader reader = new UniversalReader();
                byte[] incomingMinimal;
                incomingMinimal = reader.readMessage(in);

                AdminMessage adminMessage = new AdminMessage(incomingMinimal);

                if (!adminMessage.getValid())
                    System.out.println("message not valid! length: "+incomingMinimal.length);

                // if valid, execute the admin command and send back Confirmation
                if(adminMessage.getValid() && adminCommands.execute(adminMessage))
                    sendValid(out);
                else{
                    sendInvalid(out);
                }
                
                // what if closing all this stuff fails?
                in.close();
                out.close();
                ecsServerClient.close();

            } catch(IOException | NullPointerException e) {
                // System.out.println("Exception while handling ecs command");
                // how to handle?
            }

        }
    }
    
}
