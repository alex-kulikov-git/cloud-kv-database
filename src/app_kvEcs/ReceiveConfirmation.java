/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package app_kvEcs;

import common.messages.AdminMessage;
import common.reader.UniversalReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Adds functionality to the Command classes.
 * Receives confirmation message.
 * 
 * @author kajo
 */
public abstract class ReceiveConfirmation {
    /**
     * Sends command to server.
     * @param adminMessage - message to be send to the server
     * @param out - output stream to the server.
     * @throws IOException 
     */
    public void sendCommand(AdminMessage adminMessage, OutputStream out) throws IOException {
        byte[] sendThis = adminMessage.getByteMessage();      
        
        for(byte b : sendThis) {
                //System.out.println("sending a byte");
                out.write(b);
        }
        
        out.flush();
    }
    
    /**
     * receive confirmation message from given input stream
     * @param in the given input stream
     * @return success
     */    
    public boolean receiveConfirmation(InputStream in) {
        AdminMessage confMessage;
        byte[] incoming;
        
        try{
            UniversalReader reader = new UniversalReader();
            incoming = reader.readMessage(in);
            
        } catch(IOException | RuntimeException io_run) {
            // System.out.println("Error while receiving confirmation."); // for debugging
            return false;
        }
        
        confMessage = new AdminMessage(incoming);
        
        return confMessage.getValid(); // CHECK, IF CONFMESSAGE IS SUCCESSFUL 
    }
}
