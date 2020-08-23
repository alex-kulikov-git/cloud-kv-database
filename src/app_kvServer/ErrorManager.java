/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package app_kvServer;

import common.constants.EcsErrorAddress;
import common.logger.Constants;
import common.messages.AdminMessage;
import common.messages.KVAdminMessage;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ErrorManager {
    private static final Logger LOGGER = LogManager.getLogger(Constants.SERVER_NAME);
    
    /**
     * Sends a server failure report in form of an AdminMessage to the ecs 
     * error ip and port.  
     * The message includes the flag SERVER_DOWN and the ip and port of the 
     * server that is down. 
     * @param downIP the ip of the server that is down
     * @param downPort the port of the server that is down
     */
    public void sendServerDown(String downIP, int downPort){
        // create error message
        byte[] byteIP = ipToBytes(downIP);
        byte[] bytePort = ByteBuffer.allocate(4).putInt(downPort).array();
        byte[] payload = new byte[byteIP.length + bytePort.length];
        
        System.arraycopy(byteIP, 0, payload, 0, byteIP.length);
        System.arraycopy(bytePort, 0, payload, byteIP.length, bytePort.length);
        
        AdminMessage errorMessage  = new AdminMessage(KVAdminMessage.AdminType.SERVER_DOWN, payload); 
        
        // establish connection to the ecs and send message
        Socket socket = null;
        OutputStream out = null;
        
        try{
            socket = new Socket(EcsErrorAddress.IP, EcsErrorAddress.PORT);
            out = socket.getOutputStream();        
            sendBytes(errorMessage.getByteMessage(), out);
        }
        catch(IOException ioe){
            LOGGER.error("Unable to send \"SERVER_DOWN\" message to ecs");
            System.err.println("Unable to send \"SERVER_DOWN\" message to ecs"); // for testing purposes
        }
        
        try{
            out.close();
            socket.close();
        }
        catch(IOException ioe){
            LOGGER.error("Unable to close output stream or socket");
        }
    }
    
    private byte[] ipToBytes(String ip) {
        byte[] byteIP = new byte[4];
        String[] partials = ip.split("\\.");
        
        for(int i = 0; i < 4; i++) 
            byteIP[i] = Byte.parseByte(partials[i]);
        
        return byteIP;
    }
    
    /**
     * Sends a byte array to the given OutputStream. 
     * 
     * @param byteMessage the bytes of the message to send
     * @throws IOException in case unable to write to output stream
     */
    private void sendBytes(byte[] byteMessage, OutputStream out) throws IOException{
        for(byte b : byteMessage) {
                out.write(b);
        }        
        out.flush();
    } 
}
