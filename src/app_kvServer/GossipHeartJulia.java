/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package app_kvServer;

import common.logger.Constants;
import common.messages.AdminMessage;
import common.messages.KVAdminMessage;
import common.messages.KVMessage;
import common.messages.Message;
import common.messages.StatusValidation;
import common.reader.UniversalReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import manager.CacheManager;
import manager.SubscriptionManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class waits for another server to establish a connection for either
 * heartbeating or moving data and subs due to hashing structure changes. 
 * 
 * When a message was received, we send a valid confirmation back that we 
 * received it. In case of a request to move data or subs we delegate the 
 * request to either the cache manager or the subscription manager of the 
 * current server. 
 */
public class GossipHeartJulia implements Runnable {
    private static final Logger LOGGER = LogManager.getLogger(Constants.SERVER_NAME);

    private final Socket socket;
    private final CacheManager manager;
    private final SubscriptionManager subManager;
    private String serverName;
    private int port;
    
    /**
     * CONSTRUCTOR
     * @param socket the socket where the connection from another server comes from
     * @param manager the cache manager of the current server
     * @param subManager the subscription manager of the current server
     */
    public GossipHeartJulia(Socket socket, CacheManager manager, SubscriptionManager subManager) {
        this.socket = socket;
        this.manager = manager;
        this.subManager = subManager;
        this.serverName = socket.getInetAddress().getHostAddress();
        this.port = socket.getLocalPort();
    }
    
    /**
     * Sends the given array of bytes to the given output stream. 
     * @param byteMessage the byte message to send
     * @param out the output stream to write to
     * @throws IOException in case the writing process throws an error
     */
    private void sendBytes(byte[] byteMessage, OutputStream out) throws IOException {
        for(byte b : byteMessage) {
            out.write(b);
        }        
        out.flush();
    }
    
    /**
     * Sends the given array of bytes to the given output stream. 
     * Attaches a carriage return in the end. 
     * @param byteMessage the byte message to send
     * @param out the output stream to write to
     * @throws IOException in case the writing process throws an error
     */
    private void sendBytesR(byte[] byteMessage, OutputStream out) throws IOException {
        for(byte b : byteMessage) {
            out.write(b);
        }  
        out.write((byte) 13);
        out.flush();
    }
    
    /**
     * Gets called automatically because this class is a Runnable. 
     * 
     * Manages the established connection to another server. Sends replies to
     * incoming connections. 
     * Delegates "move data" and "move subscriptions" operations to the 
     * responsible managers of the current server. 
     */
    @Override
    public void run() {
        OutputStream out = null; 
        InputStream in = null;
        UniversalReader ur = new UniversalReader();

        try {
            in = socket.getInputStream();
            out = socket.getOutputStream();
        } catch(IOException io) {
            LOGGER.error("could not open streams.");
        }
        
        try {
            sendBytesR((new Message(KVMessage.StatusType.PUT, serverName.getBytes(), Integer.toString(port).getBytes())).getByteMessage(), out);
        } catch (IOException ex) {
            LOGGER.error("could not send connection confirmation.");
        }

        while(true){     
            try {
                // checks, whether it received a replication message, a subscription message, or a ping
                // whatever we receive here, it always comes from a server and never from a client because of port separation
                byte[] raw = ur.readMessage(in);

                if(StatusValidation.validKVStatus(raw[0])) { // if replication message
                    Message message = new Message(raw);
                    Message reply = null;

                    if(message.getValid() && ( message.getStatus().equals(KVMessage.StatusType.PUT) || message.getStatus().equals(KVMessage.StatusType.DELETE ))) {
                        // handling put, delete, update, or sub
                        KVMessage.StatusType feedback;
                        if(message.getStatus().equals(KVMessage.StatusType.PUT))
                            feedback = manager.put(message.getKey(), message.getValue());
                        else
                            feedback = manager.put(message.getKey(), "null");
                            
                        if(feedback.equals(KVMessage.StatusType.PUT_SUCCESS) || 
                           feedback.equals(KVMessage.StatusType.PUT_ERROR) ||
                           feedback.equals(KVMessage.StatusType.PUT_UPDATE)) {
                            // handling a put or update
                            reply = new Message(feedback, message.getKeyAsBytes(), message.getValueAsBytes());

                        } else if(feedback.equals(KVMessage.StatusType.DELETE_SUCCESS) ||
                                  feedback.equals(KVMessage.StatusType.DELETE_ERROR)) {
                            // handling a delete
                            reply = new Message(feedback, message.getKeyAsBytes());
                        }
                    } else if(message.getValid() && message.getStatus().equals(KVMessage.StatusType.SUB)) {
                        // handling a subscription
			subManager.addSubscription(message.getKey(), message.getValue()); 
			reply = new Message(KVMessage.StatusType.SUB_SUCCESS);				
                    }
                    if(reply == null)
			LOGGER.error("could not create reply to received message - reply = null");
                    else {
			sendBytesR(reply.getByteMessage(), out);
                    }

                } else { // if ping message
                    AdminMessage message = new AdminMessage(raw);
                    byte[] replyBytes = new byte[1];

                    if(message.getValid() && (message.getStatus().equals(KVAdminMessage.AdminType.PING)))
                        replyBytes[0] = (byte) 41;
                    else 
                        replyBytes[0] = (byte) 42;

                    sendBytes(replyBytes, out);
                }
            } catch (IOException | RuntimeException ex) {
                LOGGER.info("Server disconnected");
                System.out.println("Server disconnected");
                break;
            }
        }
                 
        try{
            in.close();
            out.close();
            socket.close();
        }
        catch(IOException ioe){
            LOGGER.error("Unable to close streams or socket");
            System.err.println("Unable to close streams or socket");
        }
    }
}
