/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package app_kvServer;

import common.constants.PortOffset;
import common.logger.Constants;
import common.messages.*;
import common.reader.UniversalReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import manager.CacheManager;

/**
 * Every X seconds this Thread sends a ping to this server's successors. 
 */
public class GossipHeartRomeo extends Thread {
    private static final Logger LOGGER = LogManager.getLogger(Constants.SERVER_NAME);
    
    private CacheManager manager;
    private MetaData metaData;
    private boolean running;
    private final String ip;
    private final int port;
    private final ErrorManager errorManager;
    
    /**
     * CONSTRUCTOR
     * @param metaData the current meta data object owned by the server
     * @param ip the ip of the current server whose part Romeo is
     * @param port the port of the current server
     */
    public GossipHeartRomeo(MetaData metaData, CacheManager manager, String ip, int port) { 
        this.metaData = metaData;
        this.manager = manager;
        this.port = port;        
        this.ip = ip;       
        this.errorManager = new ErrorManager();   
        
        LOGGER.info("started romeo on "+ip+":"+port);
    }
   
    /**
     * Sends a byte array to the given OutputStream.  
     * @param byteMessage
     * @throws IOException 
     */
    private void sendBytes(byte[] byteMessage, OutputStream out) throws IOException{
        for(byte b : byteMessage) {
                out.write(b);
        }        
        out.flush();
    } 
        
    /**
     * Sends an AdminMessage with a PING flag to a server. 
     * 
     * @param successorIP
     * @param successorPort
     * @throws IOException if unable to send ping message
     */
    private void sendPing(OutputStream out) throws IOException {
	LOGGER.info("sendPing");
        AdminMessage pingMsg = new AdminMessage(KVAdminMessage.AdminType.PING);        
        sendBytes(pingMsg.getByteMessage(), out);
    }
    
    /**
     * Listens on the input stream to receive confirmation after sending a message. 
     * 
     * If we read -1, we assume that the server is down. 
     * 
     * Otherwise, we check for the reply to be a valid message. 
     * If it's not, we receive an exception from UniversalReader and catch it. 
     * 
     * This method only does the listening and returns the reply. 
     * 
     * @param in the InputStream to listen on
     * @return the byte that was received from successor server as reply
     */
    private byte[] listen(InputStream in) throws IOException, RuntimeException { 
	LOGGER.info("listen");
        UniversalReader univReader = new UniversalReader();
        byte[] confirmation = null;
        
        confirmation = univReader.readMessage(in); // reads AdminMessage from the given inputStream
        
        return confirmation;
    }
    
    /**
     * Gets called automatically because it's a thread. 
     * 
     * Sends pings to the first and the second successor of the current server. 
     * Expects a correct reply. 
     * If unable to connect to one of the successors, 
     * or the reply received has wrong format, then we send and error message
     * informing the ECS that one of the servers is down. ECS handles the rest. 
     */
    @Override
    public void run() { 
        running = true;
        
        while(running){ // right now this is just a while(true) loop
            try{
                Thread.sleep(5000);
            } 
            catch(InterruptedException e){
                LOGGER.error("Unable to execute Thread.sleep()");
                System.err.println("Unable to execute Thread.sleep()");
            }
            
            if (metaData.isEmpty())
                continue;
            
            MetaDataEntry successor1 = metaData.getSuccessor(this.ip, this.port);
            MetaDataEntry successor2 = metaData.getSuccessor(successor1.getIP(), successor1.getPort());
            
            String firstIP = successor1.getIP();
            int firstPort_original = successor1.getPort();
            int firstPort = PortOffset.getGossipPort(successor1.getPort()); // gossip offset

            String secondIP = successor2.getIP();
            int secondPort_original = successor2.getPort();
            int secondPort = PortOffset.getGossipPort(successor2.getPort()); // gossip offset

            Socket socket = null;
            InputStream in = null;
            OutputStream out = null;

            // SEND PING MESSAGE TO THE FIRST SUCCESSOR
            // Open connection
            try{
                //LOGGER.info("Romeo opening socket to successor 1");
                socket = new Socket(firstIP, firstPort);
                in = socket.getInputStream();
                out = socket.getOutputStream();
                
                // send the ping
                //LOGGER.info("Romeo sends ping to successor 1");
                sendPing(out);          

                AdminMessage reply = new AdminMessage(listen(in));
                //LOGGER.info("Romeo received message from successor 1");

                if(! (reply.getValid())){
                    // reply not valid -> we assume that the server is down
                    LOGGER.error("Romeo sends server down (1)");
                    errorManager.sendServerDown(firstIP, firstPort_original);
                }
            }
            catch(IOException e){
                // IOException -> we assume that the server is down
                LOGGER.error("Romeo sends server down (2): "+firstIP+":"+firstPort_original);
                errorManager.sendServerDown(firstIP, firstPort_original);
            }     
            catch (RuntimeException re) {
                
            }

            try{
                in.close();
                out.close();
                socket.close();
            }
            catch(IOException ex){
                LOGGER.error("Unable to close streams or socket");
            }


            // SEND PING MESSAGE TO THE SECOND SUCCESSOR
            // Open connection
            try{
                socket = new Socket(secondIP, secondPort);
                in = socket.getInputStream();
                out = socket.getOutputStream();
                
                // send the ping
                sendPing(out);                                 

                AdminMessage reply = new AdminMessage(listen(in));

                if(! (reply.getValid())){
                    // reply not valid -> we assume that the server is down
                    LOGGER.error("Romeo sends server down (3): "+firstIP+":"+secondPort_original);
                    errorManager.sendServerDown(secondIP, secondPort_original);
                }
            }
            catch(IOException e){
                // IOException -> we assume that the server is down
                LOGGER.error("Romeo sends server down (4): "+firstIP+":"+secondPort_original);
                errorManager.sendServerDown(secondIP, secondPort_original);
            }            
            catch (RuntimeException er) {
                
            }

            try{
                in.close();
                out.close();
                socket.close();
            }
            catch(IOException ex){
                LOGGER.error("Unable to close streams or socket");
            }
        }
    }
    
    public boolean isRunning(){
        return this.running;
    }
    
    public void setRunning(boolean value){
        this.running = value;
    }
    
}
