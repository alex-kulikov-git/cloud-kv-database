package client;

import common.hashing.Hashing;
import common.messages.KVMessage;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import common.logger.Constants;
import common.messages.KVMessage.StatusType;

import common.messages.Message;
import common.messages.MetaData;
import common.messages.MetaDataEntry;

/**
 * This class implements the interface KVCommInterface. We tried to keep it as 
 * compact as possible. It contains a "library" with methods for any sort of 
 * communication between the client and the server. 
 */
public class KVStore implements KVCommInterface{

    // these are the ip and the port of the server that we are currrently connected to
    private String address; 
    private int port;
    private String email; 
    private byte[] password;
    
    private static final Logger logger = LogManager.getLogger(Constants.CLIENT_NAME);
    private Set<ClientSocketListener> listeners;    
    private Socket clientSocket;
    public KVConnection connection;
    
    private MetaData metaData;

    /**
     * Initialize KVStore with address and port of KVServer, save authentication data. 
     *
     * @param address  the address of the KVServer
     * @param port     the port of the KVServer
     * @param email    the email address of the client to authenticate on server side
     * @param password the password of the client to authenticate on server side
     */
    public KVStore(String address, int port, String email, String password) {
	this.address = address; 
        this.port = port;
        this.email = email;
        // save password right away as hash value
        this.password = Hashing.getHashValue(password); 
        
        metaData = new MetaData();
        metaData.insertServer(address, port);
    }
    
    /**
     * OLD CONSTRUCTOR => ADJUST TESTS AND THEN DELETE?
     */
    public KVStore(String address, int port) {
	this.address = address; 
        this.port = port;
        
        metaData = new MetaData();
        metaData.insertServer(address, port);
    }
    
    /**
     * Initializes and starts the client connection, and authenticates the user.
     * The connection itself is being managed by the class KVConnection. 
     * @return result of trying to connect (mostly server reply) as KVMessage
     * @throws java.net.UnknownHostException
     * @throws IOException
    */
    @Override
    public KVMessage connect() throws UnknownHostException, IOException {
        clientSocket = new Socket(address, port); // this is where the listed exceptions can emerge

        listeners = new HashSet<>(); 
        
        connection = new KVConnection(clientSocket, listeners);
        connection.start();      
        connection.setLatestMessage(null); // needed to receive correct server replies to the operations put and get
        
        // receive reply from server
        while(connection.getLatestMessage() == null) {
            try{
                Thread.sleep(100);
            }
            catch(InterruptedException e){
                logger.info("The client thread was interrupted. ");
            }
        }
        
        KVMessage latestMsg = new Message(connection.getLatestMessage().getStatus(), connection.getLatestMessage().getKeyAsBytes(), connection.getLatestMessage().getValueAsBytes());
        connection.setLatestMessage(null);
        
        // if connection established, authenticate with email and password
        if(latestMsg.getStatus().equals(StatusType.PUT) && latestMsg.getKey().equals(address) && Integer.parseInt(latestMsg.getValue()) == port){
            latestMsg = authenticate(); 
        }
        
        return latestMsg;
    }
    
    /**
     * Initializes and starts the connection WITHOUT authentification.
     * This must only be used for server-to-server connections using a KVStore object.
     * @return result of trying to connect (mostly server reply) as KVMessage
     * @throws java.net.UnknownHostException
     * @throws IOException
    */    
    public KVMessage connectServer() throws UnknownHostException, IOException {
        clientSocket = new Socket(address, port); // this is where the listed exceptions can emerge

        listeners = new HashSet<>(); 
        
        connection = new KVConnection(clientSocket, listeners);
        connection.start();      
        connection.setLatestMessage(null); // needed to receive correct server replies to the operations put and get
        
        // receive reply from server
        while(connection.getLatestMessage() == null) {
            try{
                Thread.sleep(100);
            }
            catch(InterruptedException e){
                logger.info("The client thread was interrupted. ");
            }
        }
        
        KVMessage latestMsg = new Message(connection.getLatestMessage().getStatus(), connection.getLatestMessage().getKeyAsBytes(), connection.getLatestMessage().getValueAsBytes());
        connection.setLatestMessage(null);
        
        return latestMsg;
    }
    
    /**
     * Sends an authentication request to the server that we are currently connected to. 
     * @return reply from the server as KVMessage
     */
    private KVMessage authenticate(){
        // create authentication Message
        byte[] emailBytes = email.getBytes(); // both email and password are saved in the value field of Message 
        byte[] payloadBytes = new byte[emailBytes.length + password.length]; // password/hash length should automatically be 16 bytes long
        System.arraycopy(emailBytes, 0, payloadBytes, 0, emailBytes.length);
        System.arraycopy(password, 0, payloadBytes, emailBytes.length, password.length);
        
        byte[] msgBytes = new Message(StatusType.AUTH, "_".getBytes(), payloadBytes).getByteMessage();
        
        // send authentication Message
        KVMessage latestMsg = sendAndReply(msgBytes);
        
        return latestMsg;  // we receive AUTH_SUCCESS here if everything goes right
    }
    
    /**
     * First checks with a get request, whether the requested key exists in the database. 
     * If it does, sends a subscription request to the server. 
     * @param key the key to subscribe to
     * @return result of the subscription request
     */
    public KVMessage subscribe(String key, String email){
        // check with a get request whether the key exists in the database
        byte[] msgBytes = new Message(StatusType.GET, key.getBytes()).getByteMessage();
        KVMessage latestMsg = sendAndReply(msgBytes);
        
        if(latestMsg.getStatus().equals(StatusType.GET_SUCCESS) && latestMsg.getKey().equals(key) && latestMsg.getValue() != null){
            // create subscription Message
            msgBytes = new Message(StatusType.SUB, key.getBytes(), email.getBytes()).getByteMessage(); // now also sends the email address as value

            // check if we need to send to a different server, because we only can subscribe on servers within writing range of the key
            MetaDataEntry entry = metaData.getServer(key);
            if (!entry.getIP().equals(this.address) || entry.getPort() != this.port) {           
                KVMessage temp = newConnection(entry.getIP(), entry.getPort());    
                if (temp == null) {
                    // reconnect failed -> return error
                    return new Message(StatusType.SUB_ERROR); 
                }
            }   
            
            // send subscription Message
            latestMsg = sendAndReply(msgBytes);
            
            // if server is not responsible, establish a new connection and renew the request
            if((latestMsg.getStatus() == StatusType.NOT_RESPONSIBLE) && latestMsg.getKey().equals("meta")){
                byte[] latestValue = latestMsg.getValueAsBytes();
                updateMetaData(latestValue);
                entry = metaData.getServer(key);
                KVMessage temp = newConnection(entry.getIP(), entry.getPort()); // Tear down the existing connection and create a new one with the updated meta data
                if(temp == null){
                    return latestMsg; // If new connection could not be established, return the initial message with the flag NOT_RESPONSIBLE
                }
                else return subscribe(key, email); // Trying to repeat the query after a new connection has been established
            }
        }
        else{
            // the key does not exist in the database yet => return error
            return new Message(StatusType.SUB_ERROR);
        }
        
        return latestMsg;  // Flag is SUB_SUCCESS or SUB_ERROR
    }
    
    /**
     * First checks with a get request, whether the requested key exists in the database. 
     * If it does, sends an unsub request to the server. 
     * @param key the key to unsub from
     * @return the result of the request
     */
    public KVMessage unsubscribe(String key){
        // check with a get request whether the key exists in the database
        byte[] msgBytes = new Message(StatusType.GET, key.getBytes()).getByteMessage(); 
        KVMessage latestMsg = sendAndReply(msgBytes);
        
        if(latestMsg.getStatus().equals(StatusType.GET_SUCCESS) && latestMsg.getKey().equals(key) && latestMsg.getValue() != null){
            // create unsubscribe Message
            msgBytes = new Message(StatusType.UNSUB, key.getBytes()).getByteMessage();

            // check if we need to send to a different server, because we only can subscribe on servers within writing range of the key
            MetaDataEntry entry = metaData.getServer(key);
            if (!entry.getIP().equals(this.address) || entry.getPort() != this.port) {           
                KVMessage temp = newConnection(entry.getIP(), entry.getPort());    
                if (temp == null) {
                    // reconnect failed -> return error
                    return new Message(StatusType.SUB_ERROR); 
                }
            }   
            
            // send unsub Message
            latestMsg = sendAndReply(msgBytes);
            
            // if server is not responsible, establish a new connection and renew the request
            if((latestMsg.getStatus() == StatusType.NOT_RESPONSIBLE) && latestMsg.getKey().equals("meta")){
                byte[] latestValue = latestMsg.getValueAsBytes();
                updateMetaData(latestValue);
                entry = metaData.getServer(key);
                KVMessage temp = newConnection(entry.getIP(), entry.getPort()); // Tear down the existing connection and create a new one with the updated meta data
                if(temp == null){
                    return latestMsg; // If new connection could not be established, return the initial message with the flag NOT_RESPONSIBLE
                }
                else return unsubscribe(key); // Trying to repeat the query after a new connection has been established
            }
        }
        else{
            // the key does not exist in the database yet
            return new Message(StatusType.SUB_ERROR);
        }
        
        return latestMsg;  // Flag is SUB_SUCCESS or SUB_ERROR    
    }

    /**
     * Closes the established connection by handing it over to the responsible KVConnection class. 
     */
    @Override
    public void disconnect() {
        logger.info("trying to close connection ...");
        connection.setRunning(false);
        connection.closeConnection();
        connection = null;
    }

    /**
     * Initializes the process to send a put request to the server. 
     * @param key the given key
     * @param value the given value
     * @return The reply Message from the server
     */
    @Override
    public KVMessage put(String key, String value) { // THIS THING SHOULD DETERMINE THE SERVER AND SO ON
        byte[] msgBytes;
        
        // create the message from its compounds status, key, value 
        if(!value.equals("null")){ 
            msgBytes = new Message(StatusType.PUT, key.getBytes(), value.getBytes()).getByteMessage();
        }               
        else{
            msgBytes = new Message(StatusType.DELETE, key.getBytes()).getByteMessage();
        }
        
        // check if we need to send to a different server
        MetaDataEntry entry = metaData.getServer(key);
        if (!entry.getIP().equals(this.address) || entry.getPort() != this.port) {           
            KVMessage temp = newConnection(entry.getIP(), entry.getPort());    
            if (temp == null) {
                // reconnect failed -> return error
                return new Message(StatusType.PUT_ERROR, key.getBytes());
            }
        }    
        
        // send message
        KVMessage latestMsg = sendAndReply(msgBytes);
        
        // react to server's reply
        if((latestMsg.getStatus() == StatusType.NOT_RESPONSIBLE) && latestMsg.getKey().equals("meta")){
            byte[] latestValue = latestMsg.getValueAsBytes();
            updateMetaData(latestValue);
                        
            entry = metaData.getServer(key);
            
            KVMessage temp = newConnection(entry.getIP(), entry.getPort()); // Tear down the existing connection and create a new one with the updated meta data
            if(temp == null){
                return latestMsg; // If new connection could not be established, return the initial message with the flag NOT_RESPONSIBLE
            }
            else return put(key, value); // Trying to repeat the query after a new connection has been established
        }
        
        return latestMsg;
    }

    /**
     * Sends a Message with the given byte array to the connected server
     * and waits for a reply. 
     * @param msgBytes the message bytes to send
     * @return server's reply as KVMessage
     */
    private KVMessage sendAndReply(byte[] msgBytes) {
        try{
            connection.sendMessage(msgBytes);
        } catch(IOException io) {
            logger.error("Unable to send message (put/get/authenticate) in KVStore.");
        }

        //receive reply from server         
        while(connection.getLatestMessage() == null) {
            try{
                Thread.sleep(100);
            }
            catch(InterruptedException e){
                logger.info("The client thread was interrupted. ");
            }
        }

        Message latestMsg = new Message(connection.getLatestMessage().getByteMessage());
        connection.setLatestMessage(null);

        return latestMsg;
    }

    /**
     * Tries to connect to the successors of the currently connected server. 
     * Sends the specified message to the successor that we were able to connect to. 
     * @param ip of the currently connected server
     * @param port of the currently connected server
     * @param msgBytes the message to send
     * @return server's reply as KVMessage
     */
    private KVMessage trySuccessors(String ip, int port, byte[] msgBytes) {
        KVMessage latestMsg;
        MetaDataEntry succ1 = metaData.getSuccessor(ip, port);

        if(newConnection(succ1.getIP(), succ1.getPort()) != null) {
            latestMsg = sendAndReply(msgBytes);
            if(!latestMsg.getStatus().equals(StatusType.NOT_RESPONSIBLE))
                return latestMsg;
        }

        MetaDataEntry succ2 = metaData.getSuccessor(succ1.getIP(), succ1.getPort());

        if(newConnection(succ2.getIP(), succ2.getPort()) != null) {
            latestMsg = sendAndReply(msgBytes);
            return latestMsg;
        }

        return null;
    }
	
    /**
     * Initializes the process to send a get request to the server. 
     * @param key the given key to which the user wants the value
     * @return The reply Message from the server
     */
    @Override
    public KVMessage get(String key) { // THIS SHOULD ALSO DETERMINE THE SERVER
        byte[] msgBytes = new Message(KVMessage.StatusType.GET, key.getBytes()).getByteMessage(); // create the message from its compounds status, key, (value)
        KVMessage latestMsg = sendAndReply(msgBytes);        
        
        if(latestMsg.getStatus().equals(StatusType.NOT_RESPONSIBLE) && latestMsg.getKey().equals("meta")) {
            updateMetaData(latestMsg.getValueAsBytes());
            MetaDataEntry entry = metaData.getServer(key);
            if(newConnection(entry.getIP(), entry.getPort()) != null) {
                latestMsg = sendAndReply(msgBytes);
            } else {
                KVMessage derp = trySuccessors(entry.getIP(), entry.getPort(), msgBytes);
                if(derp == null) return latestMsg; else return derp;
            }
        } else if(latestMsg.getStatus().equals(StatusType.SERVER_STOPPED)) {
            MetaDataEntry entry = metaData.getServer(key);
            KVMessage derp = trySuccessors(entry.getIP(), entry.getPort(), msgBytes);
            if(derp == null) return latestMsg; else return derp;
        }
        
        return latestMsg;
    }
    
    /**
     * Updates the newly received meta data table
     * @param update the new meta data table
     */
    private void updateMetaData(byte[] update){        
        try{
            metaData.extractMetadata(update);
        }
        catch(Exception e){
            logger.error("Meta data entries do not contain enough tokens. Unknown format! ");
            System.err.println("Message in unknown meta data format received. ");
        }
    }
    
    /**
     * The method tears down the current connection and establishes a 
     * new one to the server responsible for the calculated hash value. 
     * 
     * The method applies for put requests, as for put requests the client
     * may only address the main server responsible for this writing range. 
     * 
     * @param ip the ip of the new server to connect to
     * @param port the port of the new server to connect to
     * 
     * @return null if no connection could be established;
     *         otherwise the Message object that connect() returns. 
     */
    private KVMessage newConnection(String ip, int port){
        KVMessage result = null;
        
        disconnect();
        
        this.address = ip;
        this.port = port;
        
        try { // tries to connect to main server
            result = connect();
        } 
        catch (UnknownHostException uhe){
            logger.error("Failed to connect to main server due to unknown host");
        }
        catch (IOException ex) {
            logger.error("Unable to connect to main server");
        }
                    
        return result;
    }
    
    /**
     * Adds this client's application to the listeners array as a ClientSocketListener
     * @param listener the listener
     */
    public void addListener(ClientSocketListener listener){
        listeners.add(listener);
    }
    
    /**
     * 
     * @return the user's email address
     */
    public String getEmail(){
        return this.email;
    }

   
    
}
