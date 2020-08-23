package client;

import common.messages.Message;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Set;
import common.logger.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class is responsible for managing the connection of one specific client
 * application to the server. The connection runs within a Thread and receives 
 * messages from the server in the first place. 
 */
public class KVConnection extends Thread {
    private static final Logger logger = LogManager.getLogger(Constants.CLIENT_NAME);
    
    private boolean running;

    private Socket clientSocket;
    private Set<ClientSocketListener> listeners;
    private OutputStream output;
    private InputStream input;

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 120023; //because 120 022 is our max message length
    
    private Message latestMsg; // this is a shared resource
    
    /**
     * Initializes a connection with the given Socket and a Set of its Listeners. 
     * @param socket the socket with the corresponding ip-address and port
     * @param listeners the Set of Listeners
     */
    public KVConnection(Socket socket, Set<ClientSocketListener> listeners) {
        this.clientSocket = socket;
        this.listeners = listeners;
    }
    
    /**
     * The method is responsible for trying to close the connection. 
     */
    public synchronized void closeConnection() {
        logger.info("trying to close connection ...");

        try {
            tearDownConnection();
            listeners.forEach((listener) -> {
                listener.handleStatus(ClientSocketListener.SocketStatus.DISCONNECTED);
            });
        } catch (IOException e) {
            logger.error("Unable to close connection!");
            System.err.println("Unable to close connection!");
        }
    }
    
    /**
     * This method actively closes the connection by: 
     * closing the input and output streams and closing and destroying the socket
     * @throws IOException 
     */
    private void tearDownConnection() throws IOException {
        setRunning(false);
        logger.info("tearing down the connection ...");
        if (clientSocket != null) {
             input.close();
             output.close();
             clientSocket.close();
             clientSocket = null;
             logger.info("connection closed!");
        }
   }
   
   public boolean isRunning() {
        return running;
   }

   /**
    * Sets running to the given boolean value. 
    * @param run the new running value
    */
   public void setRunning(boolean run) {
        running = run;
   }
   
   /**
    * the method sends a Message using this socket.
    * @param msg the message in form of a byte array
    * @throws IOException some I/O error regarding the output stream 
    */
   public void sendMessage(byte[] msg) throws IOException {
        output.write(msg, 0, msg.length);
        //output.write("\r".getBytes(StandardCharsets.UTF_8));
        output.flush();
    }
   
   /**
    * Returns the last message received from the server. Might return null. 
    * 
    * @return latest message from server if there has been one
    */
   public Message getLatestMessage(){
       return latestMsg;
   }
   
   /**
    * Sets the variable which stores the last message received from the server 
    * to the given value. 
    * @param msg the new latestMsg value
    */
   public void setLatestMessage(Message msg){
       latestMsg = msg;
   }
    
    /**
     * Reads in the message from the server. Transforms it from a byte stream
     * into a Message object. 
     * @return The message from the server as a Message object. 
     * @throws IOException if sth goes wrong when reading in the byte stream
     */
    private Message receiveMessage() throws IOException {
        int index = 0;
        byte[] msgBytes = null, tmp;
        byte[] bufferBytes = new byte[BUFFER_SIZE];

        /* read first char from stream */
        byte read = (byte) input.read();	
        boolean reading = true;

        while(read != 13 && reading) {/* carriage return */
            /* if buffer filled, copy to msg array */
            if(index == BUFFER_SIZE) {
                if(msgBytes == null){
                    tmp = new byte[BUFFER_SIZE];
                    System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
                } else {
                    tmp = new byte[msgBytes.length + BUFFER_SIZE];
                    System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
                    System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
                                    BUFFER_SIZE);
                }

                msgBytes = tmp;
                bufferBytes = new byte[BUFFER_SIZE];
                index = 0;
            } 

            bufferBytes[index] = read;
            index++;

            /* stop reading if DROP_SIZE is reached */
            if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
                reading = false;
            }

            /* read next char from stream */
            read = (byte) input.read();
        }

        if(msgBytes == null){
            tmp = new byte[index];
            System.arraycopy(bufferBytes, 0, tmp, 0, index);
        } else {
            tmp = new byte[msgBytes.length + index];
            System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
            System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
        }

        msgBytes = tmp; 
        if(msgBytes == null){
            return null;
        }
        
        if(msgBytes[0] == -1) {
            throw new IOException();
        }
        
        /* build final String */
        Message msg = null;
        try{
            msg = new Message(msgBytes);
        }
        catch(Exception e){
            e.printStackTrace();
            logger.error("Server reply has unknown format. ");
            System.err.println("Server reply has unknown format. ");
        }
        return msg;
    }
    
    /**
     * Initializes the input stream and the output stream to and from the socket;
     * receives replies from the server. 
     * This method is being launched by calling start() on an object of this class. 
     * This object is a Thread implementation. 
     */
    @Override
    public void run() {
         try {
            output = clientSocket.getOutputStream();
            input = clientSocket.getInputStream();
            setRunning(true);
            logger.info("Connection established");
            
            while(isRunning()) {
                try {
                    latestMsg = receiveMessage(); // receive a message from the server and save it in a variable for later                                               
                } catch (IOException ioe) {
                    if(isRunning()) {
                        logger.error("Connection lost!");
                        System.err.println("Connection lost!");
                        try {
                            tearDownConnection();
                            listeners.forEach((listener) -> {
                                listener.handleStatus(ClientSocketListener.SocketStatus.CONNECTION_LOST);
                            });
                        } catch (IOException e) {
                                logger.error("Unable to close connection!");
                                System.err.println("Unable to close connection!");
                        }
                    }
                }				
            }
        } catch (IOException ioe) {
            logger.error("Connection could not be established!");
            System.err.println("Connection could not be established");

        } finally { // close the connection if the boolean running has been externally set to false
            if(isRunning()) {
                closeConnection();
            }
        }
    }
}
