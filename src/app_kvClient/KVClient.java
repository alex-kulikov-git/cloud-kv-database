package app_kvClient;

import client.ClientSocketListener;
import client.KVStore;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.logger.Constants;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.Arrays;

/**
 * The class is responsible for the application side of the client
 * @author blueblastindustries
 */
public class KVClient implements ClientSocketListener{
    private static final Logger logger = LogManager.getLogger(Constants.CLIENT_NAME);

    private static final String PROMPT = "Client> ";
    private BufferedReader stdin;
    private KVStore client = null;
    private boolean stop = false;
    private boolean connected = false;

    private String serverAddress;
    private int serverPort;

    /**
     * The method runs a console enabling the user to give commands to the application. 
     */
    public void run() {
	//Console Input - loop
        while (!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                stop = true;
                printError("Client does not respond - Application terminated ");
                logger.error("Client does not respond - Application terminated ");
            }
        }
    }


/* USER INPUT---------------------------------------------------------------- */
    /**
     * Checks, if input is a valid command and executes the command, if possible
     */
    private void handleCommand(String cmdLine) {
        String[] tokens = cmdLine.split("\\s+");

        switch (tokens[0]) {
            case "quit":
                stop = true;
                disconnect();
                System.out.println(PROMPT + "Application exit!");
                break;
            case "connect": // NO LONGER NEEDED => why not? we need to create a KVStore object with initial server address and port
                if (tokens.length == 5) {
                    serverAddress = tokens[1];
                    serverPort = Integer.parseInt(tokens[2]);
                    String email = tokens[3];
                    String password = tokens[4];
                    
                    connect(serverAddress, serverPort, email, password);
                } else {
                    printError("Invalid number of parameters!\nParameters: "
                            + "<server address> <server port> <email address> "
                            + "<password>");
                }   
                break;
            case "put":
                if (tokens.length >= 3) {
                    if (client != null && client.connection.isRunning()) {
                        
                        // we assume that key consists of only 1 token, as this is usually the case
                        String key = tokens[1];
                        
                        // assemble value from tokens, because value can contain spaces
                        StringBuilder value = new StringBuilder();
                        for (int i = 2; i < tokens.length; i++) {
                            value.append(tokens[i]);
                            if (i != tokens.length - 1) {
                                value.append(" ");
                            }
                        }
                        
                        String tmp = put(key, value.toString());
                        System.out.println(tmp); // The result of the put operation
                    }
                    else {
                        printError("Not connected!");
                    }
                }
                else {
            System.err.println("Unable to send 'put' message. ");
                    printError("No key or no value passed!");
                }   
                break;
            case "get":
                if (tokens.length >= 2) {
                    if (client != null && client.connection.isRunning()) {
                        String key = tokens[1];
                        
                        String tmp = get(key);
                        System.out.println(tmp); // The result of the get operation
                    }
                    else {
                        printError("Not connected!");
                    }
                }
                else {
                    printError("No key passed!");
                }   
                break;
            case "disconnect":
                disconnect();
                break;
            case "sub":
                if(tokens.length == 2){
                    if (client != null && client.connection.isRunning()) {
                        String key = tokens[1];
                        
                        String tmp = subscribe(key);
                        System.out.println(tmp);
                    }
                    else {
                        printError("Not connected!");
                    }
                }
                else {
                    printError("No key passed!");
                }
                break;
            case "unsub":
                if(tokens.length == 2){
                    if (client != null && client.connection.isRunning()) {
                        String key = tokens[1];
                        
                        String tmp = unsubscribe(key);
                        System.out.println(tmp);
                    }
                    else {
                        printError("Not connected!");
                    }
                }
                else {
                    printError("No key passed!");
                }
                break;
            case "logLevel":                
                if (tokens.length == 2) {
                    if (!setLevel(tokens[1])) {
                        printError("No valid log level!");
                        printPossibleLogLevels();
                    } else {
                        System.out.println(PROMPT +
                                "Log level changed to level " + tokens[1]);
                    }
                } else {
                    printError("Invalid number of parameters!");
                }   
                break;
            case "help":
                printHelp();
                break;
            default:
                printError("Unknown command");
                printHelp();
                break;
        }
    }
/* -------------------------------------------------------------------------- */
    
    /**
     * COMMAND CONNECT 
     * 
     * Tries to establish a connection to the server by handing it over to KVStore
     * @param address The ip-address given by the user
     * @param port The port given by the user
     * @param email User's email address for authentication
     * @param password User's password for authentication
     */
    private void connect(String address, int port, String email, String password){
        client = new KVStore(address, port, email, password);         
        
        try{
            KVMessage msg = client.connect();
            StatusType status = msg.getStatus();
            
            switch(status) {
                case PUT: 
                    if(msg.getKey().equals(address) && Integer.parseInt(msg.getValue()) == port){
                        connected = true;
                        System.out.println("Connection established"); 
                    }
                    else{
                        connected = false; // connected is set to false by default anyways, but safe is safe
                        System.err.println("Server replied incorrectly to connection request. ");
                    }                        
                    break;
                
                case AUTH_SUCCESS:
                    System.out.println("Connection established. ");
                    connected = true;
                    break;
                    
                case AUTH_ERROR:
                    System.err.println("Unable to authenticate. Wrong user or password. ");
                    connected = false;
                    break;
                    
                default: 
                    connected = false; // connected is set to false by default anyways, but safe is safe
                    System.err.println("Server replied incorrectly to connection request. ");
                    break;
            }
        }
        catch (NumberFormatException nfe) {
            printError("No valid address. Port must be a number!");
            logger.info("Unable to parse argument <port>", nfe);
        } catch (UnknownHostException e) {
            printError("Unknown Host!");
            logger.info("Unknown Host!", e);
        } catch (IOException e) {
            printError("Could not establish connection!");
            logger.warn("Could not establish connection!", e);
        }            
        client.addListener(this);
    }

    /**
     * COMMAND DISCONNECT 
     * 
     * Shuts down the connection and destroys the KVStore instance. 
     */
    private void disconnect() {
        if (client != null) {
            client.disconnect();
            client = null;
            System.out.println("Connection closed");
        }
    }
    
    /**
     * COMMAND PUT
     * 
     * Hands the put operation over to the KVStore class, gets the result of the operation back, 
     * and hands the result to the handleCommand method to print it to the console. 
     * A put operation by the user with the value being set to "" or null is 
     * being interpreted as a delete operation of the tuple. 
     * 
     * @param key The key given by the user
     * @param value The value given by the user
     * @return The result of the operation
     */
    private String put(String key, String value){ 
        if(connected){ // this method should only be reached when already connected, but still: safe is safe
            
            KVMessage msg = client.put(key, value); // hand the operation over to KVStore                
            StatusType status = msg.getStatus();
            
            switch(status){
                case PUT_SUCCESS: {
                    return "The key-value pair '" + msg.getKey() + " = " 
                        + msg.getValue() + "' has been saved successfully. ";
                }
                case PUT_UPDATE: {
                    return "The key-value pair '" + msg.getKey() + " = " 
                        + msg.getValue() + "' has been updated successfully. ";
                }
                case PUT_ERROR: {
                    return "The key-value pair could not be saved. ";
                }
                case DELETE_SUCCESS: {
                    return "The key-value pair " + msg.getKey() + " has been successfully deleted. ";
                }
                case DELETE_ERROR: { // due to our implementation of the server/disc side of the application, this flag cannot really be received
                    return "The key-value pair could not be deleted. ";
                }
                case FAILED: {
                    return "The server reported an unknown error. ";
                }
                case NOT_RESPONSIBLE:{
                    if(! msg.getKey().equals("meta") || msg.getValue() == null)
                        return "The server reported an unknown error. ";
                    else{
                        //return put(key, value); 
                        return "Unable to connect to the responsible server. ";
                    }
                }
                case SERVER_WRITE_LOCK:{
                    return "Server currently only accepts read (get) queries. ";
                }
                case SERVER_STOPPED:{
                    return "Server currently does not accept queries. Try again later. ";
                }
                default: {
                    return "An unknown error occurred when saving the key-value pair. ";
                }
            }                   
        }
        else return "You have to establish a connection to the server first!";
    }
    
    /**
     * COMMAND GET
     * 
     * Hands the get operation over to the KVStore class, gets the result of the operation back, 
     * and hands the result to the handleCommand method to print it to the console. 
     * @param key The key to which the user wants the value
     */
    private String get(String key){
        if(connected){  // this method should only be reached when already connected, but still: safe is safe
           
            KVMessage msg = client.get(key); // hand the operation over to KVStore
            StatusType status = msg.getStatus();
            
            switch(status){
                case GET_SUCCESS: {
                    return "The value to key '" + msg.getKey() + "' is '"
                        + msg.getValue() + "'. ";
                }
                case GET_ERROR: {
                    return "The value to key '" + msg.getKey() + "' could not ne found. ";
                }
                case FAILED: {
                    return "The server reported an unknown error. ";
                }
                case NOT_RESPONSIBLE:{
                    if(! msg.getKey().equals("meta") || msg.getValue() == null)
                        return "The server reported an unknown error. ";
                    else{
                        return "Unable to connect to the responsible server. ";
                    }
                }
                case SERVER_STOPPED:{
                    return "Server currently does not accept queries. Try again later. ";
                }
                default: {
                    return "An unknown error occurred when retrieving the value. ";
                }
            }            
        }
        else return "You have to establish a connection to the server first!";
    }
    
    /**
     * Hands the subscribe operation over to KVStore, gets the result of the 
     * operation back, and returns a result message as a String. 
     * @param key the key to subscribe to
     * @return result of the subscription attempt as String
     */
    private String subscribe(String key){
        if(connected){  // this method should only be reached when already connected, but still: safe is safe
           
            KVMessage msg = client.subscribe(key, client.getEmail());
            StatusType status = msg.getStatus();
            
            switch(status){
                case SUB_SUCCESS: {                          
                    return "Your subscription request to key '" + key + "' was successful. ";
                }
                case SUB_ERROR: {
                    return "Your subscription request to key '" + key + "' was not successful. "; 
                }
                case FAILED: {
                    return "The server reported an unknown error. ";
                }
                case NOT_RESPONSIBLE:{
                    if(! msg.getKey().equals("meta") || msg.getValue() == null)
                        return "The server reported an unknown error. ";
                    else{
                        return "Unable to connect to the responsible server. ";
                    }
                }
                case SERVER_WRITE_LOCK:{
                    return "Server currently only accepts read (get) queries. ";
                }
                case SERVER_STOPPED:{
                    return "Server currently does not accept queries. Try again later. ";
                }
                default: {
                    return "An unknown error occurred when executing the command. ";
                }
            }            
        }
        else return "You have to establish a connection to the server first!";
    }
    
    /**
     * Hands the unsubscribe request over to KVStore, gets the result of the 
     * operation back, and returns it as a piece of text. 
     * @param key the key
     * @return the result of the unsubscription request
     */
    private String unsubscribe(String key){
        if(connected){  // this method should only be reached when already connected, but still: safe is safe
           
            KVMessage msg = client.unsubscribe(key);
            StatusType status = msg.getStatus();
            
            switch(status){
                case SUB_SUCCESS: {                          
                    return "Your subscription request to key '" + key + "' was successful. ";
                }
                case SUB_ERROR: {
                    return "Your subscription request to key '" + key + "' was unsuccessful. "; 
                }
                case FAILED: {
                    return "The server reported an unknown error. ";
                }
                case NOT_RESPONSIBLE:{
                    if(! msg.getKey().equals("meta") || msg.getValue() == null)
                        return "The server reported an unknown error. ";
                    else{
                        return "Unable to connect to the responsible server. ";
                    }
                }
                case SERVER_WRITE_LOCK:{
                    return "Server currently only accepts read (get) queries. ";
                }
                case SERVER_STOPPED:{
                    return "Server currently does not accept queries. Try again later. ";
                }
                default: {
                    return "An unknown error occurred when executing the command. ";
                }
            }            
        }
        else return "You have to establish a connection to the server first!";
    }
    
    /**
     * COMMAND HELP
     * 
     * Displays the content of the "help" command
     */
    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("ECHO CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("connect <host> <port>");
        sb.append("\t establishes a connection to a server\n");
        sb.append(PROMPT).append("send <text message>");
        sb.append("\t\t sends a text message to the server \n");
        sb.append(PROMPT).append("disconnect");
        sb.append("\t\t\t disconnects from the server \n");

        sb.append(PROMPT).append("logLevel");
        sb.append("\t\t\t changes the logLevel \n");
        sb.append(PROMPT).append("\t\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

        sb.append(PROMPT).append("quit ");
        sb.append("\t\t\t exits the program");
        System.out.println(sb.toString());
    }

    /* complements help */
    private void printPossibleLogLevels() {
        System.out.println(PROMPT
                + "Possible log levels are:");
        System.out.println(PROMPT
                + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
    }

    /**
     * COMMAND SET LEVEL 
     * 
     * Sets the log level to the one given by the user. 
     * @param levelString the new log level
     */
    private boolean setLevel(String levelString) {
        String[] allowedParams = {"ALL", "DEBUG", "INFO", "WARN", "ERROR", "FATAL", "OFF"};

        if (!Arrays.asList(allowedParams).contains(levelString)) {
            return false;
        }

        Level level = Level.toLevel(levelString);
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
        loggerConfig.setLevel(level);
        ctx.updateLoggers();
        return true;
    }

    /**
     * complements setLevel
     * Reacts accordingly to the specific socket status. 
     * @param status the given status
     */
    @Override
    public void handleStatus(SocketStatus status) {
        if (null != status) switch (status) {
            case CONNECTED:
                break;
            case DISCONNECTED:
                System.out.print(PROMPT);
                System.out.println("Connection terminated: "
                        + serverAddress + " / " + serverPort);
                break;
            case CONNECTION_LOST:
                System.out.println("Connection lost: "
                        + serverAddress + " / " + serverPort);
                System.out.print(PROMPT);
                break;
            default:
                break;
        }

    }

    /**
     * Prints an error message. 
     * @param error the message
     */ 
    private void printError(String error) {
        System.out.println(PROMPT + "Error! " + error);
    }

    /**
     * Main entry point for the echo server application.
     *
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
        try {
            KVClient app = new KVClient();
            app.run();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error! Unable to initialize logger!");
            System.exit(1);
        }
    }

}
