/*
 *
 */
package app_kvEcs;

import common.constants.EcsErrorAddress;
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
import java.net.ServerSocket;
import java.util.Arrays;

public class ECSClient { 
    
    private static final Logger LOGGER = LogManager.getLogger(Constants.ECS_NAME);

    private static final String PROMPT = "ECS> ";
    private BufferedReader stdin;
    private final CommandManager commander;
    
    /**
     * CONSTRUCTOR
     * 
     * @param path the path of ms3-server.jar which is normally local, 
     * so we are taking it in as an argument when creating the ecs. 
     */
    public ECSClient(String path){
        this.commander = new CommandManager(path);
		ErrorBuffer buffer = new ErrorBuffer();
		(new HandleError(this.commander, buffer)).start();
		(new ErrorListener(EcsErrorAddress.PORT, buffer)).start();
    }
    
    /**
     * The method runs a console enabling the user to give commands to the application. 
     */
    public void run() {
        
	//Console Input - loop
        while (!commander.getStop()) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();               
                handleCommand(cmdLine);
            } catch (IOException e) {
                commander.setStop(true);
                System.err.println("ECS does not respond - Application terminated ");
                LOGGER.error("ECS does not respond - Application terminated ");
            }
        }
    }
    
    /* USER INPUT ----------------------------------------------------------- */
    /**
     * Checks, if input is a valid command and executes the command, if possible
     * @param cmdLine the user's input
     */
    private void handleCommand(String cmdLine) {
        String[] tokens = cmdLine.split("\\s+");
        
        if(!(tokens[0].equals("help") || commander.tryLock())) {
            System.out.println("ECS is currently locked for user requests. Try again later. ");
            LOGGER.info("ECS is currently locked for user requests");
        } else {
            switch (tokens[0]) {
                case "initService":
                    if (tokens.length == 4) {
                        try{
                            boolean result = commander.initService(tokens[1], tokens[2], tokens[3]); //  migrated
                            if(result){
                                commander.setServiceRunning(true); // ----- MIGRATE!
                                LOGGER.info("Service initialized. ");
                                System.out.println("Service initialized");
                            }
                            else{
                                LOGGER.error("Service could not be initialized. One or more servers responded with an error. ");
                                System.err.println("Service could not be initialized. Please, try again. ");
                            }
                        }
                        catch(Exception e) {
                            System.err.println("Parameters have wrong format!");
                        }
                    } 
                    else {
                        System.err.println("Invalid number of parameters!");
                    }   break;

                case "start":
                    System.out.println(PROMPT + "Starting servers...");

                    if(commander != null){
                        try{
                            boolean result = commander.start(); 
                            if(result){
                                LOGGER.info("Servers successfully started. ");
                                System.out.println("Servers successfully started");
                            }
                            else{
                                LOGGER.error("Servers could not be started. One or more servers responded with an error. ");
                                System.err.println("Servers could not be started. Please, try again. ");
                            }
                        }
                        catch(Exception e){
                            System.err.println("Unable to start servers!");
                            LOGGER.error("Unable to start servers. ");
                        }
                    }
                    else System.err.println("You have to initialize servers first!");

                    break;

                case "stop":
                    System.out.println(PROMPT + "Stopping servers...");

                    if(commander != null){
                        try{
                            boolean result = commander.stop(); 
                            if(result){
                                LOGGER.info("Servers successfully stopped. ");
                                System.out.println("Servers stopped");
                            }
                            else{
                                LOGGER.error("Servers could not be stopped. One or more servers responded with an error. ");
                                System.err.println("Servers could not be stopped. Please, try again. ");
                            }
                        }
                        catch(Exception e){
                            System.err.println("Unable to stop servers!");
                            LOGGER.error("Unable to stop servers. ");
                        }
                    }
                    else System.err.println("You have to initialize servers first!");

                    break;

                case "shutDown":
                    if(commander != null){
                        System.out.println(PROMPT + "Shutting down servers...");
                        boolean result = commander.getServiceRunning() && commander.shutDown(); //SET ALL RUNNING TO FALSE
                        if(result){
                            commander.setServiceRunning(false); // -----MIGRATE!
                            LOGGER.info("Servers shut down. ");
                            System.out.println("Servers shut down");
                        }
                        else{
                            LOGGER.error("Servers could not be shut down. One or more servers responded with an error. ");
                            System.err.println("Servers could not be shut down. Please, try again. ");
                        }

                    }
                    else System.err.println("You have to initialize servers first!");

                    break;

                case "addNode":
                    if(tokens.length == 3){
                        System.out.println(PROMPT + "Adding new node...");
                        boolean result = commander.addNode(tokens[1], tokens[2]); // ---------------------------- migrate
                        if(result){
                                LOGGER.info("New node added. ");
                                System.out.println("New node added");
                            }
                            else{
                                LOGGER.error("New node could not be added. Server responded with an error. ");
                                System.err.println("New node could not be added. Please, try again. ");
                            }
                    }
                    else{
                        System.err.println("Invalid number of parameters!");
                    }
                    break;

                case "removeNode":
                    if(commander != null){
                        System.out.println(PROMPT + "Removing node...");
                        boolean result = commander.removeNode();

                        if(result) {
                                LOGGER.info("Node removed. ");
                                System.out.println("Node removed");
                            }
                            else{
                                LOGGER.error("Node could not be removed. Server responded with an error. ");
                                System.err.println("Node could not be removed. Please, try again. ");
                            }
                    }
                    else System.err.println("You have to initialize servers first!");

                    break;

                case "quit":
                    commander.setStop(true);
                    System.out.println(PROMPT + "Application exit!");
                    System.exit(0);
                    break;

                case "logLevel":                
                    if (tokens.length == 2) {
                        if (!setLevel(tokens[1])) {
                            System.err.println("No valid log level!");
                            printPossibleLogLevels();
                        } else {
                            System.out.println(PROMPT +
                                    "Log level changed to level " + tokens[1]);
                        }
                    } else {
                        System.err.println("Invalid number of parameters!");
                    }   break;
                case "help":
                    printHelp();
                    break;
                default:
                    System.err.println("Unknown command");
                    printHelp();
                    break;
            }
            if(!tokens[0].equals("help"))
                commander.unlock();
        }
    }
    
    /* END UNSER INPUT -------------------------------------------------------*/
    
    
    /* ---------------------------------------------------------------------- */
    
    /**
     * COMMAND HELP
     * 
     * Displays the content of the "help" command
     */
    private void printHelp() { 
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("ECS HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("initService <numberOfNodes> <cacheSize> <displacementStrategy>");
        sb.append("\t initializes servers\n");
        sb.append(PROMPT).append("start");
        sb.append("\t\t\t\t starts the initialized servers \n");
        sb.append(PROMPT).append("stop");
        sb.append("\t\t\t\t stops the initialized servers \n");
        sb.append(PROMPT).append("shutDown");
        sb.append("\t\t\t\t shuts down all the initialized servers \n");
        sb.append(PROMPT).append("addNode <cacheSize> <displacementStrategy>");
        sb.append("\t\t stops the initialized servers \n");
        sb.append(PROMPT).append("removeNode");
        sb.append("\t\t removes a server at an arbitraty position \n");

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
     * Main entry point for the echo server application.
     *
     * @param args[0] the path of ms3-server.jar which is normally local, 
     * so we are taking it in as an argument when creating the ecs. 
     */
    public static void main(String[] args) {
        try {
            ECSClient app = new ECSClient(args[0]);
            app.run();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error! Unable to initialize logger!");
            System.exit(1);
        }
    }
    
    
}
