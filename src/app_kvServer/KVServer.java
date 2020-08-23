package app_kvServer;

import common.constants.PortOffset;
import common.logger.Constants;
import common.messages.MetaData;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import static javax.script.ScriptEngine.FILENAME;
import manager.CacheManager;
import manager.SubscriptionManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * represents the server 
 * 
 */
public class KVServer extends Thread {
    private static final Logger LOGGER = LogManager.getLogger(Constants.SERVER_NAME);
    
    private int port;
    private String ip;
    private ExecutorService executor;
    private CacheManager manager;
    private ServerSocket server;
    private MetaData metaData; // needs to be initialized
    private Users userList;
    private SubscriptionManager subManager;
    
    /**
     * Start KV Server at given port
     *
     * @param port - given port for storage server to operate
     * @param manager - the shared cacheManager
     */
    public KVServer(int port, CacheManager manager, Users userList, SubscriptionManager subManager) {
        this.port = port;
        this.executor = Executors.newCachedThreadPool();
        this.manager = manager;
        this.metaData = new MetaData();
        this.userList = userList;
        this.subManager = subManager;
        
        try {
            this.server = new ServerSocket(port);
	    this.ip = server.getInetAddress().getHostAddress();
        } catch(IOException ioe) {
            LOGGER.error("Unable to create ServerSocket in main server thread on: " + port);
            System.err.println("Unable to create ServerSocket in main server thread");
        }
        
    }
    
    public String getIP() {
        return this.ip;
    }
    
    public MetaData getMetaData() {
        return this.metaData;
    }
    
    public int getPort() {
        return this.port;
    }
       
    @Override
    public void run() {
        while(true) {
            try{ // passes ingoing connections to thread pool
                executor.execute(new HandleConnection(getListener().accept(), manager, metaData, userList, subManager));
                
            } catch(IOException io) {
                System.out.println("nothing...");
            }
        }   
    }
    
    /**
     * Access to server socket
     * @return ServerSocket of KVServer
     */
     public ServerSocket getListener() {
            return server;
        }
    
     /**
      * Answers if String can be parsed as integer
      * @param number - supposed to be a number
      * @return can be parsed - true; cannot be parsed - false
      */
     public static boolean validNumber(String number) {
         try{
             Integer.parseInt(number);
         } catch(NumberFormatException nf) {
             return false;
         }
         return true;
     }
     
     /**
      * Answers if the given port is in a valid port range.
      * @param port - given port
      * @return port is valid - true; port is not valid - false
      */
     public static boolean validPort(int port) {
         return port > 1024 && port < 65535;
     }
     
     /**
      * Answers if the given strategy is a valid cache strategy.
      * @param strategy - given strategy
      * @return strategy is valid - true; strategy is not valid - false
      */
     public static boolean validStrategy(String strategy) {
         switch(strategy) {
             case "FIFO": return true;
             case "LRU": return true;
             case "LFU": return true;
             default: return false;
         }
     }
        
    public static void main(String[]args) throws InterruptedException { // accept it or catch it?
        // handle wrong launch arguments - arg0/port arg1/cacheSize arg2/strategy
        if(!( args.length == 3 && validNumber(args[0]) && validNumber(args[1]) && validStrategy(args[2]) && validPort(Integer.parseInt(args[0])) )) {
            System.out.println("Invalid Launch Arguments");
            System.exit(5);
        }
               
        // subscriber List
        Users userList = new Users();
        
        // read login list from file
        BufferedReader br = null;
        FileReader fr = null;
        
	LOGGER.info(System.getProperty("user.dir"));		
        try {
            //br = new BufferedReader(new FileReader(FILENAME));
            fr = new FileReader("src/user_list.txt");
            br = new BufferedReader(fr);

            String sCurrentLine;
            while ((sCurrentLine = br.readLine()) != null) {
                String[] user_pw = sCurrentLine.split("\\s+");
                
                byte[] pw = new byte[16];
                String[] elements = user_pw[1].split("_");
                for(int i = 0; i < 16; i++)
                    pw[i] = (byte) Integer.parseInt(elements[i]);
                    
                LOGGER.info("user: " + user_pw[0]);
                String derp = "";
                for(int i = 0; i < pw.length; i++) {
                    derp += pw[i];
                    if(i < pw.length - 1) derp += "_";
                }
                    
                LOGGER.info("pw: " + derp);
                userList.addPair(user_pw[0], pw); // did we properly save the hash?
            }           
        } catch (IOException e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
						
            LOGGER.info(sw.toString());
        } finally {
            try {
                if (br != null)
                    br.close();
                if (fr != null)
                    fr.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
	}
		
        // write some log messages
        userList.logUsers();
        
        // launch arguments are not used anymore?
        SubscriptionManager sub_manager = new SubscriptionManager();
        CacheManager manager = new CacheManager(Integer.parseInt(args[1]), args[2], Integer.parseInt(args[0]));
        manager.setSubscriptionManager(sub_manager);
        KVServer theServer = new KVServer(Integer.parseInt(args[0]), manager, userList, sub_manager);
        
        // starting ecs thread
        HandleEcs adminCommandLoop = new HandleEcs(manager, Integer.parseInt(args[0]), theServer.metaData);
        adminCommandLoop.start();
        LOGGER.info("Launched Admin CommandLoop.");
        
        // starting Julia
        PingListener pingListenerLoop = new PingListener(theServer.port, theServer.metaData, manager, sub_manager); // is metaData here already initialized?
        pingListenerLoop.start();
        
        // starting Romeo
        GossipHeartRomeo romeo = new GossipHeartRomeo(theServer.getMetaData(), manager, "127.0.0.1" , theServer.getPort());
        romeo.start();   
        		
        // starting the server        
        theServer.start();
        LOGGER.info("Launched Server on port: " + theServer.port);

        // waiting for shutdown         
        while(manager.is_alive()) {
            Thread.sleep(4000);
        }
               
        Thread.sleep(5000); // gives the connected Threads time to finish requests.
        System.exit(1);
        //OS should now free all ports, sockets and kill all remaining threads.
    }
}
