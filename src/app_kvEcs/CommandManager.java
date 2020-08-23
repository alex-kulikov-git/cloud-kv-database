/*
 *
 */
package app_kvEcs;

import common.hashing.Hashing;
import common.logger.Constants;
import common.messages.AdminMessage;
import common.messages.KVAdminMessage.AdminType;
import common.messages.MetaData;
import common.messages.MetaDataEntry;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The class analyzes and forwards the commands by the ECS to the servers. 
 */
public class CommandManager {
    
    private static final Logger LOGGER = LogManager.getLogger(Constants.SERVER_NAME);
    
    // Every entry includes the server's ip, port, cache size and displacement strategy
    private ArrayList<Server> servers;    
    private MetaData metaData;
    private final ExecutorService executor;  
    private final String path;
    
    private static final String FILENAME = "src/ecs.config"; 
    private boolean stop; // is this necessary? 
    private ArrayList<ServerStatus> availableServers; // list of available servers, which may be marked as running
    private ArrayList<ServerStatus> serversDown;
    private boolean serviceRunning; 
    
    private boolean locked; // whether the user console for the ecs has to be locked
    private ReentrantLock lock;
    
    
    /**
     * CONSTRUCTOR
     * 
     * @param path - server jar location
     */
    public CommandManager(String path) {
       this.metaData = new MetaData();
       this.servers = new ArrayList<Server>();
       this.availableServers = new ArrayList<ServerStatus>();
       this.serversDown = new ArrayList<ServerStatus>();
       this.executor = Executors.newCachedThreadPool();
       this.path = path;
       this.lock = new ReentrantLock();
       loadConfigFile();
    }   
    
    public void lock() {
        this.lock.lock();
    }
    
    public void unlock() {
        this.lock.unlock(); 
    }
    
    public boolean tryLock() {
        return this.lock.tryLock();
    }
    
    /**
     *
     * @return the number of servers currently running
     */
    public int getServersRunning() {
        int result = 0;
        for(ServerStatus server : availableServers) {
            if (server.getRunning())
                result++;
        }
        return result;
    }
    
     /**
     * Commands a given server to shut down immediately. This is only used for test purposes and should not exist in a practical application!
     * @param ip the IP of the target server
     * @param port the port of the target server
     */    
    public void crashServer(String ip, int port) {
        ArrayList<Server> toCrash = new ArrayList<>();
        toCrash.add( getServer(ip,port) );
        executeGroupCommand_nowait(new AdminMessage(AdminType.CRASH), toCrash);   
    }
    
    private void addServerDown(String ip, int port) {
        serversDown.add(new ServerStatus(ip, port, false));
    }
    
    private void removeServerDown(String ip, int port) {
        for(ServerStatus crashedServer : serversDown) {
            if(crashedServer.getIp().equals(ip) && crashedServer.getPort() == port) {
                serversDown.remove(crashedServer);
                break;
            }
        }
    }
    
    /**
     * check if a server is down
     * @param ip IP of the server
     * @param port Port of the server
     * @return if the server is down
     */
    public boolean serverIsDown(String ip, int port) {
        //LOGGER.debug("checking if server is down: "+ip+":"+port);
        for(ServerStatus crashedServer : serversDown) {
            //LOGGER.debug("checking crashed server: "+crashedServer.getIp()+":"+crashedServer.getPort());
            if(crashedServer.getIp().endsWith(ip) && crashedServer.getPort() == port) {
                return true;
            }
        }
        return false;
    }
    
    
     /**
     * Loads the file ecs.config and initializes the ArrayList availableServers with entries from the file. 
     * The file itself is not being saved. 
     */
    private void loadConfigFile() {  
        BufferedReader reader = null;
        String whole = "";
        try {            
            reader = new BufferedReader(new FileReader(FILENAME));
            String sCurrentLine;
            
            while ((sCurrentLine = reader.readLine()) != null) {
                whole += sCurrentLine + "\n";
            }
        } 
        catch (FileNotFoundException ex) {
            LOGGER.error("ecs.config could not be found");
            System.err.println("ecs.config could not be found");
        } 
        catch (IOException ex) {
            LOGGER.error("ecs.config could not be read");
            System.err.println("ecs.config could not be read");
        }
        finally {
            try {
                if (reader != null)
                    reader.close();
            } 
            catch (IOException ex) {
                LOGGER.error("BufferedReader could not be closed");
                System.err.println("BufferedReader could not be closed");
            }
        }                
         
        String[] lines = whole.split("\\r?\\n");

        for(int i = 0; i < lines.length; i++){
            String[] tokens = lines[i].split("\\s+");
            String ip = tokens[1];
            int port = Integer.parseInt(tokens[2]);

            availableServers.add(new ServerStatus(ip, port, false));
        }
        
    }
       
    /**
     * Prepares the servers data and sends the request over to the CommandManager
     * @param numberOfNodes the number of nodes to be created
     * @param cacheSize the cash size of the nodes to be created
     * @param displacementStrategy the displacement strategy of the nodes to be created
     * @return if the operation was successful
     */
    public boolean initService(String numberOfNodes, String cacheSize, String displacementStrategy) throws Exception {
        int nodes = Integer.parseInt(numberOfNodes);
        int cache = Integer.parseInt(cacheSize);
        String displacement = displacementStrategy;
        
        if(nodes > availableServers.size()) return false;
        
        ArrayList<Server> servers = createServerEntries(nodes, cache, displacement);   
        
        this.servers = servers; // create server list - previously this was passed as an argument
        this.metaData = createMetaData(servers); // Overwrites old metaData, if service was previously shutDown.

        if(initServiceFinal()) { // redundant ?
            this.serviceRunning = true;
            return true;
        }
        
        this.metaData = new MetaData(); // overwrite metaData with dummy - something went wrong
        this.servers = new ArrayList<Server>(); // overwrite List of running servers with dummy - something went wrong
        
        for(ServerStatus status : this.availableServers) 
            status.setRunning(false);  // no servers started
        
        return false;
    } 
    
    
    /**
     * Initializes all the servers
     */
    private boolean initServiceFinal() {           
        return launchServers() && sendMetaData();
    } 
    
    /**
     * Starts the servers that this class is responsible for. 
     * Servers now can reply to client requests.
     * 
     * @return successful?
     */
    public boolean start() {                                                                                                                 
        return executeGroupCommand(new AdminMessage(AdminType.START), this.servers);
    }    
    
    /**
     * Stops all the servers
     * Servers now can no longer reply to client requests. 
     * @return successful?
     */
    public boolean stop() {                                                            
        return executeGroupCommand(new AdminMessage(AdminType.STOP), this.servers);  
    }
        
    /**
     * Stops all server instances and exits the remote processes. 
     * @return successful?
     */
    public boolean shutDown() {
        this.lock();
        try {
            for(ServerStatus status : availableServers) 
                status.setRunning(false);

            this.metaData = new MetaData(); // overwrites metaData with empty metaData
            //this.serversDown = new ArrayList<ServerStatus>(); // overwrites serversDown with empty List

            return executeGroupCommand(new AdminMessage(AdminType.SHUT_DOWN), this.servers);
        }
        finally {
            this.unlock();
        }
    }
    
    /**
     * Adds a new server to the range of servers running by handing over the request
     * to the CommandManager. 
     * @param cacheSize the cache size of the new server to create
     * @param displacementStrategy the displacement strategy of the new server to create
     * @return successful?
     */
    public boolean addNode(String cacheSize, String displacementStrategy) {
        int cache = Integer.parseInt(cacheSize);
                
        // what, if all servers are running?
        
        boolean found = false;
        
        int i = 0;
        while(!found) {
            i = getRandomServerID();
            if(!availableServers.get(i).getRunning() && !serverIsDown(availableServers.get(i).getIp(), availableServers.get(i).getPort()))
                found = true;
        }
        
        String ip = availableServers.get(i).getIp(); 
        int port = availableServers.get(i).getPort();
        
        Hashing hashing = new Hashing();
        byte[] hashPosition = hashing.hash(ip + ":" + port);  
        
        if(addNodeFinal_replicate(new Server(ip, port, cache, displacementStrategy, hashPosition))) {
            availableServers.get(i).setRunning(true); // sets new server to running
            return true;
        } 
        return false;
    } 
    
    /**
     * Creates and launches a new server. 
     * @param server a new server object to create
     */
    private boolean addNodeFinal(Server server) {
        // create an ArrayList with only this server, because we need an ArrayList parameter
        ArrayList<Server> thisServerOnly = new ArrayList<>();
        thisServerOnly.add(server);
        
                                                              
        boolean result;
        MetaDataEntry info;
        MetaDataEntry moveInfo;
        result = 
                (info = insertIntoMeta(server)) != null && // hash and insert into metaData
                executeGroupSshCommand(new AdminMessage(AdminType.PING), thisServerOnly) && // launch new server via ssh
                executeGroupCommand(new AdminMessage(AdminType.START), thisServerOnly) && // start new server
                executeGroupCommand(new AdminMessage(AdminType.META_DATA, this.metaData.toBytes()), thisServerOnly) && // send it the meta Data
                writeLock(getServer(info.getIP(), info.getPort())) && // lock successor
                moveData(getServer(info.getIP(), info.getPort()), (moveInfo = new MetaDataEntry(server.getIP(), server.getPort(), info.getRange())).toBytes()) && // move data
                sendMetaData() && // send meta data update to all servers
                unlockWrite(getServer(info.getIP(), info.getPort())); // unlock successor
        
        // we know if something goes wrong, but we cannot react to an error which would break the system
        
        if(result) { // adds server to the server list of running servers
            servers.add(server);
            return true;
        }
        return false;
    }
    
    /**
     * Prepares removal of random Node and calls removeNodeFinal
     * @return 
     */
    public boolean removeNode() {
        int randomRunning = 0;
        if(oneServerRunning()) { // migrated
            randomRunning = getRandomRunning();
            return removeNodeFinal_replicate(availableServers.get(randomRunning).getIp(), availableServers.get(randomRunning).getPort(), randomRunning); // migrated
        }
        
        return false;
    }
    
    /**
     * remove a crashed server from the meta data and reconciliate the system
     * @param ip IP of the crashed server
     * @param port Port of the crashed server
     */
    public void removeCrashedServer(String ip, int port) {
        if(serverIsDown(ip, port))
            return;
        
        LOGGER.info("ECS: removing crashed server");
        
        addServerDown(ip, port);
        
        // get 2 predecessors
        MetaDataEntry pred1 = metaData.getPredecessor(ip, port);
        MetaDataEntry pred2 = metaData.getPredecessor(pred1.getIP(), pred1.getPort());
        
        // get 2 successors
        MetaDataEntry succ1 = metaData.getSuccessor(ip, port);
        MetaDataEntry succ2 = metaData.getSuccessor(succ1.getIP(), succ1.getPort());
        
        // remove from meta data and server list
        Server server = getServer(ip, port);
        MetaDataEntry info = removeFromMeta(server);
        servers.remove(server);
        
        // move data around to restore replication invariant
        // pred1 to succ2
        Server pred1_server = getServer(pred1.getIP(), pred1.getPort());
        MetaDataEntry moveInfo = new MetaDataEntry(succ2.getIP(), succ2.getPort(), pred1.getRange());
        if (!pred1.getIP().equals(succ2.getIP()) || pred1.getPort() != succ2.getPort())
            this.replicateData(pred1_server, moveInfo.toBytes());
        
        // pred2 to succ1
        Server pred2_server = getServer(pred2.getIP(), pred2.getPort());
        moveInfo = new MetaDataEntry(succ1.getIP(), succ1.getPort(), pred2.getRange());
        if (!pred2.getIP().equals(succ1.getIP()) || pred2.getPort() != succ1.getPort())
            this.replicateData(pred2_server, moveInfo.toBytes());  
        
        // move the data that was on the removed server to succ3
        MetaDataEntry succ3 = metaData.getSuccessor(succ2.getIP(), succ2.getPort()); 
        Server succ1_server = getServer(succ1.getIP(), succ1.getPort());
        moveInfo = new MetaDataEntry(succ3.getIP(), succ3.getPort(), info.getRange());
        this.replicateData(succ1_server, moveInfo.toBytes());      
        
        // send the new meta data
        sendMetaData();
        
        // add a new node
        this.addNode(Integer.toString(server.getCacheSize()), server.getDisplacementStrategy());
        
        LOGGER.info("ECS: added new node");
    }
    
    /**
     * removes a server and restores the replication invariant
     * @param ip IP of the server
     * @param port Port of the server
     * @param id internal ID of the server
     * @return indicates success
     */    
    private boolean removeNodeFinal_replicate(String ip, int port, int id) {
        if (servers.isEmpty())
            return false;
        
        Server requestedServer = getServer(ip, port);
        if(requestedServer == null)
            return false;
        
        ArrayList<Server> toRemove = new ArrayList<>();
        toRemove.add(requestedServer);
        
        boolean result;
        MetaDataEntry info;
        
        // get 2 predecessors
        MetaDataEntry pred1 = metaData.getPredecessor(ip, port);
        MetaDataEntry pred2 = metaData.getPredecessor(pred1.getIP(), pred1.getPort());
        
        // get 2 successors
        MetaDataEntry succ1 = metaData.getSuccessor(ip, port);
        MetaDataEntry succ2 = metaData.getSuccessor(succ1.getIP(), succ1.getPort());        
                
        result = 
                (info = removeFromMeta(requestedServer)) != null && // remove from metaData and rehash
                sendMetaData() && // sends new meta Data to everyone
                executeGroupCommand(new AdminMessage(AdminType.SHUT_DOWN), toRemove); // shuts down the server
                
        if(result) {
            servers.remove(requestedServer); // remove from server list
            availableServers.get(id).setRunning(false);
            
            // move data around to restore the replication invariant
            // pred1 to succ2
            Server pred1_server = getServer(pred1.getIP(), pred1.getPort());
            MetaDataEntry moveInfo = new MetaDataEntry(succ2.getIP(), succ2.getPort(), pred1.getRange());
            if (!pred1.getIP().equals(succ2.getIP()) || pred1.getPort() != succ2.getPort())
                this.replicateData(pred1_server, moveInfo.toBytes());

            // pred2 to succ1
            Server pred2_server = getServer(pred2.getIP(), pred2.getPort());
            moveInfo = new MetaDataEntry(succ1.getIP(), succ1.getPort(), pred2.getRange());
            if (!pred2.getIP().equals(succ1.getIP()) || pred2.getPort() != succ1.getPort())
                this.replicateData(pred2_server, moveInfo.toBytes());  

            // move the data that was on the removed server to succ3
            MetaDataEntry succ3 = metaData.getSuccessor(succ2.getIP(), succ2.getPort()); 
            Server succ1_server = getServer(succ1.getIP(), succ1.getPort());
            moveInfo = new MetaDataEntry(succ3.getIP(), succ3.getPort(), info.getRange());
            this.replicateData(succ1_server, moveInfo.toBytes());          
            
            return true;
        } 
        return false;
    } 
    
    /**
     * creates and launches a new server and restores the replication invariant
     * @param server the server object to be added
     * @return indicates success
     */    
    private boolean addNodeFinal_replicate(Server server) {
        // create an ArrayList with only this server, because we need an ArrayList parameter
        ArrayList<Server> thisServerOnly = new ArrayList<>();
        thisServerOnly.add(server);
                                                          
        boolean result;
        MetaDataEntry info;
        MetaDataEntry moveInfo=null;
        
        // insert the node as usual
        result = 
                (info = insertIntoMeta(server)) != null && // hash and insert into metaData
                executeGroupSshCommand(new AdminMessage(AdminType.PING), thisServerOnly) && // launch new server via ssh
                executeGroupCommand(new AdminMessage(AdminType.START), thisServerOnly) && // start new server
                executeGroupCommand(new AdminMessage(AdminType.META_DATA, this.metaData.toBytes()), thisServerOnly) && // send it the meta Data
                writeLock(getServer(info.getIP(), info.getPort())) && // lock successor
                moveData(getServer(info.getIP(), info.getPort()), (moveInfo = new MetaDataEntry(server.getIP(), server.getPort(), info.getRange())).toBytes()) && // move data
                sendMetaData() && // send meta data update to all servers
                unlockWrite(getServer(info.getIP(), info.getPort())); // unlock successor
        
        if(result) { 
            // adds server to the server list of running servers
            servers.add(server);
            
            // restore the replication invariant
            // get 2 predecessors
            MetaDataEntry pred1 = metaData.getPredecessor(server.getIP(), server.getPort());
            MetaDataEntry pred2 = metaData.getPredecessor(pred1.getIP(), pred1.getPort());

            // get 2 successors
            MetaDataEntry succ1 = metaData.getSuccessor(server.getIP(), server.getPort());
            MetaDataEntry succ2 = metaData.getSuccessor(succ1.getIP(), succ1.getPort());                  
            
            // pred1 to server
            Server pred1_server = getServer(pred1.getIP(), pred1.getPort());
            info = new MetaDataEntry(server.getIP(), server.getPort(), pred1.getRange());
            if (!pred1.getIP().equals(server.getIP()) || pred1.getPort() != server.getPort())
                this.replicateData(pred1_server, info.toBytes());
            
            // pred2 to server
            Server pred2_server = getServer(pred2.getIP(), pred2.getPort());
            info = new MetaDataEntry(server.getIP(), server.getPort(), pred2.getRange());
            if (!pred2.getIP().equals(server.getIP()) || pred2.getPort() != server.getPort())
                this.replicateData(pred2_server, info.toBytes());             
                      
            // remove pred1's data from succ2
            Server succ2_server = getServer(succ2.getIP(), succ2.getPort());
            this.deleteData(succ2_server, pred1.toBytes());
                    
            // remove pred2's data from succ1
            Server succ1_server = getServer(succ1.getIP(), succ1.getPort());
            this.deleteData(succ1_server, pred2.toBytes());
            
            // remove server's data from succ3
            MetaDataEntry succ3 = metaData.getSuccessor(succ2.getIP(), succ2.getPort());
            Server succ3_server = getServer(succ3.getIP(), succ3.getPort());
            this.deleteData(succ3_server, moveInfo.toBytes());
            
            return true;
        }
        return false;
    }
    
    /**
     * Removes a server from the ring.
     * Basically sends Shut Down Command to a single server and updates the metaData.
     * @param ip - given server ip
     * @param port - given server port
     * @param id - position in the availableServers list.
     */
    private boolean removeNodeFinal(String ip, int port, int id) {
        if (servers.isEmpty())
            return false;
        
        Server requestedServer = getServer(ip, port);
        if(requestedServer == null)
            return false;
        
        ArrayList<Server> toRemove = new ArrayList<>();
        toRemove.add(requestedServer);
        
        boolean result;
        MetaDataEntry info;
        
        result = 
                (info = removeFromMeta(requestedServer)) != null && // remove from metaData and rehash
                writeLock(requestedServer) && // write lock the server
                executeGroupCommand(new AdminMessage(AdminType.META_DATA, metaData.toBytes()), toRemove) && // send meta Data
                moveData(requestedServer, info.toBytes()) && // invoke moving
                sendMetaData() && // sends new meta Data to everyone
                executeGroupCommand(new AdminMessage(AdminType.SHUT_DOWN), toRemove); // shuts down the server
                
        // we know if something goes wrong, but we cannot react to an error which would break the system
        
        if(result) {
            servers.remove(requestedServer); // remove from server list
            availableServers.get(id).setRunning(false);
            return true;
        } 
        return false;
    } 
    
    /**
     * Passes a Runnable of type SendingCommand to the threadpool.
     * @param adminMessage - command to be send
     * @param serverGroup - range of servers this command is being send to
     * @return true - every server received the command; false - something went wrong
     */
    private boolean executeGroupCommand(AdminMessage adminMessage, ArrayList<Server> serverGroup) {
        boolean[] confirmationArray = new boolean[serverGroup.size()]; // java's default value for a boolean array is false
        
        
        for(int i = 0; i < confirmationArray.length; i++) {
            executor.execute(new SendingCommand(adminMessage, serverGroup.get(i).getIP(), serverGroup.get(i).getPort(), confirmationArray, i));
        }
        
        return waitForConfirmation(confirmationArray);                              // TODO - it is probably better to return the servers that did not confirm.
    }
    
    /**
     * Passes a Runnable of type SendingCommand to the threadpool. Does not wait for execution.
     * @param adminMessage - command to be send
     * @param serverGroup - range of servers this command is being send to
     */
    private void executeGroupCommand_nowait(AdminMessage adminMessage, ArrayList<Server> serverGroup) {
        boolean[] confirmationArray = new boolean[serverGroup.size()]; // java's default value for a boolean array is false
        
        for(int i = 0; i < confirmationArray.length; i++) {
            executor.execute(new SendingCommand(adminMessage, serverGroup.get(i).getIP(), serverGroup.get(i).getPort(), confirmationArray, i));
        }
    }
    
    /**
     * Passes a Runnable of type SshCommand to the threadpool.
     * @param script - the script being executed
     * @param serverGroup - range of servers which are supposed to send confirmation
     * @return true - every server received the command; false -something went wrong
     */
    private boolean executeGroupSshCommand(AdminMessage adminMessage, ArrayList<Server> serverGroup) {
        boolean[] confirmationArray = new boolean[serverGroup.size()];
        
        
        for(int i = 0; i < confirmationArray.length; i++) {
            executor.execute(new SshCommand(new String[] {"ssh", serverGroup.get(i).getIP(), "cd " + path + "; java -jar ms3-server.jar " 
                    + serverGroup.get(i).getPort() + " " + serverGroup.get(i).getCacheSize() + " " + serverGroup.get(i).getDisplacementStrategy()}, 
                    adminMessage, serverGroup.get(i).getIP(), serverGroup.get(i).getPort(), confirmationArray, i));
        }
        
        return waitForConfirmation(confirmationArray);
    }
    
    /**
     * Waits till every element of the confirmation array is set to true.
     * (could wait a certain time only)
     * @param confirmationArray
     * @return true - everything set to true; false - some element is still false
     */
    private boolean waitForConfirmation(boolean[] confirmationArray) {
        boolean globalResponse = false;
        
        while(!globalResponse) {
            globalResponse = true;
            
            // globalResponse AND every single confirmation from the confirmation array is calculated
            for(boolean confirmationElement : confirmationArray) 
                globalResponse &= confirmationElement;
        }
        
        return globalResponse;
    }
       
    /**
     * Launches the servers via an SSH call. 
     */
    private boolean launchServers() {
        return executeGroupSshCommand(new AdminMessage(AdminType.PING), this.servers);
    }    
    
    /**
     * creates a metaData object out of a List of servers.
     * @param servers - list of servers
     * @return metaData containing the servers
     */
    private MetaData createMetaData(ArrayList<Server> servers) {
        MetaData meta = new MetaData();
        
        for(Server server : servers){
            meta.insertServer(server.getIP(), server.getPort());
        }
       
        return meta;
    }
    
    /**
     * Sends the current metaData to all servers.
     * @return success indicator
     */
    private boolean sendMetaData() {         
        AdminMessage am = new AdminMessage(AdminType.META_DATA, this.metaData.toBytes());
        return executeGroupCommand(am, this.servers);
    }   
    
    /**
     * Inserts server into the current metaData
     * @param server
     * @return success indicator
     */
    private MetaDataEntry insertIntoMeta(Server server) {
        return this.metaData.insertServer(server.getIP(), server.getPort());
    }
    
    /**
     * Removes server from the current metaData
     * @param server
     * @return success indicator
     */
    private MetaDataEntry removeFromMeta(Server server) {
        return this.metaData.removeServer(server.getIP(), server.getPort());
    }
    
    /**
     * Sends writeLock command to the given server.
     * @param server - given server
     * @return success indicator
     */
    public boolean writeLock(Server server) {
        ArrayList<Server> toLock = new ArrayList<>();
        toLock.add(server);
        return executeGroupCommand(new AdminMessage(AdminType.LOCK_WRITE), toLock);
    }
    
    /**
     * Sends unlock command to the given server
     * @param server - given server
     * @return success indicator
     */
    public boolean unlockWrite(Server server) {
        ArrayList<Server> toUnlock = new ArrayList<>();
        toUnlock.add(server);
        return executeGroupCommand(new AdminMessage(AdminType.UNLOCK_WRITE), toUnlock);
    }
    
    /**
     * Sends moveData command to the given server.
     * @param server - given server
     * @param payload - a metaData entry containing the range to be moved and the server it should be moved to.
     * @return success indicator
     */
    private boolean moveData(Server server, byte[] payload) {
        ArrayList<Server> checkYourRange = new ArrayList<>();
        checkYourRange.add(server);
        return executeGroupCommand(new AdminMessage(AdminType.MOVE_DATA, payload), checkYourRange);
    }   
    
    /**
     * Sends replicateData command to the given server.
     * @param server - given server
     * @param payload - a metaData entry containing the range to be moved and the server it should be moved to.
     * @return success indicator
     */
    private boolean replicateData(Server server, byte[] payload) {
        ArrayList<Server> checkYourRange = new ArrayList<>();
        checkYourRange.add(server);
        return executeGroupCommand(new AdminMessage(AdminType.REPLICATE_DATA, payload), checkYourRange);
    }    
    
    /**
     * Sends deleteData command to the given server.
     * @param server - given server
     * @param payload - a metaData entry containing the range to be deleted.
     * @return success indicator
     */
    private boolean deleteData(Server server, byte[] payload) {
        ArrayList<Server> checkYourRange = new ArrayList<>();
        checkYourRange.add(server);
        return executeGroupCommand(new AdminMessage(AdminType.DELETE_DATA, payload), checkYourRange);
    }      
    
    /**
     * Looks up server Element from running server list.
     * @param ip - ip of lookup
     * @param port - port of lookup
     * @return - server object or null, if not found
     */
    public Server getServer(String ip, int port) {
        for(Server server : servers) {
            if(server.getIP().equals(ip) && server.getPort() == port)
                return server;
        }
        return null;
    }
    
    /**
     * Is the server stopped?
     * @return 
     */
    public boolean getStop() {
        return this.stop;
    }
    
    /**
     * sets stop to given value
     * @param stop - given value
     */
    public void setStop(boolean stop) {
        this.stop = stop;
    }
    
    /**
     * 
     * @return whether the user console for the ecs has to be locked
     */
    public boolean getLocked(){
        return this.locked;
    }
    
    /**
     * Lock/ unlock the user console for the ecs
     * @param value 
     */
    public void setLocked(boolean value){
        this.locked = value;
    }
    
    /**
     * @return boolean indicator, if the service is running.
     */
    public boolean getServiceRunning() {
        return this.serviceRunning;
    }
    
    /**
     * Sets service to given boolean
     * @param serviceRunning 
     */
    public void setServiceRunning(boolean serviceRunning) {
        this.serviceRunning = serviceRunning;
    }
    
    /**
     * @return random number between 0 and availableServers.size() 
     */
    private int getRandomServerID() {
        Random rand = new Random();
        return rand.nextInt(availableServers.size());
    }
    
    /**
     * Checks if one Server is running.
     * @return indicator if this is the case.
     */
    public boolean oneServerRunning() {
        for(ServerStatus status : availableServers) 
            if(status.getRunning()) return true;
        
        return false;
    }
    
    /**
     * @return A random server which is running.
     */
    private int getRandomRunning() {
        while(true) {
            int candidate = getRandomServerID();
            if(availableServers.get(candidate).getRunning())
                return candidate;
        }
    }
   
    /**
     * Sets the server in availableServers to running.
     * @param server - server in question
     */
    private void setServerToX(Server server, boolean X) {
        for(ServerStatus status: availableServers) {
            if(!status.getRunning() && status.getIp().equals(server.getIP()) && status.getPort() == server.getPort()) {
                status.setRunning(X);
                return;
            }
        }
    }
    
    /**
     * Creates Server objects by using the given cacheSize and displacementStrategy
     * and by randomly choosing ip addresses and ports of servers to launch. 
     * @param numberOfNodes the number of servers to create
     * @param cacheSize the case size of the nodes to create
     * @param displacement the displacement strategy of the nodes to create
     */
    private ArrayList<Server> createServerEntries(int numberOfNodes, int cacheSize, String displacement){
        ArrayList<Server> servers = new ArrayList<>();
        
        // availableServers already initiated
        // what if numberOfNodes > availableServers.length?
        
        Hashing hashing = new Hashing();
        int started = 0;
        
        
        // always start the first server
        ServerStatus alwaysRunning = availableServers.get(0);
        servers.add(new Server(alwaysRunning.getIp(), alwaysRunning.getPort(), cacheSize, displacement, 
                hashing.hash(alwaysRunning.getIp() + ":" + alwaysRunning.getPort())));
        availableServers.get(0).setRunning(true);
        started++;
        
        int i = 0;
        while(started < numberOfNodes) {                                                            
            i = getRandomServerID();
            
            if(!availableServers.get(i).getRunning()) { // random server not running
                servers.add(new Server(availableServers.get(i).getIp(), availableServers.get(i).getPort(), cacheSize, displacement, 
                        hashing.hash(availableServers.get(i).getIp() + ":" + availableServers.get(i).getPort()))); 
                started++;
                availableServers.get(i).setRunning(true); // sets added servers to running
            }
        }        
        return servers;
        // where do we set the servers to running?
    }
	
    /**
     * 
     * @return a list containing all servers
     */
    public ArrayList<Server> getServers() { // for tests only
	return servers;
    }
	
    /**
     * stop a single server. used for debugging only.
     * @param ip IP of the server
     * @param port Port of the server
     * @return success
     */
    public boolean stopServer(String ip, int port) {
        ArrayList<Server> toStop = new ArrayList<Server>();
        toStop.add(getServer(ip, port));
        return executeGroupCommand(new AdminMessage(AdminType.STOP), toStop);
    }
    
}
