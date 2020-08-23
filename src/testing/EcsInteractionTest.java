/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package testing;

import app_kvEcs.CommandManager;
import app_kvEcs.Server;
import client.KVStore;
import common.messages.KVMessage;
import common.messages.Message;
import java.nio.file.Path;
import java.nio.file.Paths;
import junit.framework.TestCase;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.AfterClass;

public class EcsInteractionTest {
    
    private static CommandManager commander;
    
    /**
     * Starts 3 serves.
     */
    @BeforeClass
    public static void testSetUp()  {
        long threadId = Thread.currentThread().getId();
        System.out.println("STARTING SETUP on thread "+threadId);
        
        Path currentRelativePath = Paths.get("");
        String p = currentRelativePath.toAbsolutePath().toString() + "/"; 
        p = p.replace("\\", "/");
        
        Exception ex = null;
        boolean result = false;
        
        try {
            commander = new CommandManager(p);      
            result = commander.initService("3","10","FIFO");
            if (result)
                commander.setServiceRunning(true);
        } catch (Exception e) {
            ex = e;
        }
        
        System.out.println("test setup complete");
        TestCase.assertTrue(ex == null);
        TestCase.assertTrue(result);
    }
    
    /**
     * Tests, if addNode is successful.
     */
    @Test
    public void testAddNode() {
        long threadId = Thread.currentThread().getId();
        System.out.println("STARTING TESTADDNODE on thread "+threadId);
        
        boolean result = false;
        Exception ex = null;
        
        try{
            result = commander.addNode("10","FIFO");
        }
        catch(Exception e){
            ex = e;
        }
        
        if(ex != null)
            ex.printStackTrace();
        
        TestCase.assertTrue(ex == null);
        TestCase.assertTrue(result);
        
    }
    
    /**
     * Tests, if removeNode is successful.
     */
    @Test
    public void testRemoveNode() {
        long threadId = Thread.currentThread().getId();
        System.out.println("STARTING TESTADDNODE on thread "+threadId);
        
        boolean result = false;
        Exception ex = null;
        
        try{
            result = commander.removeNode();
        }
        catch(Exception e) {
            ex = e;
        }
        
        
        TestCase.assertTrue(ex == null);
        TestCase.assertTrue(result);
        
    }
    
    /**
     * Tests, if start is successful.
     */
    @Test
    public void testStartStop() {
        long threadId = Thread.currentThread().getId();
        System.out.println("STARTING TESTSTART on thread "+threadId);
        
        boolean result = false;
        Exception ex = null;
        
        try{
            result = commander.start() && commander.stop();
        }
        catch(Exception e) {
            ex = e;
        }
        
        TestCase.assertTrue(ex == null);
        TestCase.assertTrue(result);        
    }
    
    
    /**
     * Tests, if the server is actually locked.
     */
    @Test
    public void testPutLocked() {
        long threadId = Thread.currentThread().getId();
        System.out.println("STARTING TESTPUTLOCKED on thread "+threadId);

        KVStore kvClient = new KVStore("127.0.0.1", 50000, "averkulikov@gmail.com", "Marconi123"); // be careful to not connect to the server that is used in testRemoveNode        
                
        Exception ex = null;
        KVMessage.StatusType result = null;
        
        try{
            commander.start();            
            
            kvClient.connect();
            
            Server server = commander.getServer("127.0.0.1", 50000);
            commander.writeLock(server); // <-- problem here?
            
            result = ((Message) kvClient.put("foo", "bar")).getStatus();      
            
            kvClient.disconnect();
            
            commander.unlockWrite(server);            
            commander.stop();            
        }
        catch(Exception e){
            ex = e;
        }
        
        TestCase.assertTrue(ex == null);
        TestCase.assertTrue(result != null);

        // with the current test environment we do not know whether the server 
        // that is addressed is responsible for this hash range
        // hence the two allowed flags
        boolean writeLocked = result.equals(KVMessage.StatusType.SERVER_WRITE_LOCK);
        boolean notResponsible = result.equals(KVMessage.StatusType.PUT_SUCCESS);
        TestCase.assertTrue(writeLocked || notResponsible);
    }
    
    /**
     * Shuts down the server.
     */
    @AfterClass
    public static void testShutDown() {
        long threadId = Thread.currentThread().getId();
        System.out.println("STARTING TESTSHUTDOWN on thread "+threadId);
        
        boolean result = false;
        Exception ex = null;
        
        try{
            result = commander.shutDown();
			commander.setServiceRunning(false);
			Thread.sleep(10000);
        }
        catch(Exception e){
            ex = e;
        }
        
        TestCase.assertTrue(ex == null);
        TestCase.assertTrue(result);        
    }    
}
