/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package testing;

import app_kvEcs.CommandManager;
import app_kvEcs.ErrorBuffer;
import app_kvEcs.ErrorListener;
import app_kvEcs.HandleError;
import client.KVStore;
import common.constants.EcsErrorAddress;
import common.logger.Constants;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import java.nio.file.Path;
import java.nio.file.Paths;
import junit.framework.TestCase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for Milestone 4 => Replication and Failure Detection
 */
public class ReplicationTest {
    
    private static CommandManager commander;
	private static final Logger LOGGER = LogManager.getLogger(Constants.TEST_NAME);
    
    private static ErrorListener errorListener;
    private static HandleError handleError;
    
    //private static final Logger LOGGER = LogManager.getLogger(Constants.TEST_NAME);
	
    @BeforeClass
    public static void testSetUp() {
        long threadId = Thread.currentThread().getId();
        System.out.println("STARTING SETUP on thread "+threadId);
        
        Path currentRelativePath = Paths.get("");
        String p = currentRelativePath.toAbsolutePath().toString() + "/"; 
        p = p.replace("\\", "/");
        
        Exception ex = null;
        boolean result = false;
        
        try {
            commander = new CommandManager(p);
            ErrorBuffer buffer = new ErrorBuffer(); 
            (handleError = new HandleError(commander, buffer)).start(); // handles error
            (errorListener = new ErrorListener(EcsErrorAddress.PORT, buffer)).start(); // listens for error
            result = commander.initService("4","10","FIFO");
            if (result)
                commander.setServiceRunning(true);
			
        } catch (Exception e) {
            ex = e;
        }
        
        System.out.println("test setup complete");
        TestCase.assertTrue(ex == null);
        TestCase.assertTrue(result);
    }
    
    @Test
    public void testReplicate() {
        Exception ex = null;
        KVMessage response1 = null;
        KVMessage response2 = null;

        commander.start();
        KVStore kvClient = new KVStore("127.0.0.1", 50000, "averkulikov@gmail.com", "Marconi123");

        String[] key = new String[10];
        String[] value = new String[10];

        for (int i = 0; i < 10; i++) {
            key[i] = "127.0.0.1:"+Integer.toString(50000+i);
            value[i] = "somevalue";
        }

        try{
            // put all the KV-tuples
            kvClient.connect();
            for (int i = 0; i < 10; i++)
                kvClient.put(key[i], value[i]);
            
            // update a value on 127.0.0.1:50000
            kvClient.put(key[0],"updatedvalue");
            
            // now stop 127.0.0.1:50000
            commander.stopServer("127.0.0.1", 50000);
            
            // now test if we can still get key[0]
            response1 = kvClient.get(key[0]);
            
            // put on key[0] should fail now
            response2 = kvClient.put(key[0],"update2");
            
            kvClient.disconnect();
            
        } catch(Exception e) {
            ex = e;
        }
        
        TestCase.assertTrue(ex == null);
        TestCase.assertTrue(response1.getStatus() == StatusType.GET_SUCCESS);
        TestCase.assertTrue(response1.getValue().equals("updatedvalue"));
        TestCase.assertTrue(response2.getStatus() == StatusType.SERVER_STOPPED);
    }
	
    @Test
    public void crashTest() {
        try{
            // crash a server
            commander.crashServer("127.0.0.1", 50000);
    
            // give the system some time to reconciliate
            Thread.sleep(15000);
        } catch(Exception e) {
            e.printStackTrace();
        }
    
        // test if the crashed server was noticed
        TestCase.assertTrue(commander.serverIsDown("127.0.0.1", 50000));	
        
        // there should still be 4 servers running because a new one was started
        TestCase.assertTrue(commander.getServersRunning() == 4);
    }
    
    @AfterClass
    public static void testShutDown()  {
        Exception ex = null;
        boolean result = false;

        errorListener.stopRunning();
        handleError.stopRunning();
        
        try {     
            Thread.sleep(1000);
            result = commander.shutDown();
            commander.setServiceRunning(false);
            Thread.sleep(10000);
        } catch (Exception e) {
            ex = e;
        }
		
        System.out.println("test shutdown complete");
        TestCase.assertTrue(ex == null);
        TestCase.assertTrue(result);
    } 
}
