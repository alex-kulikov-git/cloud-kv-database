package testing;

import app_kvEcs.CommandManager;
import client.KVStore;
import common.logger.Constants;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import java.nio.file.Path;
import java.nio.file.Paths;
import junit.framework.TestCase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
//import org.junit.TestCase;
//import junit.framework.Test;
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.AfterClass;
//import org.junit.Before;

public class InteractionTest {
    
    private static final Logger LOGGER = LogManager.getLogger(Constants.TEST_NAME);

    private static CommandManager commander;
    private static KVStore kvClient;

    /**
     * Starts one server.
     */
    @BeforeClass
    public static void testSetUp()  {
        long threadId = Thread.currentThread().getId();
        System.out.println("STARTING SETUP on thread "+threadId);
        LOGGER.info("Interaction Test Setup started");
        
        Path currentRelativePath = Paths.get("");
        String p = currentRelativePath.toAbsolutePath().toString() + "/"; 
        p = p.replace("\\", "/");
        
        Exception ex = null;
        boolean result = false;
        
        kvClient = new KVStore("127.0.0.1",50000, "averkulikov@gmail.com", "Marconi123");
        
        try {
            commander = new CommandManager(p);      
            result = commander.initService("1","10","FIFO");
            if (result)
                commander.setServiceRunning(true);
            result &= commander.start();
            
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }
        
        System.out.println("test setup complete");
        LOGGER.info("Interaction Test Setup completed");
        TestCase.assertTrue(ex == null);
        TestCase.assertTrue(result);
    }   

    /**
     * Tests put.
     */
    @Test
    public void testPut() {
        long threadId = Thread.currentThread().getId();
        System.out.println("STARTING TESTPUT on thread "+threadId);
        LOGGER.info("Interaction Test testPut started");
        // we use the key foo2 here, because foo might have been inserted by testGet()
        
        //KVStore kvClient = new KVStore("127.0.0.1",50000);
        
        String key = "foo2";
        String value = "bar";
        KVMessage response = null;
        Exception ex = null;

        try {
            //kvClient.connect();
            response = kvClient.put(key, value);
            //kvClient.disconnect();
        } catch (Exception e) {
            ex = e;
        }

        System.out.println("ENDING TESTPUT, response is "+response.getStatus());
        TestCase.assertTrue(ex == null);
        TestCase.assertTrue(response.getStatus() == StatusType.PUT_SUCCESS);
        LOGGER.info("Interaction Test testPut completed");
    }
    

    /**
     * Do we catch put, if the client is not connected?
     */
    @AfterClass
    public static void testPutDisconnected() {
        long threadId = Thread.currentThread().getId();
        System.out.println("TRYING TO DISCONNECT on thread "+threadId);
        
        //KVStore kvClient = new KVStore("127.0.0.1",50000);
        kvClient.disconnect();
        
        String key = "foo";
        String value = "bar";
        Exception ex = null;
        
        try {
            kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

        TestCase.assertNotNull(ex);
        
        // shut down
        boolean result = false;
        ex = null;
        
        try {     
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

    /**
     * Are keys being updated?
     */
    @Test
    public void testUpdate() {
        long threadId = Thread.currentThread().getId();
        System.out.println("STARTING TESTUPDATE on thread "+threadId);
        
        //KVStore kvClient = new KVStore("127.0.0.1",50000);
        
        String key = "updateTestValue";
        String initialValue = "initial";
        String updatedValue = "updated";

        KVMessage response = null;
        Exception ex = null;

        try {
            //kvClient.connect();
            kvClient.put(key, initialValue);
            response = kvClient.put(key, updatedValue);
            //kvClient.disconnect();

        } catch (Exception e) {
            ex = e;
        }

        TestCase.assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE
                && response.getValue().equals(updatedValue));
    }

    /**
     * Are keys being deleted?
     */
    @Test
    public void testDelete() {
        long threadId = Thread.currentThread().getId();
        System.out.println("STARTING TESTDELETE on thread "+threadId);

        //KVStore kvClient = new KVStore("127.0.0.1",50000);
        
        String key = "deleteTestValue";
        String value = "toDelete";

        KVMessage response = null;
        Exception ex = null;

        try {
            //kvClient.connect();
            kvClient.put(key, value);
            response = kvClient.put(key, "null");
            //kvClient.disconnect();

        } catch (Exception e) {
            ex = e;
        }

        TestCase.assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
    }

    /**
     * Does the get command work?
     */
    @Test
    public void testGet() {
        long threadId = Thread.currentThread().getId();
        System.out.println("STARTING TESTGET on thread "+threadId);
        

        //KVStore kvClient = new KVStore("127.0.0.1",50000);
        
        String key = "foo";
        String value = "bar";
        KVMessage response = null;
        Exception ex = null;

        try {
            //kvClient.connect();
            kvClient.put(key, value);
            response = kvClient.get(key);
            //kvClient.disconnect();
        } catch (Exception e) {
            ex = e;
        }

        System.out.println("ENDING TESTGET");
        TestCase.assertTrue(ex == null && response.getValue().equals("bar"));
    }

    /**
     * Does the server properly reply, if the key is not found on the server?
     */
    @Test
    public void testGetUnsetValue() {
        long threadId = Thread.currentThread().getId();
        System.out.println("STARTING TESTGETUNSET on thread "+threadId);
        
        //KVStore kvClient = new KVStore("127.0.0.1",50000);
        
        String key = "an unset value";
        KVMessage response = null;
        Exception ex = null;

        try {
            //kvClient.connect();
            response = kvClient.get(key);
            //kvClient.disconnect();
        } catch (Exception e) {
            ex = e;
        }

        TestCase.assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
    }
}
