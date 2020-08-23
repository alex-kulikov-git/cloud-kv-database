package testing;

import app_kvEcs.CommandManager;
import client.KVStore;
import junit.framework.TestCase;

import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.AfterClass;


public class ConnectionTest  {

    private static CommandManager commander;
    
    /**
     * Starts a server.
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
            result = commander.initService("1","10","FIFO");
            if (result)
                commander.setServiceRunning(true);
            result &= commander.start();
        } catch (Exception e) {
            ex = e;
        }
        
        System.out.println("test setup complete");
        TestCase.assertTrue(ex == null);
        TestCase.assertTrue(result);
    }    
    
    /**
     * Tests, if one can connect to the server.
     */
    @Test
    public void testConnectionSuccess() {

        Exception ex = null;

        KVStore kvClient = new KVStore("127.0.0.1", 50000, "averkulikov@gmail.com", "Marconi123"); // does this die?
        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        TestCase.assertNull(ex);
    }

    /**
     * Tests, if an exception is thrown, if the Host is unknown.
     */
    @Test
    public void testUnknownHost() {
        Exception ex = null;
        KVStore kvClient = new KVStore("1285.0.0.1", 50000, "averkulikov@gmail.com", "Marconi123"); // does this die?

        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        TestCase.assertTrue(ex instanceof UnknownHostException);
    }

    /**
     * Tests, if the client recognizes an illegal port.
     */
    @Test
    public void testIllegalPort() {
        Exception ex = null;
        KVStore kvClient = new KVStore("127.0.0.1", 123456789, "averkulikov@gmail.com", "Marconi123"); // does this die?

        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        TestCase.assertTrue(ex instanceof IllegalArgumentException);
    }
    
    /**
     * Shuts down the server.
     */
    @AfterClass
    public static void testShutDown()  {
        Exception ex = null;
        boolean result = false;
        
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

}

