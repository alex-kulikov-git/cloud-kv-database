/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package testing;

import app_kvEcs.CommandManager;
import client.KVStore;
import common.logger.Constants;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import junit.framework.TestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author blueblastindustries
 */
public class ExtensionTest {
    
    private static CommandManager commander;
    
    private static final Logger LOGGER = LogManager.getLogger(Constants.TEST_NAME);
	
    @BeforeClass
    public static void testSetUp() {
        long threadId = Thread.currentThread().getId();
        System.out.println("STARTING SETUP on thread "+threadId);
        
        Path currentRelativePath = Paths.get("");
        String p = currentRelativePath.toAbsolutePath().toString() + "/"; 
        p = p.replace("\\", "/");
        
        Exception ex = null;
        boolean result = false;
        
        // launch 4 servers and error listener
        try {
            commander = new CommandManager(p);
            result = commander.initService("4","10","FIFO");
            if (result)
                commander.setServiceRunning(true); // i dont think it does anything at all
			
        } catch (Exception e) {
            ex = e;
        }
        
        System.out.println("test setup complete");
        TestCase.assertTrue(ex == null);
        TestCase.assertTrue(result);
    }
    
    
    /**
     * Tests whether a KVStore object can successfully connect and authenticate
     * on a specific server. Login data is correct. 
     */
    @Test
    public void testAuthenticateCorrectly(){
        LOGGER.info("TEST AuthenticateCorrectly");
        
        Exception ex = null;
        boolean result = false;
        KVMessage response = null;
        
        commander.start();
        KVStore kvClient = new KVStore("127.0.0.1", 50000, "averkulikov@gmail.com", "Marconi123");
        
        try{
            response = kvClient.connect();
        } catch (IOException ex1) {
            LOGGER.error("Unable to connect in ExtensionTest");
        }
       
        kvClient.disconnect();
        
        commander.stop();
        
        TestCase.assertTrue(ex == null);
        TestCase.assertTrue(response.getStatus().equals(StatusType.AUTH_SUCCESS)); // authentication successful        
    }
    
    /**
     * Tests whether a KVStore object can successfully connect and authenticate
     * on a specific server. Login email is NOT correct. 
     */
    @Test
    public void testAuthenticateWrongEmail(){
        LOGGER.info("TEST AuthenticateWrongEmail");
        
        Exception ex = null;
        boolean result = false;
        KVMessage response = null;
        
        commander.start();
        
        // yahoo.com instead gmail.com
        KVStore kvClient = new KVStore("127.0.0.1", 50000, "averkulikov@yahoo.com", "Marconi123");
        
        try{
            response = kvClient.connect();
        } catch (IOException ex1) {
            LOGGER.error("Unable to connect in ExtensionTest");
        }
       
        kvClient.disconnect();
        
        commander.stop();
        
        TestCase.assertTrue(ex == null);
        TestCase.assertTrue(response.getStatus().equals(StatusType.AUTH_ERROR)); // authentication not successful        
    }
    
    /**
     * Tests whether a KVStore object can successfully connect and authenticate
     * on a specific server. Login password is NOT correct. 
     */
    @Test
    public void testAuthenticateWrongPassword(){
        LOGGER.info("TEST AuthenticateWrongPassword");
        
        Exception ex = null;
        boolean result = false;
        KVMessage response = null;
        
        commander.start();
        // wrong password
        KVStore kvClient = new KVStore("127.0.0.1", 50000, "averkulikov@gmail.com", "wrongPassword");
        
        try{
            response = kvClient.connect();
        } catch (IOException ex1) {
            LOGGER.error("Unable to connect in ExtensionTest");
        }
       
        kvClient.disconnect();
        
        commander.stop();
        
        TestCase.assertTrue(ex == null);
        TestCase.assertTrue(response.getStatus().equals(StatusType.AUTH_ERROR)); // authentication not successful        
    }
    
    /**
     * Creates a KVStore object, which first puts a key-value tuple in the 
     * database, and then subscribes to it. 
     * 
     * After, the client updates the value of the same key in order to check
     * whether we were able to receive an email update. 
     */
    @Test
    public void testSubscribeCorrectly(){
        LOGGER.info("TEST SubscribeCorrectly");
        
        Exception ex = null;
        boolean result = false;
        KVMessage responsePut = null;
        KVMessage responseSub = null;
        
        commander.start();
        KVStore kvClient = new KVStore("127.0.0.1", 50000, "averkulikov@gmail.com", "Marconi123");
        
        try{
            kvClient.connect();
            responsePut = kvClient.put("schluessel", "wert");
            
            responseSub = kvClient.subscribe("schluessel", "averkulikov@gmail.com");        
            
            kvClient.put("schluessel", "neuerWert");
            
        } catch (IOException ex1) {
            LOGGER.error("Unable to connect or to subscribe in ExtensionTest");
        }
       
        kvClient.disconnect();
        
        commander.stop();
        
        TestCase.assertTrue(ex == null);
        TestCase.assertTrue(responsePut.getStatus().equals(StatusType.PUT_SUCCESS) || responsePut.getStatus().equals(StatusType.PUT_UPDATE) || responsePut.getStatus().equals(StatusType.DELETE_SUCCESS));
        TestCase.assertTrue(responseSub.getStatus().equals(StatusType.SUB_SUCCESS));
    }
    
    /**
     * Creates a KVStore object and instantly subscribes to a key. 
     * The key does not yet exist. We expect a subscription error. 
     */
    @Test
    public void testSubscribeIncorrectly(){
        LOGGER.info("TEST SubscribeInCorrectly");
        
        Exception ex = null;
        boolean result = false;
        KVMessage responseSub = null;
        
        commander.start();
        KVStore kvClient = new KVStore("127.0.0.1", 50000, "averkulikov@gmail.com", "Marconi123");
        
        try{
            kvClient.connect();
            
            responseSub = kvClient.subscribe("schluessel", "averkulikov@gmail.com");  
            
        } catch (IOException ex1) {
            LOGGER.error("Unable to connect or to subscribe in ExtensionTest");
        }
       
        kvClient.disconnect();
        
        commander.stop();
        
        TestCase.assertTrue(ex == null);
        TestCase.assertTrue(responseSub.getStatus().equals(StatusType.SUB_ERROR));
    }
    
    /**
     * Creates a KVStore object, which first puts a key-value tuple in the 
     * database, and then subscribes to it. 
     * 
     * After, the client unsubs again. We make another put request to the same key. 
     * Now, we should not receive an update email any longer. 
     */
    @Test
    public void testUnsubCorrectly(){
        LOGGER.info("TEST UnsubCorrectly");
        
        Exception ex = null;
        boolean result = false;
        KVMessage responseSub = null;
        KVMessage responseUnsub = null;
        
        commander.start();
        KVStore kvClient = new KVStore("127.0.0.1", 50000, "averkulikov@gmail.com", "Marconi123");
        
        try{
            kvClient.connect();
            kvClient.put("schluessel", "wert");            
            responseSub = kvClient.subscribe("schluessel", "averkulikov@gmail.com");
            
            responseUnsub = kvClient.unsubscribe("schluessel");
            
            kvClient.put("schluessel", "neuerWert");
            
        } catch (IOException ex1) {
            LOGGER.error("Unable to connect or to subscribe in ExtensionTest");
        }
       
        kvClient.disconnect();
        
        commander.stop();
        
        TestCase.assertTrue(ex == null);
        TestCase.assertTrue(responseSub.getStatus().equals(StatusType.SUB_SUCCESS));
        TestCase.assertTrue(responseUnsub.getStatus().equals(StatusType.SUB_SUCCESS));
    }
    
    
    @AfterClass
    public static void testShutDown()  {
        Exception ex = null;
        boolean result = false;

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
