package testing;

import app_kvEcs.CommandManager;
import client.KVStore;
import common.messages.KVMessage.StatusType;
import common.messages.KVMessage;
import common.messages.Message;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import junit.framework.TestCase;
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import cache.*;

public class AdditionalTest {

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
    
    @Test
    public void testCache() {
        /* we want to test if removing a large amount of values from cache
         (as performed during moveData for instance) works. */
        
        // we start with FIFO cache
        Cache cache = new FIFOCache(10);
        String[] key = new String[10];
        String[] value = new String[10];
                
        // insert 10 tuples
        for (int i = 0; i < 10; i++) {
            key[i] = Integer.toString(i);
            value[i] = Integer.toString(i);
            cache.put(key[i],value[i],false);
        }
        
        // mark half the tuples as deleted
        int j = 0;
        CacheEntry entry = cache.iteratorStart();
        while(entry != null) {
            if ((j%2)==0)
                entry.setDeleted();
            
            entry = cache.iteratorNext();
            j++;
        }
        
        // use vacuum to physically delete the tuples
        cache.vacuum();
        
        // now test if we can still get the remaining tuples
        for (int i = 0; i < 10; i++) {
            String v = cache.get(key[i]);
            
            if ((i%2)==0) {
                TestCase.assertTrue(v == null);
            }
            else {
                TestCase.assertTrue(v != null);
                TestCase.assertTrue(v.equals(value[i]));                
            }
        }
        
        TestCase.assertTrue(cache.size() == 5);
        
        
        
        // now do the same for LRU cache
        cache = new LRUCache(10);
        
        // insert 10 tuples
        for (int i = 0; i < 10; i++) {
            cache.put(key[i],value[i],false);
        }        
        
        // mark half the tuples as deleted
        j = 0;
        entry = cache.iteratorStart();
        while(entry != null) {
            if ((j%2)==0)
                entry.setDeleted();
            
            entry = cache.iteratorNext();
            j++;
        }
        
        // use vacuum to physically delete the tuples
        cache.vacuum();

        // now test if we can still get the remaining tuples
        for (int i = 0; i < 10; i++) {
            String v = cache.get(key[i]);
            
            if ((i%2)==0) {
                TestCase.assertTrue(v == null);
            }
            else {
                TestCase.assertTrue(v != null);
                TestCase.assertTrue(v.equals(value[i]));                
            }
        }
        
        TestCase.assertTrue(cache.size() == 5);


        
        // now do the same for LFU cache
        cache = new LFUCache(10);
        
        // insert 10 tuples
        for (int i = 0; i < 10; i++) {
            cache.put(key[i],value[i],false);
        }        
        
        // mark half the tuples as deleted
        j = 0;
        entry = cache.iteratorStart();
        while(entry != null) {
            if ((j%2)==0)
                entry.setDeleted();
            
            entry = cache.iteratorNext();
            j++;
        }
        
        // use vacuum to physically delete the tuples
        cache.vacuum();
        
        // now test if we can still get the remaining tuples
        for (int i = 0; i < 10; i++) {
            String v = cache.get(key[i]);
            
            if ((i%2)==0) {
                TestCase.assertTrue(v == null);
            }
            else {
                TestCase.assertTrue(v != null);
                TestCase.assertTrue(v.equals(value[i]));                
            }
        }   
        
        TestCase.assertTrue(cache.size() == 5);
    }

    /**
     * Tests the cache manager.
     */
    @Test
    public void testStorage() {
        String[] input_keys = {"1","2","3","4","5","6","7","8","9","10","11","12"};
        String[] input_values = {"a","b","c","d","e","f","g","h","i","j","k","l"};
        Exception ex = null;
        KVMessage response1 = null;
        KVMessage response2 = null;
        
        KVStore kvClient = new KVStore("127.0.0.1", 50000, "averkulikov@gmail.com", "Marconi123");
        
        try {
            kvClient.connect();
            // we use a FIFO cache with max size 10
            // therefore, the elements 1 and 2 should be on disk after this
            for (int i = 0; i < input_keys.length; i++) {
                kvClient.put(input_keys[i], input_values[i]);
            }
            
            // now test if we can still get 1 and 2   
            // it can easily be confirmed that the disk was accessed, since the server logs cache misses
            response1 = kvClient.get(input_keys[0]);
            response2 = kvClient.get(input_keys[1]);
            
            kvClient.disconnect();
        } catch (Exception e) {
            ex = e;
        }       
        
        TestCase.assertTrue(ex == null);
        TestCase.assertTrue(response1.getStatus() == StatusType.GET_SUCCESS &&
                            input_values[0].equals(response1.getValue()) );
        TestCase.assertTrue(response2.getStatus() == StatusType.GET_SUCCESS &&
                            input_values[1].equals(response2.getValue()) );        
    }
    
    /**
     * Tests the new message Format of Message.
     */
     @Test 
    public void testMessage() {
        Message msg_1 = new Message(StatusType.PUT, "key".getBytes(), "value".getBytes());
        
        byte[] keyBytes = "key".getBytes();
        byte[] valueBytes = "value".getBytes();
        byte[] payloadLength = ByteBuffer.allocate(4).putInt(valueBytes.length).array();
        byte[] byteMessage = new byte[2 + keyBytes.length + 4 + valueBytes.length];
        byteMessage[0] = (byte) 4; // status byte
        byteMessage[1] = (byte) keyBytes.length; // key length byte
        System.arraycopy(keyBytes, 0, byteMessage, 2, keyBytes.length); // key
        System.arraycopy(payloadLength, 0, byteMessage, 1 + 1 + (int) byteMessage[1], payloadLength.length); // payloadLength
        System.arraycopy(valueBytes, 0, byteMessage, 1 + 1 + keyBytes.length + 4, valueBytes.length); // payload
       
        
        Message msg_2 = new Message(byteMessage);
        
        byteMessage[0] = (byte) 122; // invalid status
        
        Message msg_3 = new Message(byteMessage);
        
        byteMessage[0] = (byte) 4; // back to valid status
        byteMessage[1] = (byte) 100; // message is smaller than given key length
        
        Message msg_4 = new Message(byteMessage);
        
        TestCase.assertTrue(msg_1.getValid() == true);
        TestCase.assertTrue(msg_2.getValid() == true);
        TestCase.assertTrue(msg_3.getValid() == false);
        TestCase.assertTrue(msg_4.getValid() == false); 
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
