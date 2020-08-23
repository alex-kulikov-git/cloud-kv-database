/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package testing;

import app_kvEcs.CommandManager;
import client.KVStore;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import junit.framework.TestCase;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * We have not debugged the performance tests yet. But this is how we imagined them.
 * 
 */
public class PerformanceTest {
    
    
    private static CommandManager commander;
    private static String[] set; 
    private static ExecutorService executor;
    
    @BeforeClass
    public static void testSetUp()  {
        executor = Executors.newCachedThreadPool();
        set = new String[10];

        for(int i = 0; i < set.length; i++) {
            String key = "";
            key += i;
            String value = "Apfelsaft";
            set[i] = key + " " + value;
        }
		
        long threadId = Thread.currentThread().getId();
        System.out.println("STARTING SETUP on thread "+threadId);
        
        Path currentRelativePath = Paths.get("");
        String p = currentRelativePath.toAbsolutePath().toString() + "/"; 
        p = p.replace("\\", "/");
        
        Exception ex = null;
        
        try {
            commander = new CommandManager(p);    
        } catch (Exception e) {
            ex = e;
        }
        
        System.out.println("test setup complete");
        TestCase.assertTrue(ex == null);
    }
    
  
    
    @Test 
    public void client1_server3() {
        SinglePerformanceTest test = new SinglePerformanceTest(commander, set, 1, 3);
        TestCase.assertTrue(test.testAndWait());
    }
    
    @Test
    public void client1_server5() {
        SinglePerformanceTest test = new SinglePerformanceTest(commander, set, 1, 5);
        TestCase.assertTrue(test.testAndWait());
    }
    
    @Test
    public void client1_server10() {
        SinglePerformanceTest test = new SinglePerformanceTest(commander, set, 1, 10);
        TestCase.assertTrue(test.testAndWait());
    }
    
    @Test
    public void client5_server3() {
        SinglePerformanceTest test = new SinglePerformanceTest(commander, set, 5, 3);
        TestCase.assertTrue(test.testAndWait());
    }
    
    @Test
    public void client5_server5() {
        SinglePerformanceTest test = new SinglePerformanceTest(commander, set, 5, 5);
        TestCase.assertTrue(test.testAndWait());
    }
    
    @Test
    public void client5_server10() {
        SinglePerformanceTest test = new SinglePerformanceTest(commander, set, 5, 10);
        TestCase.assertTrue(test.testAndWait());
    }    
    
    @Test
    public void client10_server3() {
        SinglePerformanceTest test = new SinglePerformanceTest(commander, set, 10, 3);
        TestCase.assertTrue(test.testAndWait());
    }
    
    @Test
    public void client10_server5() {
        SinglePerformanceTest test = new SinglePerformanceTest(commander, set, 10, 5);
        TestCase.assertTrue(test.testAndWait());
    }
    
    @Test
    public void client10_server10() {
        SinglePerformanceTest test = new SinglePerformanceTest(commander, set, 10, 10);
        TestCase.assertTrue(test.testAndWait());
    }    
}
