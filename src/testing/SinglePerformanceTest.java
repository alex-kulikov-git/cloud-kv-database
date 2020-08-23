/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package testing;

import app_kvEcs.CommandManager;
import common.logger.Constants;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author kajo
 */
public class SinglePerformanceTest {
    private static final Logger LOGGER = LogManager.getLogger(Constants.TEST_NAME);
    
    String[] set;
    int numberOfClients;
    int numberOfServers;
    CommandManager commander; 
    
    
    public SinglePerformanceTest(CommandManager commander, String[] set, int numberOfClients, int numberOfServers) {
        this.commander = commander; 
        this.set = set;
        this.numberOfClients = numberOfClients;
        this.numberOfServers = numberOfServers;
        
        LOGGER.info("single performance test gestartet");
    }
    
    
    /**
     * Waits till every element of the confirmation array is set to true.
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
    
    
    public boolean testAndWait() {
        long threadId = Thread.currentThread().getId();
        System.out.println("STARTING TESTWRITE with " + numberOfClients + " clients on " + numberOfServers + " servers on thread "+threadId);
        
        // initialize servers
        ExecutorService executor = Executors.newCachedThreadPool();
        
        Exception ex = null; 
        boolean result = false;
        
        try{
            result = commander.initService("" + numberOfServers,"100","FIFO");
            result &= commander.start();
            LOGGER.info("server wurden gestartet");
        }
        catch(Exception e){
            ex = e;
        }
        
        
        // WRITE test
        
        // start time
        long startTime = System.currentTimeMillis();
        
        // create confirmation array
        boolean[] confirmationArray = new boolean[numberOfClients];
        
        // add clients to executor service
        int numberOfEntries = set.length/numberOfClients;
        try{
            for(int i = 0; i < numberOfClients; i++) {
                executor.execute(new SingleServerWrite(set, (numberOfEntries * i), numberOfEntries, confirmationArray, i, true));
            }
            LOGGER.info("clients wurden mit executor gestartet");
        }
        catch(Exception e){
            ex = e;
        }
        
        // call waitForConfirmation
        result &= waitForConfirmation(confirmationArray);
        
        // stop time
        long stopTime = System.currentTimeMillis();
        double duration_write = stopTime - startTime; // in Funktion ausgeben
        displayDuration(duration_write, numberOfEntries);
        
        
        
        // GET test
        for(int i = 0; i < confirmationArray.length; i++) confirmationArray[i] = false;
        
        startTime = System.currentTimeMillis();
        
        try{
            for(int i = 0; i < numberOfClients; i++)
                executor.execute(new SingleServerWrite(set, (numberOfEntries * i), numberOfEntries, confirmationArray, i, false));
        } catch(Exception e) {
            ex = e;
        }
        
        result &= waitForConfirmation(confirmationArray);
        
        stopTime = System.currentTimeMillis();
        double duration_get = stopTime - startTime; // in Funktion ausgeben
        displayDuration(duration_get, numberOfEntries);
        
        
        
        // shut down all servers running
        try{
            result &= commander.shutDown();
            Thread.sleep(10000);
            LOGGER.info("server wurden beendet");
        }
        catch(Exception e){
            ex = e;
        }
                
        return result;
    }
    
    private void displayDuration(double duration, int numberOfEntries) {
        // write time to file/stdout
        System.out.println("Test " + numberOfClients + "clients / " + numberOfServers + "servers sending each" + numberOfEntries + "entries: ");
        
        // write tuples per second
        double durationInSecs = duration/1000;
        double tuplesPerSecond = set.length/durationInSecs;
        
        System.out.println("Tuples: " + set.length + ", duration: " + durationInSecs + " secs => Tuples/sec: " + tuplesPerSecond + "\n");
    }
}
