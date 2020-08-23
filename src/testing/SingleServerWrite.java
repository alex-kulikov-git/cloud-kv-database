/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package testing;

import client.KVStore;

public class SingleServerWrite implements Runnable{
    
    private final String[] SET;
    private final int keyOffset;
    private final int numberOfEntries;
    private boolean[] confirmationArray;
    private final int myPos;
    private boolean write;
    
    /**
     * CONSTRUCTOR
     * 
     * @param set the set of entries to send to or retrieve from server
     * @param keyOffset the offset needed to make keys unique for each running client
     * @param numberOfEntries the number of entries to write or read from server
     * @param confirmationArray the confirmation array
     * @param myPos this object's position in the confirmation array
     * @param write whether to write or read the given entries to/from server
     */
    public SingleServerWrite(String[] set, int keyOffset, int numberOfEntries, boolean[] confirmationArray, int myPos, boolean write) {
        this.SET = set;
        this.keyOffset = keyOffset;
        this.numberOfEntries = numberOfEntries;
        this.confirmationArray = confirmationArray;
        this.myPos = myPos;
        this.write = write;
    }

    @Override
    public void run() {
        testOneClient(write);
        confirmationArray[myPos] = true;
    }
    
    /**
     * Tests one server for the time it needs to execute n write requests
     */
    private void testOneClient(boolean write){        
        KVStore kvClient = new KVStore("127.0.0.1", 50000, "averkulikov@gmail.com", "Marconi123");
        
        // Testing time for 100 entries
        try {            
            kvClient.connect();
            
            for(int i = 0; i < numberOfEntries; i++){
                String[] pair = SET[keyOffset + i].split("\\s+");
                if(write)
                    kvClient.put(pair[0], pair[1]);
                else 
                    kvClient.get(pair[0]);
            }            
            
            kvClient.disconnect();       
            
        } catch (Exception e) {
            e.printStackTrace();
        }   
    }
    
}
