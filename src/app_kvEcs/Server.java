/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package app_kvEcs;

/**
 * A class for storing the attributes of one server to make server management ezier. 
 */
public class Server {
   
    private final String ip;
    private final int port; // TWO PORT PROBLEM - SOLVE WITH OFFSET?
    private final int cacheSize;
    private final String displacementStrategy;
    private final byte[] hashPosition;   // This is the hash position of the server in the circle
    
    /**
     * CONSTRUCTOR
     * 
     * Sets the main attributes of the server object: 
     * 
     * @param ip
     * @param port
     * @param cacheSize
     * @param displacement
     * @param hashPosition
     */
    public Server(String ip, int port, int cacheSize, String displacement, byte[] hashPosition) {
        this.ip = ip;
        this.port = port;
        this.cacheSize = cacheSize;
        this.displacementStrategy = displacement;
        this.hashPosition = hashPosition;
    }
    
    public String getIP(){
        return this.ip;
    }
    
    public int getPort(){
        return this.port;
    }
    
    public int getCacheSize(){
        return this.cacheSize;
    }
    
    public String getDisplacementStrategy(){
        return this.displacementStrategy;
    }
    
    public byte[] getHashPosition(){
        return this.hashPosition;
    }
    
    /**
     * 
     * @return the server parameters separated by spaces
     */
    @Override
    public String toString(){
        return this.ip + " " + this.port + " " + this.cacheSize + " " + this.displacementStrategy;
    }
}
