/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package app_kvEcs;

/**
 *
 * @author kajo
 */
public class ServerStatus {
    private String ip;
    private int port;
    private boolean running;
    
    public ServerStatus(String ip, int port, boolean running) {
        this.ip = ip;
        this.port = port;
        this.running = running;
    }
    
    public String getIp() {
        return this.ip;
    }
    
    public int getPort() {
        return this.port;
    }
    
    public boolean getRunning() {
        return this.running;
    }
    
    public void setRunning(boolean running) {
        this.running = running;
    }
}
