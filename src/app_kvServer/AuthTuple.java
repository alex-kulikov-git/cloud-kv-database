/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package app_kvServer;

/**
 *
 * 
 */
public class AuthTuple {
    private final int HASHLENGTH = 16; // or whatever
    private String user;
    private byte[] pwHash;
    
    
    public AuthTuple(byte[] raw) {
        byte[] userByte = new byte[raw.length - HASHLENGTH];
        byte[] pwByte = new byte[HASHLENGTH];
        
        System.arraycopy(raw, 0, userByte, 0, raw.length - HASHLENGTH);
        System.arraycopy(raw, raw.length - HASHLENGTH, pwByte, 0, HASHLENGTH);
        
        this.user = new String(userByte);
        this.pwHash = pwByte;
    }
    
    public AuthTuple(String user, byte[] pwHash) {
        this.user = user;
        this.pwHash = pwHash;
    }
    
    public String getUser() {
        return this.user;
    }
    
    public byte[] getPwHash() {
        return pwHash;
    }
}
