/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package common.hashing;

import common.logger.Constants;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author blueblastindustries
 */
public class Hashing {
    
    private static final Logger logger = LogManager.getLogger(Constants.CLIENT_NAME);
    
    private MessageDigest md; 
    
    /**
     * CONSTRUCTOR
     */
    public Hashing(){
        try{
            md = MessageDigest.getInstance("MD5");
        }
        catch(NoSuchAlgorithmException e){
            logger.error("MD5 is not a valid hashing algorithm. ");
        }
    }
    
    /**
     * Hashes the given String key and returns the hash value in form of a byte array
     * 
     * @param key
     * @return 
     */
    public byte[] hash(String key){                          //TODO: The two methods are duplicates - we use both in different places. Delete one of them. 
        md.update(key.getBytes(StandardCharsets.UTF_8));
        return md.digest();
    }
    
    public static byte[] getHashValue(String key) {
        MessageDigest md5digest = null;
        try{
            md5digest = MessageDigest.getInstance("MD5");
        }
        catch(NoSuchAlgorithmException e){
            logger.error("MD5 is not a valid hashing algorithm. ");
        }
        md5digest.update(key.getBytes(StandardCharsets.UTF_8));
        return md5digest.digest();
    }   
    
    
}
