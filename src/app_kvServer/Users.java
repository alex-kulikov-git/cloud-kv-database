/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package app_kvServer;

import java.util.ArrayList;
import java.util.Arrays;
import common.logger.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Represents user list.
 * It is currently possible two have two identical user ids.
 */
public class Users {
    private static final Logger LOGGER = LogManager.getLogger(Constants.SERVER_NAME);
    ArrayList<AuthTuple> pairs;
    
    public Users() {
        pairs = new ArrayList<>();
    }
       
    public void addPair(String user, byte[] pwHash) {
        pairs.add(new AuthTuple(user, pwHash));
    }
    
    public void addPair(byte[] raw) {
        pairs.add(new AuthTuple(raw));
    }
      
    public AuthTuple getByName(String user) {
        for(AuthTuple tuple : pairs) {
            if(tuple.getUser().equals(user))
                return tuple;
        }
        
        return null;
    }
    
    public void logUsers() {
        for(AuthTuple tuple : pairs) {
            LOGGER.info(tuple.getUser());
        }
    }
	
    public boolean valid(AuthTuple user_pw) { // this fails
        LOGGER.info("Looking for: " + user_pw.getUser());
		
        for(AuthTuple tuple : pairs) {
            if(tuple.getUser().equals(user_pw.getUser()) && Arrays.equals(tuple.getPwHash(), user_pw.getPwHash()))
                return true;
        }
        return false;
    }
}
