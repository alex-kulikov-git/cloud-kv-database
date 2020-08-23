/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package common.messages;

/**
 *
 * @author Andreas
 */
public class SubscriptionEntry {
    private String key;
    private String email;
    
    public SubscriptionEntry(String k, String e) {
        key = k;
        email = e;
    }
    
    public String getKey() {
        return key;
    }
    
    public String getAddress() {
        return email;
    }
}
