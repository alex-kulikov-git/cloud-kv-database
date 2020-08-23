/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package manager;

import client.KVStore;
import common.constants.PortOffset;
import common.hashing.Range;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.Iterator;
import java.util.Map;
import common.messages.SubscriptionEntry;
import java.util.function.Predicate;

/**
 *
 * @author Andreas
 */
public class SubscriptionManager {
    private final HashMap< String, ArrayList<SubscriptionEntry> > indexMap = new HashMap<>();   
    private Range delete_range = null;
    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    
    /**
     * add a subscription
     * @param key the key that is being subscribed to
     * @param address the e-mail address of the subscriber
     */
    public void addSubscription (String key, String address) {
        rwl.writeLock().lock();
        try {
            ArrayList<SubscriptionEntry> sublist = indexMap.get(key);
            if (sublist == null) {
                sublist = new ArrayList<SubscriptionEntry>();
                sublist.add( new SubscriptionEntry(key,address) );
                indexMap.put(key, sublist);
            }
            else {
                sublist.add( new SubscriptionEntry(key,address) );
            }
        }
        finally {
            rwl.writeLock().unlock();
        }
    }
    
    /**
     * transfers subscriptions to keys in a given range to a given server
     * @param range the range of keys to be moved
     * @param kvClient the KVStore object used for data transfer
     */
    public void moveData(Range range, KVStore kvClient) {
        rwl.readLock().lock();
        try {
            for (Map.Entry<String, ArrayList<SubscriptionEntry>> entry : indexMap.entrySet()) {
                String key = entry.getKey();
                if (!range.withinRange(key))
                    continue;

                ArrayList<SubscriptionEntry> sublist = entry.getValue();
                for (SubscriptionEntry e : sublist) {
                    kvClient.subscribe(key, e.getAddress());
                }
            }   
            
            // mark for later deletion
            delete_range = range;
        }
        finally {
            rwl.readLock().unlock();
        }
    }
    
    /**
     * removes all keys that were previously moved to a different server
     */
    public void vacuum() {
        if (delete_range == null)
            return;
        
        rwl.writeLock().lock();
        try {
            Predicate<String> pred = key -> delete_range.withinRange(key);
            indexMap.keySet().removeIf(pred);
            delete_range = null;
        }
        finally {
            rwl.writeLock().unlock();
        }
    }
    
    /**
     *
     * @param key
     * @return a list containing subscribers to the given key
     */
    public ArrayList<SubscriptionEntry> getSubscriptions(String key) {
        return indexMap.get(key);
    }
    
    /**
     *
     * @param key
     * @return if at least one user is subscribed to the given key
     */
    public boolean isSubscribedTo(String key) {
        return indexMap.containsKey(key);
    }
    
    /**
     * unsubscribes a user from a key
     * @param key the given key
     * @param address the e-mail address of the user
     */
    public void removeSubscription(String key, String address) {
        rwl.writeLock().lock();
        try {
            ArrayList<SubscriptionEntry> subs = getSubscriptions(key);
            if (subs == null)
                return;
        
            Predicate<SubscriptionEntry> pred = e -> e.getAddress().equals(address);
            subs.removeIf(pred);
            if (subs.isEmpty())
                indexMap.remove(key);      
        }
        finally {
            rwl.writeLock().unlock();
        }
    }
}
