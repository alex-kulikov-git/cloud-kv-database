package cache;

import common.messages.KVMessage.StatusType;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.HashMap;
import java.util.Set;
import java.util.function.Predicate;

/**
 * We implemented the FIFO-cache using a queue
 * The queue is represented by an LinkedList
 * A HashMap is additionally used to provide fast lookup
 */   
public class FIFOCache implements Cache {
    /**
     *
     * @param s the maximum number of elements the cache can store
     */     
    public FIFOCache(int s) {
        maxSize = s;
    }
    
    private final LinkedList<CacheEntry> queue = new LinkedList<CacheEntry>();
    private final HashMap<String,CacheEntry> indexMap = new HashMap<String,CacheEntry>();
    private int maxSize;
    private ListIterator<CacheEntry> itr;
    
    @Override 
    public void vacuum() {
        Predicate<CacheEntry> pred = e-> e.isDeleted();
        queue.removeIf(pred);  
            
        indexMap.values().removeIf(pred);
    }    
    
    @Override
    public CacheEntry iteratorStart() {
        itr = queue.listIterator();
        return iteratorNext();
    }
    
    @Override
    public CacheEntry iteratorNext() {
        if (!itr.hasNext())
            return null;
        return itr.next();
    }
    
    @Override
    public int size() {
        return queue.size();
    }
    
    @Override
    public StatusType put(String key, String value, Boolean is_dirty) {
        // check if key is in the cache
        CacheEntry tuple = indexMap.get(key);
        if (tuple != null) {
            tuple.setValue(value);
            return StatusType.PUT_UPDATE;
        }
        
        // check if cache is full
        if (indexMap.size() == maxSize)
            return StatusType.PUT_ERROR;
        
        // insert
        tuple = new CacheEntry(key,value,is_dirty);
        indexMap.put(key, tuple);
        queue.addLast(tuple);
        return StatusType.PUT_SUCCESS;
    }
    
    @Override
    public CacheEntry replace(String key, String value, Boolean is_dirty) {
        // remove from queue
        CacheEntry replaced_tuple = queue.removeFirst();
        
        // remove from map
        indexMap.remove(replaced_tuple.getKey());
        
        // insert new tuple
        put(key, value, is_dirty);
        return replaced_tuple;
    }  
    
    @Override
    public String get(String key) {
        CacheEntry tuple = indexMap.get(key);
        if (tuple == null)
            return null;
        return tuple.getValue();
    } 
    
    @Override
    public Boolean containsKey(String key) {
        return indexMap.containsKey(key);
    }
    
    @Override
    public CacheEntry getFirst() {
        return queue.peekFirst();
    }
   
}
