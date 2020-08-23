package cache;

import common.messages.KVMessage.StatusType;
import java.util.Set;
import java.util.Map;

public interface Cache {
    /**
     *
     * @return the number of elements in the cache
     */
    public int size();
    
    /**
     * insert a KV-tuple into the cache or update if it is already there
     * @param key the key to be inserted
     * @param value the value to be inserted
     * @param is_dirty indicates if the new cache entry is dirty
     * @return PUT_SUCCESS, UPDATE_SUCCESS, DELETE_SUCCESS or PUT_ERROR
     */
    public StatusType put(String key, String value, Boolean is_dirty);
    
    /**
     * get the corresponding value to a given key
     * @param key the given key
     * @return the value or null
     */
    public String get(String key);
    
    /**
     * replace KV-tuple in the cache with a given new one
     * which tuple is being replaced depends on the chosen strategy
     * this assumes that the cache is non-empty!
     * @param key the key to be inserted
     * @param value the value to be inserted
     * @param is_dirty indicates if the new cache entry is dirty
     * @return the cache entry that was replaced
     */
    public CacheEntry replace(String key, String value, Boolean is_dirty);
    
    /**
     * check if the cache contains a given key
     * @param key the key to be checked
     * @return true if the cache contains the key
     */
    public Boolean containsKey(String key);
    
    /**
     * 
     * @return the cache entry that will be replaced next according to the chosen strategy
     */
    public CacheEntry getFirst();
    
    /**
     * position the iterator at the first element
     * @return the first entry in the cache
     */    
    public CacheEntry iteratorStart();
    
    /**
     * advance the iterator
     * @return the entry that the iterator is positioned at
     */        
    public CacheEntry iteratorNext();
    
    /**
     * remove all elements that are flagged as deleted
     */         
    public void vacuum();
}
