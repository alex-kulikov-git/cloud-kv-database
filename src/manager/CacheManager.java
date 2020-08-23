package manager;

import cache.LFUCache;
import cache.CacheEntry;
import cache.FIFOCache;
import cache.LRUCache;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import cache.Cache;
import common.messages.KVMessage.StatusType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import common.logger.Constants;
import java.io.IOException;
import common.hashing.Range;
import client.KVStore;
import common.constants.PortOffset;
        
/**
 *
 * @author Andreas
 */
public class CacheManager {    
    private Cache cache;
    private Logger logger;
    private Boolean write_locked;
    private Boolean moved_data;
    private Boolean is_stopped;
    private Boolean is_alive;
    private StorageManager storage_manager;
    private SubscriptionManager sub_manager = null;
    final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    
    /**
     * initializes the cache and disk storage
     * @param maxsize the maximum number of elements in the cache
     * @param type the replacement strategy (LRU, LFU or FIFO)
     * @param port the port that the server is using
     */
    public CacheManager(int maxsize, String type, int port) { 
        logger = LogManager.getLogger(Constants.SERVER_NAME);
        write_locked = false;
        moved_data = false;
        is_stopped = true;
        is_alive = true;
        
        // init storage files
        storage_manager = new StorageManager(port);
        try {
            storage_manager.initClearedStorage();   // TODO: change this?
        }
        catch (IOException ioe) {
            logger.error("Could not create storage files!", ioe);
        }
        
        // init cache
        if (type.equals("LFU"))
            cache = new LFUCache(maxsize);
        else if(type.equals("FIFO"))
            cache = new FIFOCache(maxsize);
        else
            cache = new LRUCache(maxsize);
    }
    
    /**
     *
     * @return the number of elements currently in the cache
     */
    public int size() {
        return cache.size();
    }
    
    /**
     *
     * @return if the storage server is write locked
     */    
    public Boolean isWriteLocked() {
        return write_locked;
    }
    
    /**
     * lock the server so that no client can store or update items
     */        
    public void lockWrite() {
        write_locked = true;
    }
    
    /**
     * remove the write lock
     */       
    public void unLockWrite() {
        if (moved_data) {
            // after moveData() is done, we delete the moved data using vacuum()
            rwl.writeLock().lock();
            try {
                moved_data = false;
                try {
                    // delete from disk storage
                    storage_manager.vacuum();
                }
                catch (IOException e) {
                    logger.error("Exception during vacuum()", e);
                }

                // delete from cache            
                cache.vacuum();
                
                // delete subscriptions
                if (this.sub_manager != null)
                    sub_manager.vacuum();
            }
            finally {
                rwl.writeLock().unlock();
            }
        }
        write_locked = false;
    }     
    
    /**
     * start the server so that requests can be processed
     */      
    public void start() {
        is_stopped = false;
    }

    /**
     * stop the server so that no more requests can be processed
     */          
    public void stop() {
        is_stopped = true;
    }
    
    /**
     * marks the server for shut down
     */
    public void shutDown() {
        is_alive = false;
    }
    
    /**
     * 
     * @return if the server is stopped right now
     */      
    public Boolean isStopped() {
        return is_stopped;
    }
    
    /**
     * 
     * @return if the server is marked for shut down
     */
    public Boolean is_alive() {
        return is_alive;
    }
    
    public void setSubscriptionManager(SubscriptionManager m) {
        this.sub_manager = m;
    }
    
    /**
     * moves all keys in the given range to the given server
     * @param range the range of keys to be moved
     * @param ip the ip of the target server
     * @param port the port of the target server
     * @param del indicates if data should be deleted after move
     * @return whether it was possible to connect to another server
     */
    public boolean moveData(Range range, String ip, int port, boolean del) {
        // create kvstore object
        KVStore kvClient = new KVStore(ip, PortOffset.getGossipPort(port));
        
        // connect
        try {
            kvClient.connectServer();
        } catch (IOException e) {
            logger.error("Exception while connecting in moveData()", e);
            return false;
        }        
        
        rwl.writeLock().lock();
        try {
            // get from cache
            CacheEntry entry = cache.iteratorStart();
            while(entry != null) {
                if (range.withinRange(entry.getKey())) {
                    if (del)
                        entry.setDeleted();
                    kvClient.put(entry.getKey(), entry.getValue());
                }
                entry = cache.iteratorNext();
            }

            // get from storage
            if (del)
                storage_manager.moveData(range, kvClient, cache);
            else
                storage_manager.replicateData(range, kvClient, cache);

            // disconnect
            kvClient.disconnect();

            if (del) {
                // move subscribers
                // we only do this if del is true, since replication of subs is not supported currently
                if (sub_manager != null)
                    sub_manager.moveData(range, kvClient);
                
                // mark for later cleanup
                moved_data = true;
            }

            return true;
        }
        finally {
            rwl.writeLock().unlock();
        }
    }
    
    /**
     * deletes all KV-Tuples where the key is in a given range
     * @param range the range of keys to be deleted
     * @return indicates if the operation was successful
     */    
    public boolean deleteData(Range range) {
        rwl.writeLock().lock();
        
        try {
            // delete from cache
            CacheEntry entry = cache.iteratorStart();
            while(entry != null) {
                if (range.withinRange(entry.getKey())) 
                    entry.setDeleted();

                entry = cache.iteratorNext();
            }      
            cache.vacuum();

            // delete from disk        
            try {
                storage_manager.deleteData(range);
                storage_manager.vacuum();
            }
            catch (IOException ioe) {
                logger.error("IO Exception during vacuum. ",ioe);
                return false;
            }

            return true;
        }
        finally {
            rwl.writeLock().unlock();
        }
    }
    
    /**
     * attempt to find the corresponding value to a given key
     * will first check the cache and then the disk
     * inserts the key and its value into the cache if it was not there
     * @param key the key to look for
     * @return the corresponding value or null if the key was not found
     */
    public String get(String key) {
        String value = null;
        
        // acquire read lock
        rwl.readLock().lock();
        try {
            // try to get from cache first
            value = cache.get(key);
            if (value != null)
                return (value.equals("null")) ? null : value;
        }
        finally {
            rwl.readLock().unlock();
        }
        
        // log cache miss
        logger.info("Cache miss when using get on key "+key);
        
        // acquire write lock
        rwl.writeLock().lock();           
        try {        
            // check if the key was inserted in the meantime        
            value = cache.get(key);
            if (value != null)             
                return (value.equals("null")) ? null : value;      

            try {
                // try to get from disk
                // read-lock would be enough for this, but concurrent disk access is not efficient                
                value = storage_manager.getFromDisk(key);
            }
            catch (IOException e) {
                // there was an error reading the storage
                logger.error("Exception while looking up key "+key+" on disk", e);
                return null;
            }
            
            // the key does not exist in the database
            if (value == null) 
                return null;

            // insert into cache
            if (cache.put(key,value,false) != StatusType.PUT_ERROR)               
                return value;
            
            // cache is full -> need to replace a key
            CacheEntry replaced_tuple = cache.getFirst();
 
            try {
                // write replaced key and value to disk
                if (replaced_tuple.isDirty())
                    storage_manager.writeToDisk(replaced_tuple.getKey(), replaced_tuple.getValue());
            }
            catch (IOException e) {
                // the replaced tuple could not be written to disk
                // the get-operation was successful anyway, so return value
                logger.error("Exception while writing tuple ("+replaced_tuple.getKey()+", "+replaced_tuple.getValue()+") to disk", e);
                return value;
            }
            
            // replace in cache
            cache.replace(key,value,false);   
            return value;
        }
        finally {  
            rwl.writeLock().unlock(); 
        }
    }
    
    /**
     * inserts or updates a given KV-tuple in the cache
     * will attempt to replace another tuple if the cache is full
     * the replaced tuple is written to disk
     * @param key the key to be inserted
     * @param value the value to be inserted, or "null" to delete
     * @return a status type to indicate success or error
     */
    public StatusType put(String key, String value) {
        /**
         * There was some debate on what to do when deleting a non-existing tuple.
         * Returning DELETE_ERROR could be ambiguous, since the user might think that
         * the tuple does exist, but there was some internal server problem.
         * We therefore decided to return DELETE_SUCCESS in this case, ensuring the 
         * user that the deleted key does not exist anymore after the put-operation.
         */
        CacheEntry replaced_tuple = null;
        rwl.writeLock().lock();
        try {
            // try to update the tuple in cache
            StatusType result = cache.put(key, value, true);
            if (result == StatusType.PUT_UPDATE) 
                return (value.equals("null")) ? StatusType.DELETE_SUCCESS : StatusType.PUT_UPDATE;
            
            // check storage to see if it was an update or insert
            String old_value = null;
            try {
                old_value = storage_manager.getFromDisk(key);
            }
            catch (IOException e) {
                // error retrieving the data item
                logger.error("Exception while looking up key "+key+" on disk", e);
                return StatusType.PUT_ERROR;
            }
            
            // log cache miss
            if (old_value != null)
                logger.info("Cache miss when updating key "+key);    
            
            // if the tuple was already inserted into the cache, we are done
            if (result == StatusType.PUT_SUCCESS) {              
                if (old_value != null)
                    return (value.equals("null")) ? StatusType.DELETE_SUCCESS : StatusType.PUT_UPDATE;
                return (value.equals("null")) ? StatusType.DELETE_SUCCESS : StatusType.PUT_SUCCESS;
            }

            // cache is full -> need to replace a key
            replaced_tuple = cache.getFirst();

            try {
                // write the replaced KV-tuple to disk
                if (replaced_tuple.isDirty())
                    storage_manager.writeToDisk(replaced_tuple.getKey(), replaced_tuple.getValue());                              
            }
            catch (IOException e1) {
                // the replaced tuple could not be written to disk
                logger.error("Exception while writing tuple ("+replaced_tuple.getKey()+", "+replaced_tuple.getValue()+") to disk",e1);
                try {
                    // try to write (key,value) instead
                    storage_manager.writeToDisk(key, value); 
                    
                    // return success
                    if (old_value != null)
                        return (value.equals("null")) ? StatusType.DELETE_SUCCESS : StatusType.PUT_UPDATE;
                    return (value.equals("null")) ? StatusType.DELETE_SUCCESS : StatusType.PUT_SUCCESS;                    
                }
                catch (IOException e2) {
                    // the tuple could not be inserted
                    logger.error("Exception while writing tuple ("+key+", "+value+") to disk",e2);
                    return StatusType.PUT_ERROR;
                }               
            }
            
            // replace in cache and return success
            cache.replace(key, value, !value.equals(old_value));           
            if (old_value != null)
                return (value.equals("null")) ? StatusType.DELETE_SUCCESS : StatusType.PUT_UPDATE;
            return (value.equals("null")) ? StatusType.DELETE_SUCCESS : StatusType.PUT_SUCCESS;        
        }
        finally {
            rwl.writeLock().unlock();
        }        
    }
}
