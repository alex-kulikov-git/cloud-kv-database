package cache;

import common.messages.KVMessage.StatusType;
import java.util.HashMap;
import java.util.Set;
import java.util.function.Predicate;

/**
 * We implemented the LRU-cache using a doubly-linked list
 * A HashMap is additionally used to provide fast lookup
 */   
public class LRUCache implements Cache {    
    private class ListNode {
        public ListNode(CacheEntry e) {
            entry = e;
        }
        
        public void setValue(String v) {
            entry.setValue(v);
        }
        
        public String getValue() {
            return entry.getValue();
        }
        
        public CacheEntry entry;
        public ListNode next=null;
        public ListNode prev=null;
    }
    
    /**
     *
     * @param s the maximum number of elements the cache can store
     */          
    public LRUCache(int s) {
        maxSize = s;
    }

    private final HashMap<String,ListNode> indexMap = new HashMap<String,ListNode>();
    private int maxSize;
    private ListNode listHead=null;
    private ListNode listTail=null;
    private ListNode itr;
    
    private void reference(ListNode node) {
        if (node == listHead)
            return;

        // unlink node
        if (node == listTail)
            listTail = node.next;
        else
            node.prev.next = node.next;
        node.next.prev = node.prev;

        // move node to the head of the list
        listHead.next = node;
        node.prev = listHead;
        node.next = null;
        listHead = node;             
    }
    
    @Override
    public void vacuum() {   
        Predicate<ListNode> pred = n-> n.entry.isDeleted();
        indexMap.values().removeIf(pred);           
        
        ListNode node = listTail;
        while (node != null) {
            if (node.entry.isDeleted()) {
                ListNode prev = node.prev;
                ListNode next = node.next;
                if (prev != null)
                    prev.next = next;                
                if (next != null)
                    next.prev = prev;
                
                if (node == listTail)
                    listTail = node.next;
                if (node == listHead)
                    listHead = node.prev;
            }
            node = node.next;
        } 
    }    
    
    @Override
    public CacheEntry iteratorStart() {
        itr = listTail;
        return iteratorNext();
    }
    
    @Override
    public CacheEntry iteratorNext() {
        if (itr == null)
            return null;
        
        CacheEntry entry = itr.entry;
        itr = itr.next;

        return entry;
    } 
    
    @Override
    public int size() {
        return indexMap.size();
    }    
    
    @Override
    public StatusType put(String key, String value, Boolean is_dirty) {
        // check if key is in the cache
        ListNode node = indexMap.get(key);
        if (node != null) {
            node.setValue(value);
            reference(node);
            return StatusType.PUT_UPDATE;
        }
        
        // check if cache is full
        if (indexMap.size() == maxSize)
            return StatusType.PUT_ERROR;
        
        // insert at the head of the list
        node = new ListNode(new CacheEntry(key,value,is_dirty));
        node.prev = listHead;
        if (listHead != null)
            listHead.next = node;
        listHead = node;
        if (listTail == null)
            listTail = node;
        
        // insert into map
        indexMap.put(key,node);
        return StatusType.PUT_SUCCESS;
    }
    
    @Override
    public CacheEntry replace(String key, String value, Boolean is_dirty) {
        // NOTE: we assume that this is only called when the cache is full
        // remove the node at the tail
        ListNode replaced_node = listTail;
        indexMap.remove(replaced_node.entry.getKey());
        if (listTail != listHead) {
            listTail.next.prev = null;
            listTail = listTail.next;
        }
        
        // insert new key
        put(key,value,is_dirty);
        return replaced_node.entry;
    }      
    
    @Override
    public String get(String key) {
        ListNode node = indexMap.get(key);
        if (node == null)
            return null;
        
        reference(node);
        return node.getValue();
    }  
    
    @Override
    public Boolean containsKey(String key) {
        return indexMap.containsKey(key);
    }    
    
    @Override
    public CacheEntry getFirst() {
        ListNode node = listTail;
        if (node == null)
            return null;
        return node.entry;
    }    
}
