package cache;

import common.messages.KVMessage.StatusType;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.ListIterator;
import java.util.function.Predicate;

/**
 * We implemented the LFU-cache using a binary min-heap
 * The heap is represented by an ArrayList
 * A HashMap is additionally used to provide fast lookup
 */   
public class LFUCache implements Cache {
    /**
     * A heap node contains
     * entry: the cache entry represented by this node
     * priority: The number of times the tuple was referenced
     * idx: The position of the node in nodeList
     */       
    private class HeapNode {
        public HeapNode(CacheEntry e, int i) {
            priority = 1;
            idx = i;
            entry = e;
        }
        
        public void incPriority() {
            priority++;
        }
        
        public Boolean lessThan(HeapNode node) {
            return this.priority < node.priority;
        }
        
        public void setValue(String v) {
            entry.setValue(v);
        }
        
        public String getValue() {
            return entry.getValue();
        }
        
        public int idx;
        public int priority;
        public CacheEntry entry;
    }
    
    /**
     *
     * @param s the maximum number of elements the cache can store
     */       
    public LFUCache(int s) {
        maxSize = s;
    }
    
    private final ArrayList<HeapNode> nodeList = new ArrayList<HeapNode>();
    private final HashMap<String,HeapNode> indexMap = new HashMap<String,HeapNode>();
    private int maxSize;
    private ListIterator<HeapNode> itr;
    
    private void swap(HeapNode i_node, HeapNode j_node) {
        int i = i_node.idx;
        
        // swap elements in the list
        nodeList.set(i, j_node);
        nodeList.set(j_node.idx, i_node);  
        
        // swap index information
        i_node.idx = j_node.idx;
        j_node.idx = i;
    }
    
    private void bubbleDown(HeapNode node) {
        HeapNode left = left(node.idx);
        HeapNode right = right(node.idx);
        while ((left != null && node.priority > left.priority) || (right != null && node.priority > right.priority)) {
            // reaching this point, left cannot be null!
            if (right == null || left.priority < right.priority)
                swap(node, left);
            else
                swap(node, right);
            
            left = left(node.idx);
            right = right(node.idx);
        }
    }
    
    private void bubbleUp(HeapNode node) {
        HeapNode parent = parent(node.idx);
        while (parent != null && node.priority < parent.priority) {
            swap(node, parent);
            parent = parent(node.idx);
        }        
    }
    
    private void incPriority(HeapNode node) {
        node.incPriority();
        bubbleDown(node);
    }
    
    @Override
    public void vacuum() {
        // remove elements
        Predicate<HeapNode> pred = n-> n.entry.isDeleted();
        nodeList.removeIf(pred);
        
        indexMap.values().removeIf(pred);    
        
        if (nodeList.isEmpty())
          return;      
        
        // update idx of all nodes
        for (int i = 0; i < nodeList.size(); i++)
            nodeList.get(i).idx = i;
        
        // rebuild the heap
        for (int i = nodeList.size()/2-1; i>=0; --i)
          heapify(i);
    }
    
    @Override
    public CacheEntry iteratorStart() {
        itr = nodeList.listIterator();
        return iteratorNext();
    }
    
    @Override
    public CacheEntry iteratorNext() {
        if (!itr.hasNext())
            return null;
        return itr.next().entry;
    }
    
    @Override
    public int size() {
        return nodeList.size();
    }
    
    @Override
    public StatusType put(String key, String value, Boolean is_dirty) {
        // check if key is in the cache
        HeapNode node = indexMap.get(key);
        if (node != null) {
            node.setValue(value);
            incPriority(node);
            return StatusType.PUT_UPDATE;
        }
        
        // check if cache is full
        if (indexMap.size() == maxSize)
            return StatusType.PUT_ERROR;
        
        // insert
        node = new HeapNode(new CacheEntry(key,value,is_dirty), nodeList.size());
        indexMap.put(key,node);
        nodeList.add(node);
        bubbleUp(node);
        return StatusType.PUT_SUCCESS;
    }
    
    @Override
    public CacheEntry replace(String key, String value, Boolean is_dirty) {
        // retrieve current min
        CacheEntry root = nodeList.get(0).entry;
        
        // replace in node list
        HeapNode node = new HeapNode(new CacheEntry(key,value,is_dirty), 0);
        nodeList.set(0,node);
        
        // replace in index map
        indexMap.remove(root.getKey());
        indexMap.put(key, node);
        
        return root;
    }
    
    @Override
    public String get(String key) {
        HeapNode node = indexMap.get(key);
        if (node == null)
            return null;
        
        incPriority(node);
        return node.getValue();
    }
    
    @Override
    public Boolean containsKey(String key) {
        return indexMap.containsKey(key);
    }    
    
    @Override
    public CacheEntry getFirst() {
        HeapNode root = nodeList.get(0);
        if (root == null)
            return null;
        return root.entry;
    }    
    
    private void heapify(int index) {
        HeapNode left = left(index);
        if(left == null)
            return;
        int parentIndex = index;
        if( left.priority < nodeList.get(parentIndex).priority )
            parentIndex = left.idx;
        HeapNode right = right(index);
        if( right != null && right.priority < nodeList.get(parentIndex).priority )
            parentIndex = right.idx;
        if(parentIndex != index) {
            swap(nodeList.get(parentIndex), nodeList.get(index));
            heapify(parentIndex);
        }
    }    
    
    private HeapNode parent(int i) {
        int idx = (i - 1) / 2;
        if (idx < 0)
            return null;
        return nodeList.get(idx);
    }

    private HeapNode left(int i) {
        int idx = 2*i + 1;
        if (idx >= nodeList.size())
            return null;
        return nodeList.get(idx);
    }

    private HeapNode right(int i) {
        int idx = 2*i + 2;
        if (idx >= nodeList.size())
            return null;        
        return nodeList.get(idx);
    }    
}
