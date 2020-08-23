package cache;

public class CacheEntry {
    
    /**
     *
     * @param k the key
     * @param v the value
     * @param b indicates if the entry is dirty or not
     */
    public CacheEntry(String k, String v, Boolean b) {
        key = k;
        value = v;
        dirty = b;
        deleted = false;
    }
    
    /**
     * overwrites the value and sets dirty if necessary
     * @param v the new value
     */
    public void setValue(String v) {
        if (!value.equals(v))
            dirty = true;
        value = v;
    }
    
    /**
     *
     * @return the key
     */
    public String getKey() {
        return key;
    }
    
    /**
     *
     * @return the value
     */
    public String getValue() {
        return value;
    }
    
    /**
     *
     * @return if the entry is dirty
     */
    public Boolean isDirty() {
        return dirty;
    }
 
    /**
     * marks this entry to be deleted later
     */
    public void setDeleted() {
        deleted = true;   
    }
    
    /**
     *
     * @return if the entry is deleted
     */    
    public Boolean isDeleted() {
        return deleted;
    }
    
    private String key;
    private String value;
    private Boolean dirty;
    private Boolean deleted;
}
