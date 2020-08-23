/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package common.hashing;

import java.math.BigInteger;

/**
 *
 * @author kajo
 */
public class Range {
    private BigInteger min;
    private BigInteger max;
    
    public Range(BigInteger min, BigInteger max) { // REFACTOR
        this.min = min;
        this.max = max;
    }
    
    public Range(byte[] min, byte[] max) {
        this.min = new BigInteger(min);
        this.max = new BigInteger(max);
    }
    
    /**
     * @param value a 128-bit MD5-hash value
     * @return if the given value is within the range
     */
    public boolean withinRange(byte[] value){
        BigInteger bg = new BigInteger(value);
        return withinRange(bg);
    }
    
    /**
     * @param key a key in String format
     * @return if the given value is within the range
     */
    public boolean withinRange(String key){
        byte[] hash = Hashing.getHashValue(key);
        return withinRange(hash);
    }    
    
     /**
      * @param value a 128-bit MD5-hash value in BigInteger format
      * @return if the given value is within the range
      */
    public boolean withinRange(BigInteger value){
        if (min.compareTo(max) < 0) {
            // this is a normal range where min<max
            return value.compareTo(max) <= 0 && value.compareTo(min) >= 0; // Note: The range includes the min and the max values themselves among others
        }
        
        // here we need to wrap around
        return value.compareTo(max) <= 0 || value.compareTo(min) >= 0;       
    }
    
     /**
      * Splits the range using a given value as separator
      * @param value the value used as separator between the two segments
      * @return the second (new) segment
      */    
    public Range split(BigInteger value){
        BigInteger old_min = min;
        min = value.add(BigInteger.ONE);

        return new Range(old_min, value);
    }
    
    public void setMin(BigInteger new_min) {
        min = new_min;
    }
    
    public BigInteger getMin() {
        return min;
    }
    
    public BigInteger getMax() {
        return max;
    }
    
}
