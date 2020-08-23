package common.messages;

import common.hashing.Range;
import java.math.BigInteger;
import java.nio.ByteBuffer;


/**
 * HOW THE META DATA FUNCTIONALITY WORKS: 
 * 
 * 
 * The sender constructs a string with all the new meta data entries by 
 * concatenating them. This looks like this: 
 * 
 * String data = new MetaData(ip, port, hashLow, hashHigh).toString() + ... + ... ;
 * 
 * Then the sender attaches the meta data string as a value to a Message object. 
 * The key has to be chosen as "meta". The Message object is being transformed to 
 * a byte array and sent to the recipient. 
 * 
 * 
 * The recipient checks whether the key of the received Message object equals 
 * "meta". If so, the value of the received Message object is the meta data. 
 * The recipient extracts the meta data by calling the Constructor like this: 
 * 
 * MetaData entry = new MetaData(String value)
 * 
 *  
 */
public class MetaDataEntry {
    private final String ip;
    private final int port;
    private final Range range;
     
    /**
     * CONSTRUCTOR
     * 
     * 4 bytes IP | 4 bytes Port | 16 bytes lower border | 16 bytes upper border
     * @param entry - 40 bytes of entry data 
     * 
     */
    public MetaDataEntry(byte[] entry) {    // NullpointerException?
        byte[] byteIP = new byte[4];
        byte[] bytePort = new byte[4];
        byte[] lowerBorder = new byte[16];
        byte[] upperBorder = new byte[16];
        
        System.arraycopy(entry, 0, byteIP, 0, 4);
        System.arraycopy(entry, 4, bytePort, 0, 4);
        System.arraycopy(entry, 8, lowerBorder, 0, 16);
        System.arraycopy(entry, 24, upperBorder, 0, 16);
        
        this.ip = byteIP[0] + "." + byteIP[1] + "." + byteIP[2] + "." + byteIP[3];
        this.port = ByteBuffer.wrap(bytePort).getInt();
        this.range = new Range(lowerBorder, upperBorder);
    }
    
    
    /**
     * CONSTRUCTOR
     * 
     * @param ip
     * @param port
     * @param range
     */
    public MetaDataEntry(String ip, int port, Range range){
        this.ip = ip;
        this.port = port; 
        this.range = range;
    }
    
    /**
     * Returns whether the given value is within the hash range of this meta data entry
     * 
     * @param value the value for which the check is being carried out
     * @return whether the input value is within the hash range of this meta data entry
     */
    public boolean withinHashRange(byte[] value){
        return range.withinRange(value);
    }
    
    public boolean withinHashRange(String value) {
        return range.withinRange(value);
    }
    
    private byte[] ipToBytes() {
        byte[] byteIP = new byte[4];
        String[] partials = this.ip.split("\\.");
        
        for(int i = 0; i < 4; i++) 
            byteIP[i] = Byte.parseByte(partials[i]);
        
        return byteIP;
    }
    
    /**
     * 4 bytes IP | 4 bytes port | 16 bytes lower boundary | 16 bytes upper boundary
     * @return 40 bytes message
     */
    public byte[] toBytes() {
        byte[] myMessage = new byte[40];
        System.arraycopy(ipToBytes(), 0, myMessage, 0, 4); // 4 bytes IP
        System.arraycopy(ByteBuffer.allocate(4).putInt(port).array(), 0, myMessage, 4, 4); // 4 bytes port
        System.arraycopy(range.getMin().toByteArray(), 0, myMessage, 8, 16); // 16 bytes lower boundary
        System.arraycopy(range.getMax().toByteArray(), 0, myMessage, 24, 16); // 16 bytes upper boundary
        return myMessage;
    }
    
    /**
     * split the range into two around a given separator
     * @param value the hash value that separates the new ranges
     * @return the new range
     */    
    public Range splitRange(BigInteger value) {
        return range.split(value);
    }
    
    /**
     * replace the minimum of the hash range
     * @param new_min the new minimum
     */        
    public void updateMin(BigInteger new_min) {
        range.setMin(new_min);
    }    
    
    /**
     * 
     * @return the ip
     */
    public String getIP(){
        return this.ip;
    }
    
    /**
     * 
     * @return the port
     */
    public int getPort(){
        return this.port;
    }
    
    /**
     * 
     * @return the low end of the range
     */
    public BigInteger getHashLow(){
        return this.range.getMin();
    }
    
    /**
     * 
     * @return the high end of the range
     */
    public BigInteger getHashHigh(){
        return this.range.getMax();
    }
    
   /**
     * 
     * @return the range
     */    
    public Range getRange() {
        return this.range;
    }
    
      
}
