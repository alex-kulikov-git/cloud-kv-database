package common.messages;

import common.hashing.Hashing;
import common.hashing.Range;
import java.math.BigInteger;
import java.util.ArrayList;

/**
 * Implements the meta data interface to manage all the information on running servers. 
 */
public class MetaData implements MetaDataInterface{
    private ArrayList<MetaDataEntry> metaData;
    
    public MetaData() {
        this.metaData = new ArrayList<MetaDataEntry>();
    }
    
    /**
     * @return number of meta data entries/ number of servers running
     */
    public int numberOfEntries(){
        return metaData.size();
    }
    
    /**
     * Adds entry to current list of metaData
     * @param entry - entry to be added
     */
    public void addEntry(MetaDataEntry entry) { // is this used?
        metaData.add(entry);
    }
    
    /**
     * Removes entry from meta Data
     * @param entry - entry to be removed
     */
    public void removeEntry(MetaDataEntry entry) { // is this used?
        metaData.remove(entry); // please test removal, by passing a new entry with the same fields
    }
    
    /**
     * Replaces metaData with new ArrayList<MetaDataEntry> 
     * @param metaData - new List
     */
    private void replaceMetaData(ArrayList<MetaDataEntry> metaData) {
        this.metaData = metaData;
    }
    
    /**
     * Converts String to byte and decodes metaData
     * @param raw
     */
    @Override
    public void extractMetadata(byte[] raw) { // how to separate entries? 
        ArrayList<MetaDataEntry> tmp = new ArrayList<>();
        
        if(raw.length % 40 != 0)
            throw new RuntimeException("encoding, decoding not working.");
        
        for(int i = 0; i < raw.length / 40; i++) {
            byte[] byteEntry = new byte[40];
            System.arraycopy(raw, i * 40, byteEntry, 0, 40);
            tmp.add(new MetaDataEntry(byteEntry));
        }
        
        replaceMetaData(tmp);   
    }
    
    
    /**
     * @param ip - of the server calling this method
     * @param port - of the server calling this method
     * @param key - the requested key
     * @return if server is allowed to handle the request
     */
    public boolean withinReadingRange(String ip, int port, String key) {
        // key is within range of this server or within range of one of the two predecessors
        MetaDataEntry currentServer = getEntry(ip, port);
        MetaDataEntry predOne = getPredecessor(ip, port);
        MetaDataEntry predTwo = getPredecessor(predOne.getIP(), predOne.getPort());
        
        return currentServer.withinHashRange(key) || predOne.withinHashRange(key) || predTwo.withinHashRange(key);
    }
    
    /**
     * @param ip - of the server calling this method
     * @param port - of the server calling this method
     * @param key - the requested key
     * @return if server is allowed to handle the request
     */
    public boolean withinWritingRange(String ip, int port, String key) {
        // key is within range of this server
        return getEntry(ip, port).withinHashRange(key);
    }
    
    
    /**
     * Converts metaData to String - can be passed to new Message( ... )
     * @return String of metaData
     */
    @Override
    public byte[] toBytes() {
        byte[] concat = new byte[40 * metaData.size()];
        
        for(int i = 0; i < metaData.size(); i++) 
            System.arraycopy(metaData.get(i).toBytes(), 0, concat, i * 40, 40);
        
        return concat;
    }
    
    @Override
    public Boolean isEmpty() {
        return metaData.isEmpty();
    }
    
    /**
     * 
     * @return the first entry in the meta data
     */    
    public MetaDataEntry getFirst() {
        if (metaData.isEmpty())
            return null;
        return metaData.get(0);
    }
    
    /**
     * Looks for a meta data entry to the given ip and port 
     * 
     * @param ip given ip
     * @param port given port
     * @return The meta data entry with the specified ip and port
     */
    @Override
    public MetaDataEntry getEntry(String ip, int port) {
        for(MetaDataEntry entry : metaData)
            if(entry.getIP().equals(ip) && entry.getPort() == port)
                return entry;
        
        throw new RuntimeException("This server is not in the list."); // Is this possible?
    }
    
    /**
     * Looks for a meta data entry that has a hash range which includes the given hash value. 
     * 
     * @param hashValue the given hash value
     * @return the meta data entry with the suiting hash range
     */
    @Override
    public MetaDataEntry getServer(byte[] hashValue) {
        for(MetaDataEntry entry : metaData)
            if(entry.withinHashRange(hashValue))
                return entry;
        
        throw new RuntimeException("hashValue is not within global hash range"); // Is this possible?
    }
    
    /**
     * Looks for a meta data entry that has a hash range which includes the given key
     * 
     * @param key the given key
     * @return the meta data entry with the suiting hash range
     */    
    @Override
    public MetaDataEntry getServer(String key) {
        byte[] hash = Hashing.getHashValue(key);
        return getServer(hash);
    }
    
    /**
     * Delivers the MetaDataEntry of the successor to the server with the given ip and port. 
     * 
     * @param ip the ip of the current server
     * @param port the port of the current server
     * @return the MetaDataEntry of the successor to the given entry
     */
    public MetaDataEntry getSuccessor(String ip, int port){
        MetaDataEntry current = getEntry(ip, port);
        
        // Get highest value of the current entry's range
        BigInteger currentHashHigh = current.getHashHigh();
        
        // compute a hash value that is included in the successors range 
        BigInteger successorHashLow = currentHashHigh.add(BigInteger.ONE); // This should be successor's hashLow
        
        // get the successor in whose range the new hash value is
        MetaDataEntry successor = getServer(successorHashLow.toByteArray());
        
        return successor;
    }
    
    /**
     * Delivers the MetaDataEntry of the predecessor to the server with the given ip and port. 
     * @param ip the ip of the current server
     * @param port the port of the current server
     * @return the MetaDataEntry of the predecessor to the given entry
     */
    public MetaDataEntry getPredecessor(String ip, int port){
        MetaDataEntry current = getEntry(ip, port);
        
        // Get lowest value of the current entry's range
        BigInteger currentHashLow = current.getHashLow();
        
        // Calculate the highest value of the predecessor's range with: (Lowest Value Current - 1) = (Highest Value Predecessor)
        BigInteger predecessorHashHigh = currentHashLow.subtract(BigInteger.ONE);
        
        // Get the corresponding server entry to the calculated highest value of the predecessor's range
        MetaDataEntry predecessor = getServer(predecessorHashHigh.toByteArray());
        
        return predecessor;
    }
    
    /**
     * Inserts a new server 
     * 
     * @param ip the server's ip
     * @param port the server's port
     * @return a meta data entry containing the range of the inserted server and the ip/port of the successor
     */    
    @Override
    public MetaDataEntry insertServer(String ip, int port) {       
        // compute the hash value
        byte[] hash = Hashing.getHashValue(ip + ":" + Integer.toString(port));
        BigInteger bg = new BigInteger(hash);
        
        if (metaData.isEmpty()) {            
            Range range = new Range(bg.add(BigInteger.ONE), bg);
            metaData.add(new MetaDataEntry(ip,port,range));
            return null;
        }
        
        // find the responsible server (successor) and split its range
        MetaDataEntry e = getServer(hash);
        Range range = e.splitRange(bg);
        
        metaData.add(new MetaDataEntry(ip,port,range));
        
        // needs to return range of the new server and ip/port of successor
        return new MetaDataEntry(e.getIP(),e.getPort(),range);
    }
    
    /**
     * Removes a server 
     * 
     * @param ip the server's ip
     * @param port the server's port
     * @return a meta data entry containing the range of the removed server and the ip/port of the successor
     */      
    @Override
    public MetaDataEntry removeServer(String ip, int port) {      
        if (metaData.isEmpty())
            return null;

        // find ip:port in the meta data
        MetaDataEntry e = getEntry(ip,port);
        if (e == null)
            return null;
        
        if (metaData.size() == 1) {
            metaData.clear();
            return null;
        }
        
        // find and update successor
        BigInteger removed_high = e.getHashHigh();
        for(MetaDataEntry entry : metaData) {
            if(entry.getHashLow().subtract(removed_high).equals(BigInteger.ONE)) {
                // update range of successor node
                entry.updateMin(e.getHashLow()); 
                
                // remove the element
                metaData.remove(e);

                // needs to return ip/port of successor and range of removed server
                return new MetaDataEntry(entry.getIP(), entry.getPort(), new Range(e.getHashLow(),e.getHashHigh()));     
            } 
        }
               
        return null;
    }
    
    /**
     * Prints metaData for debugging purposes.
     */
    public void printMetaData() {
        for(MetaDataEntry entry : metaData) {
            System.out.println("ip: " + entry.getIP() + "; port: " + entry.getPort());
        }
    }
    
}
