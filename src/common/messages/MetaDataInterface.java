package common.messages;

/**
 * The Interface for the MetaData class
 * 
 */
interface MetaDataInterface {
    
    public byte[] toBytes();
    
    public void extractMetadata(byte[] raw);

    public Boolean isEmpty();
    
    public MetaDataEntry getEntry(String ip, int port);
    
    public MetaDataEntry getServer(byte[] hashValue);
    
    public MetaDataEntry getServer(String key);
    
    public MetaDataEntry insertServer(String ip, int port);
    
    public MetaDataEntry removeServer(String ip, int port);
    
    
}
