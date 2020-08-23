package manager;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.nio.ByteBuffer;
import cache.Cache;
import client.KVStore;
import common.hashing.Range;
import java.io.FileInputStream;
import java.io.BufferedInputStream;

/**
 *   We use a heap file to store KV-tuples
 *   We additionally use an index on the keys for faster lookup
 */  
public class StorageManager {
    // constants for storage format
    private static final int MAX_VALUE_SIZE = 120000;
    private static final int ITEM_HEADER_SIZE = 6;
    
    // constants for index format
    // index entry format: KEY | DATAPOS | EXTRA
    // EXTRA = SIZE OF KEY | DELETED
    private static final int KEYSIZE = 20;
    private static final int EXTRASIZE = 2;
    private static final int INDEXSIZE = 8;
    private static final int ENTRYSIZE = KEYSIZE+INDEXSIZE+EXTRASIZE;

    private int storage_number;
       
    /**
     * initialize the storage number so that storage files of different server processes
     * on the same machine can be distinguished
     * @param port port of the server that uses the storage manager
     */
    public StorageManager(int port) {
        // we use port to distinguish storage files
        storage_number = port;
    }
    
    /**
     * move keys in the given range to the given server and mark them for deletion
     * @param range the range of keys to be moved
     * @param kvClient the KVStore object that is used to send data
     * @param cache the cache of this server
     */
    public void moveData(Range range, KVStore kvClient, Cache cache) {        
        moveOrDeleteData(range,kvClient,cache,true,true);
    }
    
    /**
     * copy keys in the given range to the given server
     * @param range the range of keys to be moved
     * @param kvClient the KVStore object that is used to send data
     * @param cache the cache of this server
     */
    public void replicateData(Range range, KVStore kvClient, Cache cache) {        
        moveOrDeleteData(range,kvClient,cache,true,false);
    }    
    
    /**
     * delete data in the given range
     * @param range the range of keys to be moved
     */
    public void deleteData(Range range) {        
        moveOrDeleteData(range,null,null,false,true);
    }       
    
    /**
     * move and/or delete data in a given range
     * @param range the range of keys to be moved/deleted
     * @param kvClient the KVStore object that is used to send data
     * @param cache the cache of this server
     * @param move indicates if the data should be moved
     * @param del indicates if the data should be deleted
     */
    private void moveOrDeleteData(Range range, KVStore kvClient, Cache cache, boolean move, boolean del) {        
        ByteBuffer buf = ByteBuffer.allocate(100*ENTRYSIZE);
        byte[] valbytes = new byte[MAX_VALUE_SIZE];
        try {
            // establish files and streams
            RandomAccessFile index = getIndexRAFile("r");
            RandomAccessFile storage = null;
            if (!del) {
                // if we don't delete data, read access is enough
                storage = getStorageRAFile("r");
            }
            else {
                storage = getStorageRAFile("rw");
            }
            FileInputStream fis = new FileInputStream(index.getFD());
            BufferedInputStream bis = new BufferedInputStream(fis);            
            while(index.getFilePointer() < index.length()) {
                buf.position(0); 
                
                // read from the index
                int length = bis.read(buf.array(), 0, buf.capacity());           
                while (buf.position() < length) {
                    // extract the key
                    String padded_key = new String(buf.array(), buf.position(), KEYSIZE);
                    buf.position(buf.position()+KEYSIZE);
                    
                    // read position in storage file
                    long datapos = buf.getLong();   
                    
                    // read keysize
                    byte keysize = buf.get();
                    String key = padded_key.substring(0, keysize);
                    
                    // skip deleted flag
                    buf.position(buf.position()+1);

                    // check if key is in the range
                    if (!range.withinRange(key))
                        continue;

                    // access storage now                  
                    storage.seek(datapos);
                    
                    // check if the tuple is deleted
                    byte deleted = storage.readByte();
                    if (deleted != 0)
                        continue;
                    
                    // set deleted
                    if (del) {
                        storage.seek(datapos);
                        storage.write((byte)1);
                    }
                    
                    if (!move) {
                        // if we do not move data, we are done
                        continue;
                    }
                    
                    // check if key is in the cache
                    if (cache.containsKey(key))
                        continue;                    

                    // skip key size
                    storage.skipBytes(1);

                    // get total size            
                    int totalsize = storage.readInt();

                    // skip to the data
                    storage.seek(datapos+ITEM_HEADER_SIZE+keysize);

                    // get the data
                    storage.read(valbytes, 0, totalsize-keysize);                    

                    // send the KV-Tuple
                    kvClient.put(key, new String(valbytes, 0, totalsize-keysize));
                }
            }
            index.close();
            storage.close();
        }
        catch (IOException ioe) {
            
        }
    }    
    
    /**
     * creates storage and index files for disk storage
     * deletes old storage and index files if there are any
     * @throws IOException if the files could not be created
     */
    public void initClearedStorage() throws IOException {
        // storage file
        File file = getStorageFile();      
        if (file.exists()) {
            file.delete();
        }

        // create new storage file
        try {
            file.createNewFile();
        }
        catch (IOException ioe) {
            throw ioe;
        }
        
        // index file
        file = getIndexFile();
        if (file.exists()) {
            file.delete();
        }       
        
        // create new index file
        try {
            file.createNewFile();
        }
        catch (IOException ioe) {
            throw ioe;
        }        
    }        

    /**
     * creates storage and index files for disk storage if they don't exist already
     * @throws IOException if the files could not be created
     */
    public void initStorage() throws IOException {
        // storage file
        File file = getStorageFile();

        // create new storage file if necessary
        if (!file.exists()) {
            try {
                file.createNewFile();
            }
            catch (IOException ioe) {
                throw ioe;
            }
        }
        
        // index file
        file = getIndexFile();
        
        // create new index file if necessary
        if (!file.exists()) {
            try {
                file.createNewFile();
            }
            catch (IOException ioe) {
                throw ioe;
            }        
        }
    }    
    
    /**
     * write a given KV-tuple to disk
     * @param key the key of the tuple
     * @param value the value of the tuple
     * @throws IOException if an error occured while updating the storage file
     */
    public void writeToDisk(String key, String value) throws IOException {
        try {
            // delete
            if (value.equals("null")) {
                deleteFromDisk(key);
                return;
            }

            // update or insert
            updateOnDisk(key, value);  
        }
        catch (IOException ioe) {
            throw ioe;
        }
    }    
    
    /**
     * get the corresponding value to a given key from disk
     * @param key the key to be looked for
     * @return the corresponding value or null if the key was not found
     * @throws IOException if an error occured while reading the storage file
     */
    public String getFromDisk(String key) throws IOException {
        // get path to storage file
        RandomAccessFile file = null;  
        String padded_key = pad_key(key);
        
        try {
            // get datapos from index
            file = getIndexRAFile("r");
            long datapos = get_datapos(file,padded_key);
            file.close();
            if (datapos < 0)
                return null;

            file = getStorageRAFile("r");
            file.seek(datapos+1);
            
            // get key size
            byte keysize = file.readByte();
            
            // get total size            
            int totalsize = file.readInt();
            
            // skip to the data
            file.seek(datapos+ITEM_HEADER_SIZE+keysize);
            
            // get the data
            byte[] buf = new byte[totalsize-keysize];
            file.read(buf);
            file.close();
            return new String(buf);
        }
        catch (IOException ioe) {
            throw ioe;
        }    
    }     
    
    /**
     *
     * @return the number of bytes in the storage file
     */
    public long getStorageSize() {
        try {
            RandomAccessFile file = getStorageRAFile("r");
            long size = file.length();
            file.close();
            return size;
        }
        catch (IOException ioe) {
            
        }
        return -1;
    }
    
    // the maximum amount of bytes to be stored in an array
    private static final int MAX_ARRAY_SIZE = 1*1000*1000;

    /**
     * compacts the storage space by removing all tuples that are flagged as deleted
     * @throws IOException if there was an error while updating the storage
     */
    public void vacuum() throws IOException {
        try {
            // open storage and index files
            RandomAccessFile file = getStorageRAFile("rw");
            RandomAccessFile index_file = getIndexRAFile("rw");
            // we rebuild the index
            // note: this is not the most efficient solution, but we ran out of time
            index_file.setLength(0);
            
            byte[] keybuf = new byte[KEYSIZE];
            long datapos = 0;
            long deleted_start = -1;
            ByteBuffer data = ByteBuffer.allocate(MAX_ARRAY_SIZE);

            Boolean scanning_deleted_block = false;
            while (datapos < file.length()) {
                file.seek(datapos);

                // check if this entry is deleted
                Boolean deleted = file.readBoolean();
                if (!scanning_deleted_block && deleted) {
                    // we found the end of a non-deleted block
                    if (deleted_start >= 0) {
                        // copy the current non-deleted data block
                        file.seek(deleted_start);
                        file.write(data.array(), 0, data.position());
                        deleted_start += data.position();
                        
                        // go to the current position again
                        file.seek(datapos+1);
                        data.position(0);
                    }
                    else
                        deleted_start = datapos;
                    scanning_deleted_block = true;
                }
                else if (scanning_deleted_block && !deleted) {
                    // we found the end of a deleted block
                    scanning_deleted_block = false;
                }
                
                if (scanning_deleted_block) {
                    // we are scanning a deleted block -> just skip the data item
                    file.skipBytes(1);
                    datapos += ITEM_HEADER_SIZE + file.readInt();
                    continue;
                }
                                
                // we are scanning a non-deleted block               
                byte keysize = file.readByte();
                int itemsize = file.readInt();                
                if (deleted_start >= 0) {
                    if (data.position() + ITEM_HEADER_SIZE + itemsize > MAX_ARRAY_SIZE) {
                        // we store too much data in main memory
                        // copy the current non-deleted data block
                        file.seek(deleted_start);
                        file.write(data.array(), 0, data.position());
                        deleted_start += data.position();
                        
                        // go to the current position again
                        file.seek(datapos+ITEM_HEADER_SIZE);
                        data.position(0);
                    }
                    
                    int old_position = data.position();
                    
                    // append header to data
                    data.put((byte)0);
                    data.put(keysize);
                    data.putInt(itemsize);
                    
                    // read the data item   
                    data.position(data.position() + itemsize);
                    file.read(data.array(), data.position()-itemsize, itemsize);
                    
                    // insert into index
                    String key = new String(Arrays.copyOfRange(
                                            data.array(), 
                                            old_position+ITEM_HEADER_SIZE, 
                                            old_position+ITEM_HEADER_SIZE+keysize));
                    insert_index(index_file, key, deleted_start + old_position);
                }
                else {
                    // read the key
                    file.read(keybuf,0,keysize);                    
                    String key = new String(keybuf,0,keysize);
                    
                    // insert into index
                    insert_index(index_file, key, datapos);
                }
                
                datapos += ITEM_HEADER_SIZE + itemsize;
            }
            
            if (scanning_deleted_block) {
                // we reached EOF while scanning a deleted block -> truncate file
                file.setLength(deleted_start);
            }
            else {
                // we reached EOF while scanning a non-deleted block -> one more write
                if (deleted_start >= 0) {
                    // copy the data block
                    file.seek(deleted_start);
                    file.write(data.array(), 0, data.position());
                    file.setLength(deleted_start + data.position());
                }                
            }
            
            file.close();
            index_file.close();
        }
        catch (IOException ioe) {
            throw ioe;
        }
    }    
    
    /**
     * attempt to delete the KV-tuple corresponding to a given key from the disk
     * this flags the affected tuple as deleted, but does not free disk space
     * use vacuum() to free disk space
     * @param key the key to be deleted
     */    
    private void deleteFromDisk(String key) throws IOException {  
        RandomAccessFile file = null;   
        String padded_key = pad_key(key);
        
        try {
            // delete from index
            file = getIndexRAFile("rw");
            long datapos = delete_index(file,padded_key);
            file.close();
            if (datapos < 0)
                return;

            // set deleted-flag in the data
            file = getStorageRAFile("rw");
            file.seek(datapos);
            file.writeBoolean(true);
            file.close();
        }
        catch (IOException ioe) {
            throw ioe;
        }           
    }
    
    /**
     * attempt to insert a given KV-tuple into the storage file
     * @param padded_key the key to be inserted padded to KEYSIZE
     * @param key the key to be inserted
     * @param value the value to be inserted
     */      
    private void insertOnDisk(String padded_key, String key, String value) throws IOException {
        RandomAccessFile file = null;
        
        try {
            file = getStorageRAFile("rw");
            
            // skip to end
            file.seek(file.length());
            
            // write data header
            long datapos = file.length();
            file.writeBoolean(false);
            file.writeByte(key.length());
            file.writeInt(key.length()+value.length());
            
            // write data
            file.write(key.getBytes());
            file.write(value.getBytes());
            file.close();
            
            // write index
            file = getIndexRAFile("rw");
            insert_index(file,key,datapos);
            file.close();                         
        }
        catch (IOException ioe) {
            throw ioe;
        }        
    }
    
    /**
     * attempt to overwrite a tuple with a given key
     * @param key the key of the tuple that will be updated
     * @param value the new value
     */       
    private void updateOnDisk(String key, String value) throws IOException {
        // first we check if the key is already on disk
        RandomAccessFile file = null;     
        String padded_key = pad_key(key);
        
        try {
            // get datapos from index
            file = getIndexRAFile("rw");
            long datapos = get_datapos(file,padded_key);
            file.close();
            
            if (datapos < 0) {
                // key is not on disk -> insert
                insertOnDisk(padded_key, key, value);
                return;
            }            

            file = getStorageRAFile("rw");
            file.seek(datapos+1);
            
            // read key size
            byte key_size = file.readByte();
            
            // get value size
            int old_value_size = file.readInt() - key_size;
            
            // check if there is enough space
            int size_diff = old_value_size - value.length();
            if (size_diff == 0 || size_diff >= ITEM_HEADER_SIZE+2) {
                // update value size
                file.seek(datapos+2);
                file.writeInt(value.length() + key_size);
                
                // skip to data
                file.seek(datapos+ITEM_HEADER_SIZE+key_size);
                
                // write data
                file.write(value.getBytes());
                                
                if (size_diff != 0) {
                    // we need to fill up some extra space
                    // write dummy tuple with deleted-flag
                    ByteBuffer buf = ByteBuffer.allocate(ITEM_HEADER_SIZE);
                    buf.put((byte)1);
                    buf.put((byte)1);
                    buf.putInt(size_diff - ITEM_HEADER_SIZE);
                    file.write(buf.array(),0,buf.position());
                }
                
                file.close();
                return;
            }
            
            // not enough space -> set deleted-flag instead
            file.seek(datapos);
            file.writeBoolean(true);
            file.close();
            
            // value could not be updated -> insert instead
            insertOnDisk(padded_key,key,value);
        }
        catch (IOException ioe) {
            throw ioe;
        }       
    }    
    
    private File getStorageFile() {
        Path currentRelativePath = Paths.get("");
        String p = currentRelativePath.toAbsolutePath().toString();        
        File file = new File(p+"/storage"+Integer.toString(storage_number)+".txt");     
        
        return file;
    }
    
    private File getIndexFile() {
        Path currentRelativePath = Paths.get("");
        String p = currentRelativePath.toAbsolutePath().toString();        
        File file = new File(p+"/index"+Integer.toString(storage_number)+".txt");     
        
        return file;
    }
    
    private RandomAccessFile getStorageRAFile(String mode) throws IOException {
        Path currentRelativePath = Paths.get("");
        String p = currentRelativePath.toAbsolutePath().toString();   
        RandomAccessFile file = null;
        
        try {
            file = new RandomAccessFile(p+"/storage"+Integer.toString(storage_number)+".txt", mode);    
        }
        catch (IOException ioe) {
            throw ioe;
        }
        
        return file;
    }
    
    private RandomAccessFile getIndexRAFile(String mode) throws IOException {
        Path currentRelativePath = Paths.get("");
        String p = currentRelativePath.toAbsolutePath().toString();   
        RandomAccessFile file = null;
        
        try {
            file = new RandomAccessFile(p+"/index"+Integer.toString(storage_number)+".txt", mode);    
        }
        catch (IOException ioe) {
            throw ioe;
        }
        
        return file;
    }    
    
    /**
     *   use binary search to find a key in the index represented by sorted array A
     *   returns position p such that A[p] <= key <= A[p+1]
     *   this assumes that the input key is padded to KEYSIZE
     */      
    private long binary_search (RandomAccessFile file, String key, long posL, long posH) throws IOException {
        try {
            byte[] buf = new byte[KEYSIZE];
            long mid = (posL+posH)/2;
            file.seek(mid*ENTRYSIZE);
            file.read(buf);
            
            if (key.equals(new String(buf)) || mid == posL)
                return mid;
            if (key.compareTo(new String(buf)) > 0) {
                return binary_search(file,key,mid,posH);
            }
            return binary_search(file,key,posL,mid);
        }
        catch (IOException ioe) {
            throw ioe;
        }   
    }    

    /**
     *   inserts a (key,datapos) tuple into the index structure
     *   updates datapos if key is already in the index   
     */    
    private void insert_index (RandomAccessFile RAfile, String key, long datapos) throws IOException {
        String padded_key = pad_key(key);
        byte keysize = (byte)key.length();
         try {
             // check if file is empty
             if (RAfile.length() == 0) {
                 RAfile.seek(0);
                 
                 // do the insert
                 RAfile.write(padded_key.getBytes());
                 RAfile.writeLong(datapos);
                 RAfile.writeByte(keysize);
                 RAfile.writeByte((byte)0);
                 return;
             }

             // check if key can be inserted at the end
             byte[] buf = new byte[KEYSIZE];
             RAfile.seek(RAfile.length()-ENTRYSIZE);
             RAfile.read(buf);  
             if (RAfile.length() == ENTRYSIZE && padded_key.equals(new String(buf))) {
                 RAfile.writeLong(datapos);
                 return;
             }
             if (padded_key.compareTo(new String(buf)) > 0) {
                 RAfile.seek(RAfile.length());
                 
                 // do the insert
                 RAfile.write(padded_key.getBytes());
                 RAfile.writeLong(datapos);
                 RAfile.writeByte(keysize);
                 RAfile.writeByte((byte)0);                 
                 return;
             }

             // check if key can be inserted at the start             
             long inspos = -1;
             RAfile.seek(0);
             RAfile.read(buf); 
             if (padded_key.compareTo(new String(buf)) < 0)
                inspos = 0;

             // search the file
             if (inspos < 0) {
                long idx = binary_search(RAfile,padded_key,0,RAfile.length()/ENTRYSIZE-1);
                RAfile.seek(idx*ENTRYSIZE);
                RAfile.read(buf); 
                if (padded_key.equals(new String(buf))) {
                    RAfile.writeLong(datapos);
                    return;
                }
                inspos = idx*ENTRYSIZE + ENTRYSIZE;
                //RAfile.skipBytes(INDEXSIZE);
                RAfile.seek(inspos);
                RAfile.read(buf); 
                if (padded_key.equals(new String(buf))) {
                    RAfile.writeLong(datapos);
                    return;   
                }
             }
             
             // shift block by block            
             long cur_pos = RAfile.length();
             byte[] shift_buf = new byte[MAX_ARRAY_SIZE];
             while(cur_pos > inspos) {  
                 long size = cur_pos - inspos;
                 if (size > MAX_ARRAY_SIZE) {
                     size = MAX_ARRAY_SIZE;
                     cur_pos -= MAX_ARRAY_SIZE;
                 }
                 else
                     cur_pos = inspos;
                 
                 RAfile.seek(cur_pos);
                 RAfile.read(shift_buf,0,(int)size);                 
                 RAfile.seek(cur_pos+ENTRYSIZE);
                 RAfile.write(shift_buf,0,(int)size);   
             }
             
             // do the insert
             RAfile.seek(inspos);
             RAfile.write(padded_key.getBytes());   
             RAfile.writeLong(datapos);  
             RAfile.writeByte(keysize);
             RAfile.writeByte((byte)0);             
        }
        catch (IOException ioe) {
            throw ioe;
        }            
    }    
    
    /**
     *   delete a key from the index structure
     *   returns the datapos connected to the key or -1 if the key was not found 
     *   this assumes that the input key is padded to KEYSIZE
     */    
    private long delete_index (RandomAccessFile RAfile, String key) throws IOException {
         try {
             long filepos = find_index(RAfile,key);
             if (filepos < 0)
                 return -1;
             filepos *= ENTRYSIZE;

             // get page index
             RAfile.seek(filepos+KEYSIZE);
             long datapos = RAfile.readLong();
             
             // check if key is the last one in the array
             if (filepos == RAfile.length() - ENTRYSIZE) {
                 RAfile.setLength(RAfile.length() - ENTRYSIZE);
                 return datapos;
             }            
             
             // shift block by block            
             long cur_pos = filepos+ENTRYSIZE;
             byte[] shift_buf = new byte[MAX_ARRAY_SIZE];
             while(cur_pos < RAfile.length()) {  
                 long size = RAfile.length() - cur_pos;
                 if (size > MAX_ARRAY_SIZE)
                     size = MAX_ARRAY_SIZE;
                 
                 RAfile.seek(cur_pos);
                 RAfile.read(shift_buf,0,(int)size);                 
                 RAfile.seek(cur_pos-ENTRYSIZE);
                 RAfile.write(shift_buf,0,(int)size);   

                 cur_pos += size;
             }
               
             return datapos;
        }
        catch (IOException ioe) {
            throw ioe;
        }         
    }       
    
    /**
     *   find a key in the index
     *   returns the position of the key in the sorted array or -1 if the key was not found 
     *   this assumes that the input key is padded to KEYSIZE
     */        
    private long find_index (RandomAccessFile RAfile, String key) throws IOException {
        try {
             // check if file is empty
             if (RAfile.length() == 0) {
                 return -1;
             }

             // check if key is larger than max
             byte[] buf = new byte[KEYSIZE];
             RAfile.seek(RAfile.length()-ENTRYSIZE);
             RAfile.read(buf); 
             if (RAfile.length() == ENTRYSIZE && key.equals(new String(buf)))
                 return 0;
             if (key.compareTo(new String(buf)) > 0) {
                 return -1;
             }

             // check if key is smaller than min
             RAfile.seek(0);
             RAfile.read(buf); 
             if (key.compareTo(new String(buf)) < 0) {
                return -1;
             }

            long idx = binary_search(RAfile,key,0,RAfile.length()/ENTRYSIZE-1);
            RAfile.seek(idx*ENTRYSIZE);
            RAfile.read(buf); 
            if (key.equals(new String(buf)))
                return idx;
            //RAfile.skipBytes(INDEXSIZE);
            RAfile.seek(idx*ENTRYSIZE + ENTRYSIZE);
            RAfile.read(buf); 
            if (key.equals(new String(buf)))
                return idx+1;   
        }
        catch (IOException ioe) {
            throw ioe;
        }   
        return -1;
    }    
    
    /**
     *   find a key in the index structure
     *   returns the datapos connected to the key or -1 if the key was not found 
     *   this assumes that the input key is padded to KEYSIZE
     */        
    private long get_datapos (RandomAccessFile RAfile, String key) throws IOException {
         try {
             long filepos = find_index(RAfile,key);
             if (filepos < 0)
                 return -1;
             filepos *= ENTRYSIZE;
             RAfile.seek(filepos+KEYSIZE);
             return RAfile.readLong();            
        }
        catch (IOException ioe) {
            throw ioe;
        }         
    }    
    
    private String pad_key (String key) {
        String padded_key = key;
        while (padded_key.length() < KEYSIZE) {
            padded_key += "A";
        }            
        return padded_key;
    }
}
