package common.messages;

import java.nio.ByteBuffer;

public class Message implements KVMessage {

    private byte[] byteMessage; 
    private boolean valid;
    
    /**
     * The programmer creates a new message.
     * @param status - given status
     * @param key - given key
     * @param value - given value
     */
    public Message(StatusType status, byte[] key, byte[] value) { // I trust in us to correctly use this constructor
        this.valid = true;
        this.byteMessage = composeMessage(status, key, value);
    }
    
    public Message(StatusType status, byte[] key) { // for short Messages
        this.valid = true;
        this.byteMessage = composeMessage(status, key);       
    }
    
    public Message(StatusType status) { // for byte Messages
        this.valid = true;
        this.byteMessage = composeMessage(status);
    }
    
    /*
    
    GET                 - status | key_length | key | -
    GET_ERROR           - status | key_lenght | key | -
    GET_SUCCESS         - status | key_length | key | value_length | value
    PUT                 - status | key_length | key | value_length | value
    PUT_SUCCESS         - status | key_length | key | value_length | value
    PUT_UPDATE          - status | key_length | key | value_length | value
    PUT_ERROR           - status | key_length | key | value_length | value
    DELETE              - status | key_length | key | -
    DELETE_SUCCESS      - status | key_length | key | -
    DELETE_ERROR        - status | key_length | key | -
    FAILED              - status | key_length | key | message_length | message
    NOT_RESPONSIBLE     - status | key_length | key | message_length | metaData
    SERVER_STOPPED      - status
    SERVER_WRITE_LOCK   - status
    AUTH                - status | key_length | key | message_length | user | pw-hash (fixed hash length) 
    AUTH_SUCCESS        - status
    AUTH_ERROR          - status
    SUB                 - status | key_length | key | value_length | value
    SUB_SUCCESS         - status 
    SUB_ERROR           - status
    UNSUB               - status | key_length | key | - 
    
    two groups - one with value, one without payload:
    without: GET, GET_ERROR, DELETE, DELETE_ERROR, DELETE_SUCCESS
    
    check, if status is valid
        
    for short and long
        check, if key_length is valid
    
    for short
        check, if remaining bytes have key length only
    
    for long
        check, if value length and remaining message add up
    */
    
    
    /* Constructors --------------------------------------------------------- */
    
    /**
     * Used to create a message out of received bytes.
     * valid is set to false, if the message format is not valid.
     * @param byteMessage - received bytes
     */
    public Message(byte[] byteMessage) { 
        this.valid = true; 
        this.byteMessage = byteMessage;
        
        if(validStatus(byteMessage[0])) {
            if(messageIsByte(byteToStatus(byteMessage[0]))) {
                this.valid &= byteMessage.length == 1;
            } else if(messageIsShort(byteToStatus(byteMessage[0]))) {
                this.valid &= validKeyLength(byteMessage[1]) && restIsKey(byteMessage);
            } else {
                this.valid &= validKeyLength(byteMessage[1]) && mathIsFine(byteMessage) && validPayloadLength(byteMessage);
            }
        } else {
            this.valid = false;
        }
        
    }
        
    private boolean messageIsByte(StatusType status) {
        switch(status) {
            case SERVER_STOPPED: return true;
            case SERVER_WRITE_LOCK: return true;
            case AUTH_SUCCESS: return true;
            case AUTH_ERROR: return true;
            case SUB_SUCCESS: return true;
            case SUB_ERROR: return true;
            default: return false;
        }
    }
    
    /**
     * Answers according to an already set status flag, if the message has no value field
     * @return no value field - true; value field - false
     */
    private boolean messageIsShort(StatusType status) {
        switch(status) {
            case GET: return true;
            case GET_ERROR: return true;
            case DELETE: return true;
            case DELETE_ERROR: return true;
            case DELETE_SUCCESS: return true;
            case UNSUB: return true;
            default: return false;
        }
    }
    
    /* Switches ------------------------------------------------------------- */
    
    /**
     * Parsing status to status byte.
     * @param status - given status
     * @return status byte
     */
    private byte statusToByte(StatusType status) {
        switch(status) {
            case GET: return (byte) 1;
            case GET_ERROR: return (byte) 2;
            case GET_SUCCESS: return (byte) 3;
            case PUT: return (byte) 4;
            case PUT_SUCCESS: return (byte) 5;
            case PUT_UPDATE: return (byte) 6;
            case PUT_ERROR: return (byte) 7;
            case DELETE: return (byte) 8;
            case DELETE_SUCCESS: return (byte) 9;
            case DELETE_ERROR: return (byte) 10;
            case FAILED: return (byte) 11;
            case NOT_RESPONSIBLE: return (byte) 12;
            case SERVER_STOPPED: return (byte) 14;
            case SERVER_WRITE_LOCK: return (byte) 15;
            case AUTH: return (byte) 16;
            case AUTH_SUCCESS: return (byte) 17;
            case AUTH_ERROR: return (byte) 18;
            case SUB: return (byte) 19;
            case SUB_SUCCESS: return (byte) 20;
            case SUB_ERROR: return (byte) 61;
            case UNSUB: return (byte) 62;
            default: throw new RuntimeException("status byte not valid - programmers fault"); // WRONG STATUS TYPE
        }
    }
    
    /**
     * Decoding status byte.
     * Sets valid to false, if the status byte is not representing a valid status.
     * @param status - given status byte
     * @return status if valid or null if not valid
     */
    private StatusType byteToStatus(byte status) {
        switch((int) status) {
            case 1: return StatusType.GET;
            case 2: return StatusType.GET_ERROR;
            case 3: return StatusType.GET_SUCCESS;
            case 4: return StatusType.PUT;
            case 5: return StatusType.PUT_SUCCESS;
            case 6: return StatusType.PUT_UPDATE; 
            case 7: return StatusType.PUT_ERROR; 
            case 8: return StatusType.DELETE; 
            case 9: return StatusType.DELETE_SUCCESS;
            case 10: return StatusType.DELETE_ERROR;
            case 11: return StatusType.FAILED;
            case 12: return StatusType.NOT_RESPONSIBLE;
            case 14: return StatusType.SERVER_STOPPED;
            case 15: return StatusType.SERVER_WRITE_LOCK;
            case 16: return StatusType.AUTH;
            case 17: return StatusType.AUTH_SUCCESS;
            case 18: return StatusType.AUTH_ERROR;
            case 19: return StatusType.SUB;
            case 20: return StatusType.SUB_SUCCESS;
            case 61: return StatusType.SUB_ERROR;
            case 62: return StatusType.UNSUB;
            default: 
                valid = false;
                return null;// WRONG STATUS TYPE - RETURNING NULL SHOULD NOT MATTER
        }
    }
    
    
    /* Validation ----------------------------------------------------------- */
    
    private boolean validStatus(byte status) {
        switch((int) status) {
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
            case 8:
            case 9:
            case 10:
            case 11:
            case 12:
            case 14:
            case 15:
            case 16:
            case 17:
            case 18:
            case 19:
            case 20:
            case 61:
            case 62:
                return true;
            default: 
                return false;
        }
    }
    
    /**
     * Validates the key length
     * @param key_length
     * @return 
     */
    private boolean validKeyLength(byte key_length) {
        if((int) key_length > 20 || (int) key_length < 1) {
            return false;
        }
        return true;
    }
    
    /**
     * Checks, if the rest of the message contains the key only
     * @param byteMessage
     * @return indicator
     */
    private boolean restIsKey(byte[] byteMessage) {
        return (byteMessage.length - 1 - 1 - (int) byteMessage[1]) == 0;
    }
    
    /**
     * Checks format in general.
     * @param byteMessage
     * @return indicator
     */
    private boolean mathIsFine(byte[] byteMessage) {
        if(byteMessage.length < 1 + 1 + (int) byteMessage[1] + 4 + 1) { // status | key_length | key | 4 bytes | min. 1 byte
            return false;
        }
        byte[] payloadLength = new byte[4];
        System.arraycopy(byteMessage, 1 + 1 + (int) byteMessage[1], payloadLength, 0, 4);
        return (byteMessage.length - 1 - 1 - (int) byteMessage[1] - 4 - byteArrayToLength(payloadLength)) == 0;
    }
    
    /**
     * Checks, if the payload does not exceed 120 kb
     * @param byteMessage
     * @return indicator
     */
    private boolean validPayloadLength(byte[] byteMessage) {
        byte[] payloadLength = new byte[4];
        System.arraycopy(byteMessage, 1 + 1 + (int) byteMessage[1], payloadLength, 0, 4);
        return byteArrayToLength(payloadLength) < 120000;
    }
    
    
    /* Helper --------------------------------------------------------------- */
    
    private int byteArrayToLength(byte[] length) {
        return ByteBuffer.wrap(length).getInt();
    }
   
    
    
    /* Composer ------------------------------------------------------------- */
    
    /**
     * Parses all given fields to byte and concatenates these byte arrays.
     * @param status - given status
     * @param key - given key
     * @param value - given value
     * @return status | length | key | (value) as byte array
     */
    private byte[] composeMessage(StatusType status, byte[] key, byte[] value) { // composes long message
        byte[] byteMessage = new byte[1 + 1 + key.length + 4 + value.length];
        byteMessage[0] = statusToByte(status); // writes status byte
        byteMessage[1] = (byte) key.length; // writes length byte
        System.arraycopy(key, 0, byteMessage, 2, key.length); // writes key
        System.arraycopy(ByteBuffer.allocate(4).putInt(value.length).array(), 0, byteMessage, 1 + 1 + key.length, 4); // writes valueLength
        System.arraycopy(value, 0, byteMessage, 1 + 1 + key.length + 4, value.length); // writes value
        
        return byteMessage;
    }
    
    private byte[] composeMessage(StatusType status, byte[] key) { // composes short message
        byte[] byteMessage = new byte[1 + 1 + key.length];
        byteMessage[0] = statusToByte(status);
        byteMessage[1] = (byte) key.length;
        System.arraycopy(key, 0, byteMessage, 2, key.length);
        
        return byteMessage;
    }
    
    private byte[] composeMessage(StatusType status) { // composes byte message
        byte[] statusByte = new byte[1];
        statusByte[0] = statusToByte(status);
        return statusByte;
    }
    
    /* Gets ----------------------------------------------------------------- */
    
    @Override
    public byte[] getKeyAsBytes() { 
        byte[] key = new byte[(int) byteMessage[1]];
        System.arraycopy(byteMessage, 2, key, 0, byteMessage[1]);
        return key;
    }
    
    @Override
    public byte[] getValueAsBytes() {
        byte[] payloadLength = new byte[4];
        System.arraycopy(byteMessage, 1 + 1 + (int) byteMessage[1], payloadLength, 0, 4);
        byte[] payload = new byte[byteArrayToLength(payloadLength)];
        System.arraycopy(byteMessage, 1 + 1 + (int) byteMessage[1] + 4, payload, 0, byteArrayToLength(payloadLength));
        return payload;
    }
   
    
    @Override
    public String getKey() {
        return new String(getKeyAsBytes());
    }

    @Override
    public String getValue() {
        return new String(getValueAsBytes());
    }

    @Override
    public StatusType getStatus() {
        return byteToStatus(byteMessage[0]);
    }
    
    public byte[] getByteMessage() {
        return byteMessage;
    }
    
    public boolean getValid() {
        return valid;
    }
    
    public void setValid(boolean valid) {
        this.valid = valid;
    }
}
