/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package common.reader;

import common.messages.StatusValidation;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class UniversalReader {
    protected byte[] extractFromBuffer(byte[] buffer, int messageLength) { 
        byte[] readMessage = new byte[messageLength];
        System.arraycopy(buffer, 0, readMessage, 0, messageLength);
        return readMessage;
    }
    
    protected void readBytes(byte[] buffer, int numberOfBytes, InputStream in) throws IOException, RuntimeException {
        int receiving;
        for(int i = 0; i < numberOfBytes; i++) {
            receiving = in.read();
            if(receiving == -1) throw new RuntimeException("disconnected");
            buffer[i] = (byte) receiving;
        }
    }
    
    /**
     * Read a message from an input stream.
     * Can handle both KV messages and Admin messages.
     * @param in the input stream
     * @return the read message
     * @throws IOException
     * @throws RuntimeException
     */
    public byte[] readMessage(InputStream in) throws IOException, RuntimeException {
        byte[] buffer = new byte[120030];
        int messageLength = 0;
        byte statusByte;
        readBytes(buffer, 1, in);
        statusByte = buffer[0];
        messageLength += 1;
     
        if(StatusValidation.validAdminStatus(statusByte)) {
            // reading an admin message
            PayloadReader confReader = new PayloadReader();
            return confReader.readPayloadMessage(in, buffer, messageLength);
        } else {
            // reading a KV message
            KVReader confReader = new KVReader();
            return confReader.readKVMessage(in, buffer, messageLength);
        }       
    }
    
    
    private class KVReader extends UniversalReader {   
        private boolean messageIsByte(byte status) {
            switch((int) status) {
                case 14:// SERVER_STOPPED  
                case 15:// SERVER_WRITE_LOCK
                case 17:// AUTH_SUCCESS
                case 18:// AUTH_ERROR
                case 20:// SUB_SUCCESS
                case 61:// SUB_ERROR
                    return true;
                default: return false;
            }
        }


        /**
         * Answers according to an already set status flag, if the message has no value field
         * @return no value field - true; value field - false
         */
        private boolean messageIsShort(byte status) {
            switch((int) status) {
                case 1:// GET
                case 2:// GET_ERROR
                case 8:// DELETE
                case 10:// DELETE_ERROR
                case 9:// DELETE_SUCCESS
                case 62:// UNSUB
                    return true;
                default: 
                    return false;
            }
        }


        public byte[] readKVMessage(InputStream in, byte[] buffer, int messageLength) throws IOException, RuntimeException {
            byte statusByte;
            statusByte = buffer[0];

            if(StatusValidation.validKVStatus(statusByte)) {
                if(!messageIsByte(statusByte)) {
                    byte[] keyLength = new byte[1];
                    readBytes(keyLength, 1, in);
                    System.arraycopy(keyLength, 0, buffer, messageLength, keyLength.length);
                    messageLength += 1;
                    byte[] key = new byte[(int) keyLength[0]];
                    readBytes(key, key.length, in);
                    System.arraycopy(key, 0, buffer, messageLength, key.length);
                    messageLength += key.length;
                    if(!messageIsShort(statusByte)) {
                        byte[] payloadLength = new byte[4];
                        readBytes(payloadLength, payloadLength.length, in);
                        System.arraycopy(payloadLength, 0, buffer, messageLength, payloadLength.length);
                        messageLength += 4;
                        byte[] payload = new byte[ByteBuffer.wrap(payloadLength).getInt()];

                        if(payload.length + messageLength < buffer.length) {
                            readBytes(payload, payload.length, in);
                            System.arraycopy(payload, 0, buffer, messageLength, payload.length);
                            messageLength += payload.length;
                        }
                    }
                }
            }

            return extractFromBuffer(buffer, messageLength);
        }
    }
    
        
    private class PayloadReader extends UniversalReader {
        private boolean messageIsShort(byte status) {
            switch((int) status) {
                /* short AdminMessage */
                case 21:// START
                case 22:// STOP
                case 23:// SHUT_DOWN
                case 25:// LOCK_WRITE
                case 26:// UNLOCK_WRITE
                case 28:// PING
                case 29:// CRASH

                /* short ConfirmationMessage */
                case 41:// RECEIVED_AND_EXECUTED
                case 42:// AN_ERROR_OCCURED
                    return true;

                    
                default: return false; // hence, META_DATA and MOVE_DATA also delivers false
            }
        }


        /**
         * @param in -InputStream
         * @return Delivers the AdminMessage in form of a byte array. 
         * @throws IOException
         * @throws RuntimeException 
         */    
        public byte[] readPayloadMessage(InputStream in, byte[] buffer, int messageLength) throws IOException, RuntimeException {
            byte statusByte;
            statusByte = buffer[0];
            
            if(StatusValidation.validAdminStatus(statusByte) && !messageIsShort(statusByte)) { 
                byte[] payloadLength = new byte[4];
                readBytes(payloadLength, payloadLength.length, in);
                System.arraycopy(payloadLength, 0, buffer, messageLength, payloadLength.length);
                messageLength += 4;
                byte[] payload = new byte[ByteBuffer.wrap(payloadLength).getInt()];

                if(payload.length + messageLength < buffer.length) {
                    readBytes(payload, payload.length, in);
                    System.arraycopy(payload, 0, buffer, messageLength, payload.length);
                    messageLength += payload.length;
                }
            }

            return extractFromBuffer(buffer, messageLength);

        }
    }
    
}
