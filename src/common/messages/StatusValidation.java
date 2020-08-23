/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package common.messages;

/**
 *
 * @author kajo
 */
public interface StatusValidation {
    public static boolean validKVStatus(byte status) {
        switch((int) status) {
            case 1:// GET  
            case 2:// GET_ERROR
            case 3:// GET_SUCCESS
            case 4:// PUT
            case 5:// PUT_SUCCESS
            case 6:// PUT_UPDATE
            case 7:// PUT_ERROR
            case 8:// DELETE
            case 9:// DELETE_SUCCESS
            case 10:// DELETE_ERROR
            case 11:// FAILED
            case 12:// NOT_RESPONSIBLE
            case 14:// SERVER_STOPPED
            case 15:// SERVER_WRITE_LOCK
            case 16:// AUTH
            case 17:// AUTH_SUCCESS
            case 18:// AUTH_ERROR
            case 19:// SUB
            case 20:// SUB_SUCCESS 
            case 61:// SUB_ERROR
            case 62:// UNSUB
                return true;
            default: 
                return false;
        }
    }
    
    public static boolean validAdminStatus(byte status) {
        switch((int) status) {
            /* AdminMessage */
            case 21:// META_DATA 
            case 22:// START 
            case 23:// STOP 
            case 24:// SHUT_DOWN  
            case 25:// LOCK_WRITE  
            case 26:// UNLOCK_WRITE  
            case 27:// MOVE_DATA 
            case 28:// PING 
            case 29:// CRASH
            case 30:// REPLICATE_DATA
            case 31:// DELETE_DATA

            /* ConfirmationMessage */
            case 41:// RECEIVED_AND_EXECUTED
            case 42:// AN_ERROR_OCCURED
            case 43:// SERVER_DOWN
                return true;
            default: return false;
        }
    }
}
