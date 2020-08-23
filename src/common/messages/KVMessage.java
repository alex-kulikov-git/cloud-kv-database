package common.messages;

public interface KVMessage {

    /**
     * byte range 1 to 20
     */
    public enum StatusType {
        GET, 			/* Get - request */
        GET_ERROR, 		/* requested tuple (i.e. value) not found */
        GET_SUCCESS, 	/* requested tuple (i.e. value) found */
        PUT, 			/* Put - request */
        PUT_SUCCESS, 	/* Put - request successful, tuple inserted */
        PUT_UPDATE, 	/* Put - request successful, i.e. value updated */
        PUT_ERROR, 		/* Put - request not successful */
        DELETE, 		/* Delete - request */
        DELETE_SUCCESS, /* Delete - request successful */
        DELETE_ERROR, 	/* Delete - request successful */
        FAILED,			/* Any request - unknown error */
        NOT_RESPONSIBLE,	/* Wrong server addressed - meta data has to be updated */
        SERVER_WRITE_LOCK,	/* Server currently only accepts read/get requests */
        SERVER_STOPPED,	/* Server not accepting queries */
        AUTH,           /* Authentification message */ 
        AUTH_SUCCESS,   /* Authentification success */
        AUTH_ERROR,		/* Authentification error */
        SUB,            /* Subscribe message */
        SUB_SUCCESS,    /* Subscription success */
        SUB_ERROR,		/* Subscription error */
        UNSUB			/* Unsubscribe */
    }

    public byte[] getKeyAsBytes();
    
    public byte[] getValueAsBytes();
    
    /**
     * @return the key that is associated with this message,
     * null if not key is associated.
     */
    public String getKey();

    /**
     * @return the value that is associated with this message,
     * null if not value is associated.
     */
    public String getValue();

    /**
     * @return a status string that is used to identify request types,
     * response types and error types associated to the message.
     */
    public StatusType getStatus();

}


