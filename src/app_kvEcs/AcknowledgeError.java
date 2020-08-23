/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package app_kvEcs;

import common.logger.Constants;
import common.messages.AdminMessage;
import common.reader.UniversalReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
//import java.util.logging.Level;
//import java.util.logging.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * writes error into buffer.
 */
public class AcknowledgeError implements Runnable {
    private Socket errorSocket;
    private ErrorBuffer buffer;
    private static final Logger LOGGER = LogManager.getLogger(Constants.SERVER_NAME);
    
    public AcknowledgeError(Socket errorSocket, ErrorBuffer buffer) {
        this.errorSocket = errorSocket;
        this.buffer = buffer;
    }
    
    @Override
    public void run() {
        // Read ErrorMessage
        AdminMessage confMessage = null;
        LOGGER.debug("AcknowledgeError run start");
        
        try {
            UniversalReader confReader = new UniversalReader();
            confMessage = new AdminMessage(confReader.readMessage(errorSocket.getInputStream()));
        } catch (IOException ex) {
           // what to do ? 
        }
        
        
        if(!confMessage.getValid()) {
            // if not - ?
        } else {
            buffer.lock();  // Aquire Lock on Buffer
            LOGGER.debug("AcknowledgeError added to buffer");
            buffer.add(confMessage.getPayload()); // Write into Buffer 
            buffer.unlock(); // Unlock Buffer
        }
    }
}
