/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package app_kvServer;

import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import java.util.Properties;
import javax.mail.*;
import javax.mail.internet.*;

/**
 *
 * @author kajo
 */
public class EMailService extends Thread {
    private String address;
    private StatusType status;
    private KVMessage kvMessage; // ACHTUNG, DIE MAIL LIB HAT AUCH EINE MESSAGE KLASSE!
    
    public EMailService(String address, StatusType status, KVMessage kvMessage) {
        this.address = address;
        this.status = status;
        this.kvMessage = kvMessage;
    }
    
    @Override
    public void run() {
        final String username = "cloudgroup01@gmail.com";
        final String password = "thisisderp";

        Properties props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.host", "smtp.gmail.com");
        props.put("mail.smtp.port", "587");

        Session session = Session.getInstance(props,
            new javax.mail.Authenticator() {
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(username, password);
                }
        });

        try {
            Message message = new MimeMessage(session);
            message.setFrom(new InternetAddress("cloudgroup01@gmail.com"));
            message.setRecipients(Message.RecipientType.TO,
                    InternetAddress.parse(address));
            message.setSubject("Update on " + kvMessage.getKey());

            // Different Message, if Key has been deleted?
            String text = "Dear User, \n\n";

            switch(status) {
                case PUT_UPDATE:
                    text += "your key has been updated to: " + kvMessage.getValue() + ".";
                    break;
                case DELETE_SUCCESS:
                    text += "your key has been deleted.";
                    break;
                case SUB:
                    text += "you have subscribed to the key: " + kvMessage.getKey() + ".";
                    break;
                case UNSUB:
                    text += "you have unsubscribed from the key: " + kvMessage.getKey() + ".";
            }

            message.setText(text);
            Transport.send(message);

        } catch (MessagingException e) {
                throw new RuntimeException(e);
        }
    }
}
    

