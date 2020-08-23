package app_kvServer;

import common.constants.PortOffset;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import common.messages.KVMessage.StatusType;
import common.messages.*;
import manager.CacheManager;
import common.logger.*;
import common.reader.UniversalReader;
import java.nio.charset.StandardCharsets;
import manager.SubscriptionManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Handles the accepted connection.
 *
 */
public class HandleConnection implements Runnable {

	private static final Logger LOGGER = LogManager.getLogger(Constants.SERVER_NAME);

	private InputStream in;
	private OutputStream out;
	private CacheManager manager;
	private MetaData metaData;
	private String clientName; //client address - includes client port
	private String serverName; //server address
	private int port; //server port
	private Users userList;
	private SubscriptionManager subManager;

	HandleConnection(Socket client, CacheManager manager, MetaData metaData, Users userList, SubscriptionManager subManager) {
            try {
                this.in = client.getInputStream();
                this.out = client.getOutputStream();
                this.clientName = client.getRemoteSocketAddress().toString();
                this.serverName = client.getInetAddress().getHostAddress();
                this.port = client.getLocalPort();
                LOGGER.info("Accepted connection from: " + this.clientName);
                this.manager = manager;
                this.metaData = metaData;
                this.userList = userList;
                this.subManager = subManager;
            } catch (IOException io) {
                LOGGER.error("Could not accept client.");
            }
	}

	/**
	 * sends given message to client
	 *
	 * @param reply - given message
	 */
	private void sendReply(Message reply) {
            System.out.println("Sending: " + reply.getByteMessage().length);
            try {
                for (byte b : reply.getByteMessage()) {
                        out.write(b);
                }

                out.write("\r".getBytes(StandardCharsets.UTF_8));

                out.flush();
            } catch (IOException io) {
                // what to do, if the reply fails?
            }
	}

	/**
	 * Sends an Message to a server.
	 *
	 * @param out
	 * @throws IOException if unable to send message
	 */
	private void sendReplication(Message message, OutputStream out) throws IOException { // Maybe merge this with sendReply() ?
            byte[] byteMessage = message.getByteMessage();

            for (byte b : byteMessage) {
                out.write(b);
            }
            out.flush();
	}

	/**
	 * Replicates the message with the passed parameters to the successors of
	 * this server.
	 *
	 * @param message
	 */
	private void replicate(Message message) {
            // determine successors
            MetaDataEntry successor1 = metaData.getSuccessor(this.serverName, this.port);
            MetaDataEntry successor2 = metaData.getSuccessor(successor1.getIP(), successor1.getPort());

            String firstIP = successor1.getIP();
            int firstPort = PortOffset.getGossipPort(successor1.getPort()); // gossip offset
            LOGGER.info("succ1 ip " + successor1.getIP());
            LOGGER.info("succ1 port " + successor1.getPort());

            String secondIP = successor2.getIP();
            int secondPort = PortOffset.getGossipPort(successor2.getPort()); // gossip offset
            LOGGER.info("succ1 ip " + successor1.getIP());
            LOGGER.info("succ1 port " + successor1.getPort());

            Socket socket = null;
            ErrorManager errorManager = new ErrorManager();

            // we have to be careful to not use the class variables in and out
            OutputStream outputStream = null;
            InputStream inputStream = null;
            UniversalReader ur = new UniversalReader();
            int hack = -1;

            // send message to first successor
            try {
                socket = new Socket(firstIP, firstPort); // initial connection
                inputStream = socket.getInputStream();
                outputStream = socket.getOutputStream();

                Message connected = new Message(ur.readMessage(inputStream)); // init message
                if (!connected.getStatus().equals(StatusType.PUT)) {
                    LOGGER.error("did not receive confirmation.: " + connected.getByteMessage()[0]);
                    return; // abort replication
                }

                hack = inputStream.read();
                if (hack == -1) {
                    throw new IOException();
                } else if (!(hack == 13)) {
                    LOGGER.error("did not receive carriage return.");
                    return; // abort replication
                }

                sendReplication(message, outputStream);

                Message reply = new Message(ur.readMessage(inputStream));
                LOGGER.info("received status byte: " + reply.getByteMessage()[0]);

                hack = inputStream.read();
                if (hack == -1) {
                    throw new IOException();
                } else if (!(hack == 13)) {
                    LOGGER.error("did not receive carriage return.");
                    return; // abort replication
                }

                if (!(reply.getStatus().equals(StatusType.PUT_SUCCESS) || reply.getStatus().equals(StatusType.PUT_UPDATE) || reply.getStatus().equals(StatusType.DELETE_SUCCESS))) {
                    LOGGER.error("Replication failed on successor 1.");
                }
            } catch (IOException e) {
                    LOGGER.error("sending error message to ecs.");
                    errorManager.sendServerDown(firstIP, firstPort);
            }

            try {
                    inputStream.close();
                    outputStream.close();
                    socket.close();
            } catch (IOException ex) {
                    LOGGER.error("Unable to close streams or socket");
            }

            // send message to second successor
            try {
                socket = new Socket(secondIP, secondPort);
                inputStream = socket.getInputStream();
                outputStream = socket.getOutputStream();

                Message connected = new Message(ur.readMessage(inputStream)); // init message
                if (!connected.getStatus().equals(StatusType.PUT)) {
                    LOGGER.error("did not receive confirmation.");
                    return; // abort replication
                }

                hack = inputStream.read();
                if (hack == -1) {
                    throw new IOException();
                } else if (!(hack == 13)) {
                    LOGGER.error("did not receive carriage return.");
                    return; // abort replication
                }

                sendReplication(message, outputStream);

                Message reply = new Message(ur.readMessage(inputStream)); // what to do with /r ?
                LOGGER.info("received status byte: " + reply.getByteMessage()[0]);

                hack = inputStream.read();
                if (hack == -1) {
                    throw new IOException();
                } else if (!(hack == 13)) {
                    LOGGER.error("did not receive carriage return.");
                    return; // abort replication
                }

                if (!(reply.getStatus().equals(StatusType.PUT_SUCCESS) || reply.getStatus().equals(StatusType.PUT_UPDATE) || reply.getStatus().equals(StatusType.DELETE_SUCCESS))) {
                    LOGGER.error("Replication failed on successor 2.");
                }
            } catch (IOException e) {
                LOGGER.error("sending error message to ecs.");
                errorManager.sendServerDown(secondIP, secondPort);
            }

            try {
                inputStream.close();
                outputStream.close();
                socket.close();
            } catch (IOException ex) {
                LOGGER.error("Unable to close streams or socket");
            }
	}

        @Override
	public void run() {
            boolean connected = true;
            UniversalReader reader = new UniversalReader();

            /*
            System.out.println("server " + serverName);
            System.out.println("client " + clientName);
             */
            System.out.println("ServerName: " + serverName);
            System.out.println("Port: " + Integer.toString(port));

            LOGGER.info("before sendRyply");
            sendReply(new Message(StatusType.PUT, serverName.getBytes(), Integer.toString(port).getBytes())); // client should know, that port is not coded as string
            LOGGER.info("after sendReply");

            /*
                AUTHENTIFICATION
             */
            byte[] authBytes = null;
            Message toDo = null;

            try {
                authBytes = reader.readMessage(in);
            } catch (IOException | RuntimeException io_run) {
                System.out.println("Something went wrong.");
                connected = false;
            }

            LOGGER.info("received auth bytes");

            toDo = new Message(authBytes);
            AuthTuple user_pw = null;
            if (toDo.getValid()) {
                user_pw = new AuthTuple(toDo.getValueAsBytes());
                LOGGER.info("handle - user: " + user_pw.getUser());
                String derp = "";
                for (int i = 0; i < 16; i++) {
                    derp += user_pw.getPwHash()[i];
                    if (i < 15) {
                        derp += "-";
                    }
                }
                LOGGER.info("handle - pw: " + derp);
                connected = userList.valid(user_pw);
            } else {
                connected = false;
            }

            if (connected) {
                sendReply(new Message(StatusType.AUTH_SUCCESS));
            } else {
                sendReply(new Message(StatusType.AUTH_ERROR));
            }

            while (connected) { // waiting for input
                // status byte + length byte + 20 bytes key-max. + 120 kb = 120 022 byte
                byte[] incomingMinimal;
                toDo = null;

                // listens to stream as long, as connected
                try {
                    incomingMinimal = reader.readMessage(in);
                } catch (IOException | RuntimeException io_run) {
                    System.out.println("Something went wrong.");
                    break;
                }

                toDo = new Message(incomingMinimal);
                System.out.println("Valid is: " + toDo.getValid());

                if (toDo.getValid()) { // reply with FAILED, if message is not valid
                    /* ---------------------------------------------------------- */                    
                    
                    // old debug output
                    /*System.out.println("looking up in metadata: " + serverName + ":" + port);
                    MetaDataEntry e = metaData.getFirst();
                    if (e == null) {
                        System.out.println("meta data empty");
                    } else {
                        System.out.println("first element: " + e.getIP() + ":" + e.getPort());
                    }*/
                    
                    /* ---------------------------------------------------------- */

                    // is the message valid ?
                    // is the server stopped ?
                    // GET - withinGetRange?
                    // PUT/DELETE - withinWritingRange ?
                    //              is the server Stopped?
                    /*
                    if(!metaData.getEntry(serverName, port).withinHashRange(toDo.getKey().getBytes(StandardCharsets.UTF_8)) ) {
                        sendReply(new Message(StatusType.NOT_RESPONSIBLE, "meta".getBytes(), metaData.toBytes()));
                     */
                    if (!manager.isStopped()) {
                        switch (toDo.getStatus()) {
                            case GET:
                                if (metaData.withinReadingRange(this.serverName, this.port, toDo.getKey())) { // within reading range ?
                                    String feedback_get = manager.get(toDo.getKey());

                                    if (feedback_get != null) {
                                            sendReply(new Message(StatusType.GET_SUCCESS, toDo.getKeyAsBytes(), feedback_get.getBytes()));
                                    } else {
                                            sendReply(new Message(StatusType.GET_ERROR, toDo.getKeyAsBytes()));
                                    }
                                } else { // not responsible
                                    sendReply(new Message(StatusType.NOT_RESPONSIBLE, "meta".getBytes(), metaData.toBytes()));
                                }

                                break;
                            case PUT:
                            case DELETE:
                            case SUB:
                            case UNSUB:// it seems that the DELETE flag is not really needed (but old code works fine)
                                if (!metaData.withinWritingRange(this.serverName, this.port, toDo.getKey())) { // not within writing range ?
                                    sendReply(new Message(StatusType.NOT_RESPONSIBLE, "meta".getBytes(), metaData.toBytes()));
                                } else if (manager.isWriteLocked()) {
                                    sendReply(new Message(StatusType.SERVER_WRITE_LOCK));
                                } else {
                                    switch (toDo.getStatus()) { // this is where e-mails may be sent
                                        case PUT:
                                            StatusType feedback_put = manager.put(toDo.getKey(), toDo.getValue());

                                            if (feedback_put.equals(StatusType.PUT_SUCCESS) || feedback_put.equals(StatusType.PUT_UPDATE)) { // send e-mail for update
                                                replicate(toDo);
                                                if (feedback_put.equals(StatusType.PUT_UPDATE) && subManager.isSubscribedTo(toDo.getKey())) {
                                                        // SEND MAIL
                                                        (new EMailService(user_pw.getUser(), feedback_put, toDo)).start();
                                                }
                                                sendReply(new Message(feedback_put, toDo.getKeyAsBytes(), toDo.getValueAsBytes()));

                                            } else {
                                                sendReply(new Message(StatusType.PUT_ERROR, toDo.getKeyAsBytes(), toDo.getValueAsBytes()));
                                            }

                                            break;
                                        case DELETE: // send e-mail for delete
                                            StatusType feedback_del = manager.put(toDo.getKey(), "null");

                                            if (feedback_del.equals(StatusType.DELETE_SUCCESS)) {
                                                if (subManager.isSubscribedTo(toDo.getKey())) {
                                                        (new EMailService(user_pw.getUser(), feedback_del, toDo)).start();
                                                }
                                                replicate(toDo);
                                                sendReply(new Message(StatusType.DELETE_SUCCESS, toDo.getKeyAsBytes()));
                                            } else {
                                                sendReply(new Message(StatusType.DELETE_ERROR, toDo.getKeyAsBytes()));
                                            }

                                            break;
                                        case SUB:
                                            if (!subManager.isSubscribedTo(toDo.getKey())) {
                                                subManager.addSubscription(toDo.getKey(), user_pw.getUser());
                                                (new EMailService(user_pw.getUser(), StatusType.SUB, toDo)).start();
                                            }
                                            sendReply(new Message(StatusType.SUB_SUCCESS));
                                            break;
                                        case UNSUB:
                                            if (subManager.isSubscribedTo(toDo.getKey())) {
                                                subManager.removeSubscription(toDo.getKey(), user_pw.getUser());
                                                (new EMailService(user_pw.getUser(), StatusType.UNSUB, toDo)).start();
                                            }
                                            sendReply(new Message(StatusType.SUB_SUCCESS));
                                    }
                                }
                                break;

                            default:
                                sendReply(new Message(StatusType.FAILED, "-".getBytes(), "INVALID FORMAT".getBytes()));
                                LOGGER.error(clientName + " invalid request");
                                break;
                        }
                    } else {
                        sendReply(new Message(StatusType.SERVER_STOPPED));
                    }
                } else {
                    sendReply(new Message(StatusType.FAILED, "-".getBytes(), "INVALID FORMAT".getBytes()));
                    LOGGER.error(clientName + " invalid request");
                }
            }
            try {
                in.close();
                out.close();
            } catch (IOException e) {
                LOGGER.error("Unable to close streams");
            }
            LOGGER.info(clientName + " disconnected.");
	}
}
