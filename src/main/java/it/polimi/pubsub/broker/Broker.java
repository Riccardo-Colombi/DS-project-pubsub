package it.polimi.pubsub.broker;

import it.polimi.pubsub.broker.components.BrokerHandler;
import it.polimi.pubsub.broker.components.*;
import it.polimi.pubsub.broker.storage.WriteAheadLog;
import it.polimi.pubsub.messages.*;
import it.polimi.pubsub.messages.brokerMessages.BrokerPublishMessage;
import it.polimi.pubsub.messages.brokerMessages.BrokerSubscribeMessage;
import it.polimi.pubsub.messages.brokerMessages.BrokerUnsubscribeMessage;
import it.polimi.pubsub.network.TcpConnection;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

// TODO Update this to be coherent with Raft
// State machine for roles LEADER, FOLLOWER, CANDIDATE
// Add random timout for leader election

public class Broker {

    private final String BROKER_ID;

    private final TopicSubscribersTable topicSubscribersTable;
    private final ConnectionRegistry connectionRegistry;
    private final AckTracker ackTracker;
    private final WriteAheadLog wal;
    private final BrokerRole role;

    Cache<String, Boolean> messageCache = Caffeine.newBuilder()
            .maximumSize(10000)
            .expireAfterWrite(CACHE_LIFETIME, TimeUnit.MINUTES)
            .build();

    private final ExecutorService executor;
    private final ScheduledExecutorService scheduler;
    private AtomicLong brokerCounter;

    private final int CLIENT_PORT;
    private final int BROKER_PORT;

    private final AtomicInteger testCountForSnapshot = new AtomicInteger(0);

    private static final long ACK_TIMEOUT_MS = 5000;
    private static final long RETRY_INTERVAL_MS = 2000;
    private static final long HEARTBEAT_TIMEOUT_MS = 10000;
    private static final long HEARTBEAT_CHECK_INTERVAL_MS = 3000;
    private static final long CACHE_LIFETIME = 2;

    public Broker(String brokerId, int brokerPort, int clientPort, String walFilePath) throws Exception {
        BROKER_ID = brokerId;
        this.CLIENT_PORT = clientPort;
        this.BROKER_PORT = brokerPort;

        this.topicSubscribersTable = new TopicSubscribersTable();
        this.connectionRegistry = new ConnectionRegistry();
        this.ackTracker = new AckTracker();
        this.wal = new WriteAheadLog(walFilePath);
        this.executor = Executors.newCachedThreadPool();
        this.scheduler = Executors.newScheduledThreadPool(2);
        this.brokerCounter = new AtomicLong(0);

        this.role = BrokerRole.FOLLOWER;

        recoverState();
    }

    public void run() {
        try {
            startHeartbeatChecker();
            startRetryScheduler();
            executor.submit(this::runClientListener);
            executor.submit(this::runBrokerListener);
        } catch (Exception e) {
            //TODO the above thread must rethrow the caught exception so we can enter in this catch and gracefully shutdown
            System.err.println("Fatal error: " + e.getMessage());
        }
    }

    private void runClientListener() {
        try (ServerSocket ss = new ServerSocket(CLIENT_PORT)) {

            System.out.println("Listening for clients on " + CLIENT_PORT);

            while (true) {
                Socket socket = ss.accept();
                TcpConnection conn = new TcpConnection(socket);

                ClientHandler handler = new ClientHandler(conn, this, executor);
                handler.start();
            }
        } catch (Exception e) {
            System.err.println("TcpServer error: " + e.getMessage());
        }
    }

    private void runBrokerListener() {
        try (ServerSocket ss = new ServerSocket(BROKER_PORT)) {

            System.out.println("Listening for brokers on " + BROKER_PORT);

            while (true) {
                Socket socket = ss.accept();
                socket.setSoTimeout(5000); // handshake must complete within 5s

                TcpConnection conn = new TcpConnection(socket);
                BrokerHandler handler = new BrokerHandler(conn, this, executor);

                try {
                    handler.performHandshake(BROKER_ID); // blocks until nodeId is known
                    socket.setSoTimeout(0);              // remove timeout for normal ops
                    handler.start();                     // readLoop/writeLoop now safe
                    shareTopicSubscribers(handler);      // nodeId is guaranteed non-null
                } catch (Exception e) {
                    System.err.println("Handshake failed, rejecting connection: " + e.getMessage());
                    conn.close(); // don't let a bad peer into the system
                }
            }
        } catch (Exception e) {
            System.err.println("TcpServer error: " + e.getMessage());
        }
    }

    public void connectToPeer(String brokerHost, int brokerPort) throws Exception {
        Socket socket = new Socket(brokerHost, brokerPort);
        socket.setSoTimeout(5000);

        TcpConnection conn = new TcpConnection(socket);
        BrokerHandler handler = new BrokerHandler(conn, this, executor);

        handler.performHandshake(BROKER_ID);
        socket.setSoTimeout(0);
        handler.start();
        shareTopicSubscribers(handler);
    }

    private void recoverState() throws Exception {
        List<Message> messages = wal.recover();
        System.out.println("Recovering state from WAL: " + messages.size() + " entries.");

        for (Message msg : messages) {
            switch (msg.getType()) {
                case SUBSCRIBE:
                    topicSubscribersTable.addClientSubscription(((SubscribeMessage) msg).getTopic(), msg.getSenderId());
                    break;

                case UNSUBSCRIBE:
                    topicSubscribersTable.removeClientSubscription(((UnsubscribeMessage) msg).getTopic(), msg.getSenderId());
                    break;

                case PUBLISH:
                    PublishMessage pub = (PublishMessage) msg;
                    // Snapshot dei subscriber iscritti in questo preciso
                    // momento del recover: rispecchia chi c'era quando il
                    // messaggio era arrivato al broker originariamente.
                    Set<String> subscribersAtThatTime = topicSubscribersTable.getInterestedClients(pub.getTopic());
                    if (!subscribersAtThatTime.isEmpty()) {
                        ackTracker.trackDelivery(pub, subscribersAtThatTime);
                    }
                    break;

                case ACK:
                    AckMessage ack = (AckMessage) msg;
                    ackTracker.handleAck(ack.getAcknowledgedMessageId(), ack.getSenderId());
                    break;

                default:
                    // This is for HEARTBEAT
                    break;
            }
        }

        System.out.println("State recovery complete.");
    }

    public void onMessageReceived(Message message) throws Exception {
        // todo probably move to the handle message methods
        if (message.getType() != MessageType.HEARTBEAT) {

            testCountForSnapshot.incrementAndGet();

            wal.append(message);

            System.out.println("Message received - " +
                    "message ID: " + message.getSequenceNumber() +
                    ", sender ID: " + message.getSenderId() +
                    ", message type: " + message.getType()
            );
        }

        // handle the message
        dispatchMessage(message);

        // if testCountForSnapshot >= ... -> take snapshot

        // todo not atomic, multiple thread could read 4 at almost the same time and take multiple snapshot (probably creating problems)
        if (testCountForSnapshot.get() >= 4) {
            testCountForSnapshot.set(0);
            takeSnapshot();
        }
    }

    private void dispatchMessage(Message message) throws Exception {
        switch (message.getType()) {
            case SUBSCRIBE:
                handleSubscribe((SubscribeMessage) message);
                break;
            case BROKER_SUBSCRIBE:
                handleBrokerSubscribe((BrokerSubscribeMessage) message);
                break;
            case UNSUBSCRIBE:
                handleUnsubscribe((UnsubscribeMessage) message);
                break;
            case BROKER_UNSUBSCRIBE:
                handleBrokerUnsubscribe((BrokerUnsubscribeMessage) message);
                break;
            case PUBLISH:
                handlePublish((PublishMessage) message);
                break;
            case BROKER_PUBLISH:
                handleBrokerPublish((BrokerPublishMessage) message);
                break;
            case ACK:
                handleAcknowledgement((AckMessage) message);
                break;
            case HEARTBEAT:
                updateHeartbeat(message.getSenderId());
                break;
            default:
                System.err.println("Unknown message type received.");
        }
    }

    private void handleSubscribe(SubscribeMessage msg) {
        String clientId = msg.getSenderId();
        updateHeartbeat(clientId);

        boolean newTopicOfInterest = topicSubscribersTable.addClientSubscription(msg.getTopic(), clientId);

        if (newTopicOfInterest) {
            forwardSubscribeMessage(msg.getTopic(), BROKER_ID, null);
        }

        // send an acknowledgment to the client
        sendAck(clientId, msg.getMessageId());
    }

    private void handleBrokerSubscribe(BrokerSubscribeMessage msg) {
        String senderId = msg.getSenderId();
        String topic = msg.getTopic();
        String subbedBrokerId = msg.getInterestedBrokerId(); // original broker which subscribed
        updateHeartbeat(senderId);

        // broker received its subscription message back
        if (msg.getInterestedBrokerId().equals(BROKER_ID)) {
            sendAck(senderId, msg.getMessageId());
            return;
        }

        boolean newBrokerSubscription = topicSubscribersTable.addBrokerSubscription(topic, subbedBrokerId, senderId);

        if (newBrokerSubscription)
            forwardSubscribeMessage(topic, subbedBrokerId, senderId);

        // send an acknowledgment to the broker
        sendAck(senderId, msg.getMessageId());
    }

    private void forwardSubscribeMessage(String topic, String subbedBrokerId, String previousHopId) {
        long brokerSeqNum = brokerCounter.incrementAndGet();
        long currentTime = System.currentTimeMillis();
        BrokerSubscribeMessage sm = new BrokerSubscribeMessage(BROKER_ID, brokerSeqNum, currentTime, subbedBrokerId, topic);

        for (BrokerHandler handler : connectionRegistry.getConnectedBrokerHandlers()) {
            // don't send back the message to the sender
            if (!handler.getNodeId().equals(previousHopId))
                handler.send(sm);
        }
    }

    private void handleUnsubscribe(UnsubscribeMessage msg) {
        String clientId = msg.getSenderId();
        String topic = msg.getTopic();
        updateHeartbeat(clientId);

        boolean removedTopicOfInterest = topicSubscribersTable.removeClientSubscription(topic, clientId);

        if (removedTopicOfInterest) {
            forwardUnsubscribeMessage(topic, BROKER_ID, null);
        }

        // send acknowledgement to the client
        sendAck(clientId, msg.getMessageId());
    }

    private void handleBrokerUnsubscribe(BrokerUnsubscribeMessage msg) {
        String senderId = msg.getSenderId();
        String topic = msg.getTopic();
        String unsubscribedBrokerId = msg.getInterestedBrokerId(); // original broker which unsubscribed
        updateHeartbeat(senderId);

        // broker received its unsubscription message back
        if (msg.getInterestedBrokerId().equals(BROKER_ID)) {
            sendAck(senderId, msg.getMessageId());
            return;
        }

        boolean removedBrokerSubscription = topicSubscribersTable.removeBrokerSubscription(topic, unsubscribedBrokerId);

        if (removedBrokerSubscription) {
            forwardUnsubscribeMessage(topic, unsubscribedBrokerId, senderId);
        }

        // send acknowledgement to the broker
        sendAck(senderId, msg.getMessageId());
    }

    private void forwardUnsubscribeMessage(String topic, String unsubscribedBrokerId, String previousHopId) {
        long brokerSeqNum = brokerCounter.incrementAndGet();
        long currentTime = System.currentTimeMillis();
        BrokerUnsubscribeMessage sm = new BrokerUnsubscribeMessage(BROKER_ID, brokerSeqNum, currentTime, unsubscribedBrokerId, topic);

        for (BrokerHandler handler : connectionRegistry.getConnectedBrokerHandlers()) {
            // don't send back the message to the sender
            if (!handler.getNodeId().equals(previousHopId))
                handler.send(sm);
        }
    }

    private void handlePublish(PublishMessage msg) {
        String publisherId = msg.getSenderId();
        updateHeartbeat(publisherId);

        String publishMessageId = msg.getMessageId();
        messageCache.asMap().put(publishMessageId, true);

        // forward the message to interested clients and brokers
        forwardPublishMessage(msg);

        // send acknowledgment to the publisher
        sendAck(publisherId, msg.getMessageId());
    }

    private void handleBrokerPublish(BrokerPublishMessage msg) {
        String brokerId = msg.getSenderId();
        updateHeartbeat(brokerId);

        String publishMessageId = msg.getOriginalMessageId();
        boolean newMessage = messageCache.asMap().putIfAbsent(publishMessageId, true) == null;

        if (newMessage) {
            forwardPublishMessage(msg.getOriginalMessage());
        }

        // send acknowledgment to the broker which forwarded the message
        sendAck(brokerId, msg.getMessageId());
    }

    private void forwardPublishMessage(PublishMessage msg) {
        String topic = msg.getTopic();
        Set<String> interestedClients = topicSubscribersTable.getInterestedClients(topic);
        Set<String> interestedBrokersNextHops = topicSubscribersTable.getNextHopOfInterestedBrokers(topic);

        if (!interestedBrokersNextHops.isEmpty()) {
            long brokerSeqNum = brokerCounter.incrementAndGet();
            long currentTime = System.currentTimeMillis();
            BrokerPublishMessage brokerMsg = new BrokerPublishMessage(BROKER_ID, brokerSeqNum, currentTime, msg);

            // todo create a proper ackTracker for the broker? track them as normal messages?
            ackTracker.trackDelivery(brokerMsg, interestedBrokersNextHops);

            for (String interestedBrokerId : interestedBrokersNextHops) {
                BrokerHandler handler = connectionRegistry.getBrokerHandler(interestedBrokerId);
                if (handler != null)
                    handler.send(brokerMsg);
            }
        }

        if (!interestedClients.isEmpty()) {
            ackTracker.trackDelivery(msg, interestedClients);

            for (String interestedClientId : interestedClients) {
                ClientHandler handler = connectionRegistry.getClientHandler(interestedClientId);
                if (handler != null)
                    handler.send(msg);
            }
        }
    }

    // TODO review if a proper broker ack tracker get created
    void handleAcknowledgement(AckMessage msg) {
        String senderId = msg.getSenderId();
        updateHeartbeat(senderId);

        ackTracker.handleAck(msg.getAcknowledgedMessageId(), senderId);
    }

    public void updateHeartbeat(String senderId) {
        ConnectionHandler handler = connectionRegistry.getConnectionHandler(senderId);

        if (handler != null)
            handler.updateHeartbeat();
    }

    // -----------------------------------------------------------------------------------------------------------------

    private void startHeartbeatChecker() {
        scheduler.scheduleAtFixedRate(this::checkHeartbeats, 0, HEARTBEAT_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void checkHeartbeats() {
        for (ConnectionHandler handler : connectionRegistry.getConnectedHandlers()) {
            if (handler.isTimedOut(HEARTBEAT_TIMEOUT_MS)) {
                System.out.println("Node " + handler.getNodeId() + " timed out, disconnecting.");
                handler.forceDisconnect();
            }
        }
    }

    private void startRetryScheduler() {
        scheduler.scheduleAtFixedRate(this::retryExpiredMessages, 0, RETRY_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void retryExpiredMessages() {
        List<PendingMessage> expiredMessages = ackTracker.getExpiredMessages(ACK_TIMEOUT_MS);

        // resend unacked messages to brokers and clients
        for (PendingMessage pending : expiredMessages) {

            pending.resetTimestamp(); // update message sent time

            for (String subscriberId : pending.getPendingSubscribers()) {
                ConnectionHandler handler = connectionRegistry.getConnectionHandler(subscriberId);
                if (handler != null) {
                    handler.send(pending.getMessage());
                }
            }
        }
    }

    void sendAck(String nodeId, String msgId) {
        ConnectionHandler handler = connectionRegistry.getConnectionHandler(nodeId);

        if (handler != null) {
            Message ack = generateAck(msgId);
            handler.send(ack);
        }
    }

    AckMessage generateAck(String msgId) {
        long brokerSeqNum = brokerCounter.incrementAndGet();
        long currentTime = System.currentTimeMillis();
        return new AckMessage(BROKER_ID, brokerSeqNum, currentTime, msgId);
    }

    public void addClientToRegistry(String clientId, ClientHandler handler) {
        connectionRegistry.registerClient(clientId, handler);
    }

    public void addPeerToRegistry(String brokerId, BrokerHandler handler) {
        connectionRegistry.registerBroker(brokerId, handler);
    }

    // TODO: When a client is definitively disconnected, his subscriptions remains in TopicSubscribersTable
    // TODO: Broker will track ACK from a client that never exists

    // TODO: hint from Boglio: add inside disconnectClient an UnsubscribeMessage
    public void onClientDisconnection(String clientId) {
        if (connectionRegistry.isClientConnected(clientId))
            connectionRegistry.disconnectClient(clientId);
    }

    /* TODO add updated topic subscriber table to snapshot and maybe
        On reconnection → immediately re-announce local subscriptions to the new neighbor
        Every 30s → re-announce local subscriptions to all neighbors
        On crash cleanup → removeBrokersByNextHop, let the refresh rebuild the paths naturally
     */
    public void onBrokerDisconnection(String brokerId) {
        // TODO how to proceed at broker disconnection?
        if (connectionRegistry.isBrokerConnected(brokerId))
            connectionRegistry.disconnectBroker(brokerId);
    }

    // todo add brokers info in snapshot
    /* todo takeSnapshot and onMessageReceived could run at the same time, so the snapshot could lose some messages
       we could lock the structure needed in snapshot and message received */
    private synchronized void takeSnapshot() throws Exception {
        List<Message> snapshotState = new java.util.ArrayList<>();
        long currentTime = System.currentTimeMillis();

        // Saving Subscribe Table

        for (String topic : topicSubscribersTable.getTopics()) {
            for (String clientId : topicSubscribersTable.getInterestedClients(topic)) {
                SubscribeMessage subMsg = new SubscribeMessage(
                        clientId,
                        0,
                        currentTime,
                        topic
                );
                snapshotState.add(subMsg);
            }
        }

        // Saving Pending Messages

        for (PendingMessage pending : ackTracker.getPendingMessages()) {
            snapshotState.add(pending.getMessage());
        }

        wal.snapshot(snapshotState);
    }

    private void shareTopicSubscribers(BrokerHandler handler) {
        topicSubscribersTable.getTopics().forEach(topic -> {
            // communicate all topics this broker is interested into
            if (!topicSubscribersTable.getInterestedClients(topic).isEmpty()) {
                long brokerSeqNum = brokerCounter.incrementAndGet();
                long currentTime = System.currentTimeMillis();
                BrokerSubscribeMessage sm = new BrokerSubscribeMessage(BROKER_ID, brokerSeqNum, currentTime, BROKER_ID, topic);

                ackTracker.trackDelivery(sm, Collections.singleton(handler.getNodeId()));
                handler.send(sm);
            }

            // communicate all topics other brokers are interested into
            topicSubscribersTable.getInterestedBrokers(topic).forEach(interestedBroker -> {
                long brokerSeqNum = brokerCounter.incrementAndGet();
                long currentTime = System.currentTimeMillis();
                BrokerSubscribeMessage sm = new BrokerSubscribeMessage(BROKER_ID, brokerSeqNum, currentTime, interestedBroker, topic);

                ackTracker.trackDelivery(sm, Collections.singleton(handler.getNodeId()));
                handler.send(sm);
            });
        });
    }
}
