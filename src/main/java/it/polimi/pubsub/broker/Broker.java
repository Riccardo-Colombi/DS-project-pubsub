package it.polimi.pubsub.broker;

import it.polimi.pubsub.broker.components.*;
import it.polimi.pubsub.broker.storage.WriteAheadLog;
import it.polimi.pubsub.messages.*;
import it.polimi.pubsub.network.TcpConnection;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import java.util.List;

// TODO Update this to be coherent with Raft
// State machine for roles LEADER, FOLLOWER, CANDIDATE
// Add random timout for leader election

public class Broker {

    private final TopicSubscribersTable topicSubscribersTable;
    private final ClientRegistry clientRegistry;
    private final AckTracker ackTracker;
    private final WriteAheadLog wal;
    private final BrokerRole role;

    private final ExecutorService executor;
    private final ScheduledExecutorService scheduler;
    private AtomicLong brokerCounter;
    private int port;

    private static final long ACK_TIMEOUT_MS = 5000;
    private static final long RETRY_INTERVAL_MS = 2000;
    private static final long HEARTBEAT_TIMEOUT_MS = 10000;
    private static final long HEARTBEAT_CHECK_INTERVAL_MS = 3000;

    public Broker(String walFilePath) throws Exception {
        this.topicSubscribersTable = new TopicSubscribersTable();
        this.clientRegistry = new ClientRegistry();
        this.ackTracker = new AckTracker();
        this.wal = new WriteAheadLog(walFilePath);
        this.executor = Executors.newCachedThreadPool();
        this.scheduler = Executors.newScheduledThreadPool(2);
        this.brokerCounter = new AtomicLong(0);
        this.port = 8080;

        this.role = BrokerRole.FOLLOWER;

        recoverState();
    }

    public void run() {

        try (ServerSocket serverSocket = new ServerSocket(port)) {

            System.out.println("Broker listening on port " + port);

            startHeartbeatChecker();
            startRetryScheduler();

            while (true) {
                Socket socket = serverSocket.accept();
                System.out.println("New client connected: " + socket.getRemoteSocketAddress());
                TcpConnection connection = new TcpConnection(socket);

                ClientHandler handler = new ClientHandler(connection, this, executor);
                handler.start();
            }

        } catch (Exception e) {
            System.err.println("TcpServer error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void recoverState() throws Exception {
        // todo check if it works
        long tempValue;
        for (Message msg : wal.recover()) {
            tempValue = msg.getSequenceNumber();
            if (tempValue > this.brokerCounter.longValue()) {
                this.brokerCounter.set(tempValue);
            }
            // Replay the historical messages to rebuild RAM state
            dispatchMessage(msg);
        }

        // TODO: handle pending messages
        // Partial solution on Boglio's notes
    }

    public void onMessageReceived(Message message) throws Exception {
        // todo probably move to the handle message methods
        if (message.getType() != MessageType.HEARTBEAT) {
            wal.append(message);

            // If dim(wal) > 10kB -> take snapshot

            if (wal.getSize() > 10 * 1024) {
                System.out.println("Starting snapshot");
                takeSnapshot();
            }

            System.out.println("Message received - " +
                    "message ID: " + message.getSequenceNumber() +
                    ", sender ID: " + message.getSenderId() +
                    ", message type: " + message.getType()
            );
        }

        // handle the message
        dispatchMessage(message);
    }

    private void dispatchMessage(Message message) throws Exception {
        switch (message.getType()) {
            case SUBSCRIBE:
                handleSubscribe((SubscribeMessage) message);
                break;
            case UNSUBSCRIBE:
                handleUnsubscribe((UnsubscribeMessage) message);
                break;
            case PUBLISH:
                handlePublish((PublishMessage) message);
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

        topicSubscribersTable.addSubscription(msg.getTopic(), clientId);

        sendAck(clientId, msg.getMessageId());
    }

    private void handleUnsubscribe(UnsubscribeMessage msg) {
        String clientId = msg.getSenderId();
        updateHeartbeat(clientId);

        topicSubscribersTable.removeSubscription(msg.getTopic(), clientId);

        sendAck(clientId, msg.getMessageId());
    }

    private void handlePublish(PublishMessage msg) {
        String publisherId = msg.getSenderId();
        updateHeartbeat(publisherId);

        String topic = msg.getTopic();
        Set<String> interestedClients = topicSubscribersTable.getSubscribers(topic);

        if (!interestedClients.isEmpty()) {
            ackTracker.trackDelivery(msg, interestedClients);

            for (String interestedClientId : interestedClients) {
                ClientHandler handler = clientRegistry.getClientHandler(interestedClientId);
                if (handler != null)
                    handler.send(msg);
            }
        }

        sendAck(publisherId, msg.getMessageId());
    }

    public void handleAcknowledgement(AckMessage msg) {
        String clientId = msg.getSenderId();
        updateHeartbeat(clientId);

        ackTracker.handleAck(msg.getAcknowledgedMessageId(), clientId);
    }

    public void updateHeartbeat(String senderId) {
        ClientHandler handler = clientRegistry.getClientHandler(senderId);

        if (handler != null)
            handler.updateHeartbeat();
    }

    private void startHeartbeatChecker() {
        scheduler.scheduleAtFixedRate(this::checkHeartbeats, 0, HEARTBEAT_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void checkHeartbeats() {
        for (ClientHandler handler : clientRegistry.getConnectedClientHandlers()) {
            if (handler.isTimedOut(HEARTBEAT_TIMEOUT_MS)) {
                System.out.println("Client " + handler.getClientId() + " timed out, disconnecting.");
                handler.forceDisconnect();
            }
        }
    }

    private void startRetryScheduler() {
        scheduler.scheduleAtFixedRate(this::retryExpiredMessages, 0, RETRY_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void retryExpiredMessages() {
        List<PendingMessage> expired = ackTracker.getExpiredMessages(ACK_TIMEOUT_MS);
        for (PendingMessage pending : expired) {

            pending.resetTimestamp(); // update message sent time

            for (String clientId : pending.getPendingSubscribers()) {
                ClientHandler handler = clientRegistry.getClientHandler(clientId);
                if (handler != null) {
                    handler.send(pending.getMessage());
                }
            }
        }
    }

    /**
     * Send an acknowledgement message for message with the specified msgId, to the client with the specified clientId
     * @param clientId
     * @param msgId
     */
    public void sendAck(String clientId, String msgId) {
        ClientHandler handler = clientRegistry.getClientHandler(clientId);

        if (handler != null) {
            Message ack = generateAck(msgId);
            handler.send(ack);
        }
    }

    /**
     * generate an acknowledgement message for message with the specified msgId
     * @param msgId
     * @return
     */
    public AckMessage generateAck(String msgId) {
        long brokerSeqNum = brokerCounter.incrementAndGet();
        long currentTime = System.currentTimeMillis();
        return new AckMessage("Broker", brokerSeqNum, currentTime, msgId);
    }

    public void addClientToRegistry(String clientId, ClientHandler handler) {
        if (!clientRegistry.isClientConnected(clientId))
            clientRegistry.registerClient(clientId, handler);
    }

    public void onClientDisconnection(String clientId) {
        if (clientRegistry.isClientConnected(clientId))
            clientRegistry.disconnectClient(clientId);
    }

    private synchronized void takeSnapshot() throws Exception {
        List<Message> snapshotState = new java.util.ArrayList<>();
        long currentTime = System.currentTimeMillis();

        // Saving Subscribe Table

        for (String topic : topicSubscribersTable.getTopics()) {
            for (String clientId : topicSubscribersTable.getSubscribers(topic)) {
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
}
