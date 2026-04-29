package it.polimi.pubsub.broker;

import it.polimi.pubsub.broker.components.*;
import it.polimi.pubsub.broker.storage.WriteAheadLog;
import it.polimi.pubsub.messages.*;
import it.polimi.pubsub.network.TcpConnection;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Central broker with support for durable subscribers and client reconnection.
 *
 * Reconnect flow
 * 1. Client reconnects with the same clientId.
 * 2. addClientToRegistry() updates the handler in ClientRegistry.
 * 3. deliverOfflineMessages() drains the OfflineMessageStore for that clientId
 *    and sends every buffered PublishMessage down the new connection.
 * 4. The AckTracker still holds the pending ACKs, so the retry scheduler
 *    will also re-send any unACKed messages automatically.
 *
 * Disconnect flow
 * 1. ClientHandler detects the TCP drop and calls broker.onClientDisconnection().
 * 2. ClientRegistry.markOffline() removes the handler but keeps the record.
 * 3. Non-durable subscriptions are removed from TopicSubscribersTable.
 * 4. Durable subscriptions remain; future PublishMessages for offline durable
 *    subscribers are stored in OfflineMessageStore.
 */
public class Broker {
    private final TopicSubscribersTable     topicSubscribersTable;
    private final ClientRegistry            clientRegistry;
    private final AckTracker                ackTracker;
    private final WriteAheadLog             wal;
    private final OfflineMessageStore       offlineStore;
    private final BrokerRole                role;


    private final ExecutorService           executor;
    private final ScheduledExecutorService  scheduler;
    private AtomicLong                      brokerCounter = new AtomicLong(0);
    private final AtomicInteger             messagesAfterLastSnap = new AtomicInteger(0);
    private int                             port;

    private static final long SNAPSHOT_EVERY_N_MESSAGES     = 50;
    private static final long ACK_TIMEOUT_MS                = 5000;
    private static final long RETRY_INTERVAL_MS             = 2000;
    private static final long HEARTBEAT_TIMEOUT_MS          = 10000;
    private static final long HEARTBEAT_CHECK_INTERVAL_MS   = 3000;

    public Broker(String walFilePath, String offlineStorePath) throws Exception {
        this.topicSubscribersTable = new TopicSubscribersTable();
        this.clientRegistry        = new ClientRegistry();
        this.ackTracker            = new AckTracker();
        this.wal                   = new WriteAheadLog(walFilePath);
        this.offlineStore          = new OfflineMessageStore(offlineStorePath);
        this.executor              = Executors.newCachedThreadPool();
        this.scheduler             = Executors.newScheduledThreadPool(2);
        this.port                  = 8080;
        this.role                  = BrokerRole.FOLLOWER;

        offlineStore.recover();
        recoverState();

    }

    // ------------------------------------------------------------------
    // Startup
    // ------------------------------------------------------------------
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

    // ------------------------------------------------------------------
    // WAL recovery
    // ------------------------------------------------------------------
    private void recoverState() throws Exception {
        // todo check if it works
        for (Message msg : wal.recover()) {
            // Replay the historical messages to rebuild RAM state
            dispatchMessage(msg);
        }
    }

    public void onMessageReceived(Message message) throws Exception {
        // todo probably move to the handle message methods
        if (message.getType() != MessageType.HEARTBEAT) {
            wal.append(message);

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
}
