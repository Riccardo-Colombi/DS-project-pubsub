package it.polimi.pubsub.broker;

import it.polimi.pubsub.broker.components.BrokerHandler;
import it.polimi.pubsub.broker.components.*;
import it.polimi.pubsub.broker.storage.WriteAheadLog;
import it.polimi.pubsub.messages.*;
import it.polimi.pubsub.messages.brokerMessages.*;
import it.polimi.pubsub.network.TcpConnection;
import it.polimi.pubsub.broker.states.*;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

// TODO Update this to be coherent with Raft
// State machine for roles LEADER, FOLLOWER, CANDIDATE
// Add random timout for leader election

// TODO RAFT
//  state machine for different roles with different management of messages by each role
//      a follower must redirect clients to the leader
//      non blocking waits for election and ack wait
//  once we create a broker he starts as a Leader so he can process messages even by himself
//  methods for joining a cluster
//      if cluster composed of 2 brokers instant election of one of them
//      if composed by more then one check if request was send to a leader, if not redirect
//  when a broker joins the cluster his information must be send to others broker of the cluster for election purposes
//  timout for the leader
//

public class Broker {

    private final String BROKER_ID;

    private final TopicSubscribersTable topicSubscribersTable;
    private final ConnectionRegistry connectionRegistry;
    private final AckTracker ackTracker;
    private final WriteAheadLog wal;
    private final OfflineMessageStore offlineStore;

    // Cluster state
    private volatile BrokerState state;
    private String clusterId;
    private volatile int currentTerm;
    private String currentLeaderId;
    private String leaderHost;       // host del leader, usato per redirect
    private int leaderPort;          // broker port del leader, usato per redirect broker→broker
    private int leaderClientPort;    // client port del leader, usato per redirect client→leader

    private final ConcurrentHashMap<String, PendingMessage> pendingEntries = new ConcurrentHashMap<>();
    record ProxyRecord(String clientId, String originMessageId, String proxyMessageId) {}

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

    private static final long ACK_TIMEOUT_MS              = 5000;
    private static final long RETRY_INTERVAL_MS           = 2000;
    private static final long HEARTBEAT_TIMEOUT_MS        = 10000;
    private static final long HEARTBEAT_CHECK_INTERVAL_MS = 3000;
    private static final long HEARTBEAT_INTERVAL_MS       = 3000;
    private static final long CACHE_LIFETIME              = 2;


    public Broker(String brokerId, int brokerPort, int clientPort,
                  String walFilePath, String offlineStorePath) throws Exception {
        BROKER_ID        = brokerId;
        this.CLIENT_PORT = clientPort;
        this.BROKER_PORT = brokerPort;

        this.topicSubscribersTable = new TopicSubscribersTable();
        this.connectionRegistry    = new ConnectionRegistry();
        this.ackTracker            = new AckTracker();
        this.wal                   = new WriteAheadLog(walFilePath);
        this.offlineStore          = new OfflineMessageStore(offlineStorePath);
        this.executor              = Executors.newCachedThreadPool();
        this.scheduler             = Executors.newScheduledThreadPool(2);
        this.brokerCounter         = new AtomicLong(0);
        this.state                 = new SoloState(this);

        offlineStore.recover();   // load buffered messages from disk before accepting connections
        recoverState();
    }

    public void run() {
        try {
            startHeartbeat();
            startHeartbeatChecker();
            startRetryScheduler();
            executor.submit(this::runClientListener);
            executor.submit(this::runBrokerListener);
        } catch (Exception e) {
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
                    handler.performHandshake(BROKER_ID, BROKER_PORT); // blocks until nodeId is known
                    socket.setSoTimeout(0);              // remove timeout for normal ops
                    addPeerToRegistry(handler.getNodeId(), handler);
                    handler.start();                     // readLoop/writeLoop now safe
                    shareTopicSubscribers(handler);      // nodeId is guaranteed non-null

                    System.out.println(BROKER_ID + " connected to " + handler.getNodeId());
                } catch (Exception e) {
                    System.err.println("Handshake failed, rejecting connection: " + e.getMessage());
                    conn.close(); // don't let a bad peer into the system
                }
            }
        } catch (Exception e) {
            System.err.println("TcpServer error: " + e.getMessage());
        }
    }

    public BrokerHandler connectToPeer(String brokerHost, int brokerPort) throws Exception{
        Socket socket = new Socket(brokerHost, brokerPort);
        socket.setSoTimeout(5000);

        TcpConnection conn = new TcpConnection(socket);
        BrokerHandler handler = new BrokerHandler(conn, this, executor);

        handler.performHandshake(BROKER_ID, BROKER_PORT);
        socket.setSoTimeout(0);
        addPeerToRegistry(handler.getNodeId(), handler);
        handler.start();
        shareTopicSubscribers(handler);

        System.out.println(BROKER_ID + " connected to " + handler.getNodeId());
        return handler;
    }

    private void recoverState() throws Exception {
        List<Message> messages = wal.recover();
        System.out.println("Recovering state from WAL: " + messages.size() + " entries.");

        for (Message msg : messages) {
            switch (msg.getType()) {
                case SUBSCRIBE:
                    SubscribeMessage sub = (SubscribeMessage) msg;
                    // Preserve the durable flag stored in the snapshot
                    topicSubscribersTable.addClientSubscription(sub.getTopic(), sub.getSenderId(), sub.isDurable());
                    break;

                case UNSUBSCRIBE:
                    topicSubscribersTable.removeClientSubscription(((UnsubscribeMessage) msg).getTopic(), msg.getSenderId());
                    break;

                case PUBLISH:
                    PublishMessage pub = (PublishMessage) msg;
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
        if (message.getType() != MessageType.HEARTBEAT) {
            testCountForSnapshot.incrementAndGet();
            wal.append(message);

            System.out.println("Message received - " + "Broker: " + BROKER_ID +
                    ", message ID: " + message.getSequenceNumber() +
                    ", sender ID: " + message.getSenderId() +
                    ", message type: " + message.getType());
        }

        state.onMessageReceived(message);

        // todo not atomic, multiple thread could read 4 at almost the same time and take multiple snapshot (probably creating problems)
        if (testCountForSnapshot.get() >= 4) {
            testCountForSnapshot.set(0);
            takeSnapshot();
        }
    }

    public void dispatchMessage(Message message) throws Exception {
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
            case JOIN_CLUSTER:
                state.onMessageReceived(message);   // delega allo stato corrente
                break;
            case JOIN_CLUSTER_ACK:
                handleJoinClusterAck((JoinClusterAckMessage) message);
                break;
            case REDIRECT_TO_LEADER:
                handleRedirectToLeader((RedirectToLeaderMessage) message);
                break;
            default:
                System.err.println("Unknown message type received.");
        }
    }

    private void handleSubscribe(SubscribeMessage msg) {
        String clientId = msg.getSenderId();
        updateHeartbeat(clientId);

        // Pass durable flag through to the table
        boolean newTopicOfInterest = topicSubscribersTable.addClientSubscription(
                msg.getTopic(), clientId, msg.isDurable());

        if (newTopicOfInterest) {
            forwardSubscribeMessage(msg.getTopic(), BROKER_ID, null);
        }

        // send an acknowledgment to the client
        sendAck(clientId, msg.getMessageId());
    }

    private void handleBrokerSubscribe(BrokerSubscribeMessage msg) {
        String senderId      = msg.getSenderId();
        String topic         = msg.getTopic();
        String subbedBroker  = msg.getInterestedBrokerId();
        updateHeartbeat(senderId);

        if (subbedBroker.equals(BROKER_ID)) {
            sendAck(senderId, msg.getMessageId());
            return;
        }

        boolean newBrokerSubscription = topicSubscribersTable.addBrokerSubscription(topic, subbedBroker, senderId);

        if (newBrokerSubscription)
            forwardSubscribeMessage(topic, subbedBroker, senderId);

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

    /**
     * Deliver a publish message to all interested clients and brokers.
     *
     * For clients:
     *   - ONLINE           -> send directly via their handler
     *   - OFFLINE + durable -> buffer in OfflineMessageStore for delivery on reconnect
     *   - OFFLINE + non-durable -> drop (original behaviour)
     */
    private void forwardPublishMessage(PublishMessage msg) {
        String topic = msg.getTopic();
        Set<String> interestedClients         = topicSubscribersTable.getInterestedClients(topic);
        Set<String> interestedBrokersNextHops = topicSubscribersTable.getNextHopOfInterestedBrokers(topic);

        // Forward to peer brokers (unchanged)
        if (!interestedBrokersNextHops.isEmpty()) {
            long brokerSeqNum = brokerCounter.incrementAndGet();
            long currentTime  = System.currentTimeMillis();
            BrokerPublishMessage brokerMsg = new BrokerPublishMessage(BROKER_ID, brokerSeqNum, currentTime, msg);

            ackTracker.trackDelivery(brokerMsg, interestedBrokersNextHops);

            for (String nextHop : interestedBrokersNextHops) {
                BrokerHandler handler = connectionRegistry.getBrokerHandler(nextHop);
                if (handler != null)
                    handler.send(brokerMsg);
            }
        }

        // Forward to local clients
        if (!interestedClients.isEmpty()) {
            ackTracker.trackDelivery(msg, interestedClients);

            for (String clientId : interestedClients) {
                ClientHandler handler = connectionRegistry.getClientHandler(clientId);

                if (handler != null) {
                    handler.send(msg);
                } else {
                    // Cerca se il client è su un broker remoto
                    BrokerHandler brokerHandler = connectionRegistry.findBrokerForClient(clientId);
                    if (brokerHandler != null) {
                        long brokerSeqNum = brokerCounter.incrementAndGet();
                        BrokerPublishMessage brokerMsg = new BrokerPublishMessage(
                                BROKER_ID, brokerSeqNum, System.currentTimeMillis(), msg);
                        ackTracker.trackDelivery(brokerMsg,
                                Collections.singleton(brokerHandler.getNodeId()));
                        brokerHandler.send(brokerMsg);
                    } else if (topicSubscribersTable.isDurableSubscriber(clientId, topic)) {
                        try {
                            offlineStore.enqueue(clientId, msg);
                        } catch (Exception e) {
                            System.err.println("[OfflineStore] Failed to buffer: " + e.getMessage());
                        }
                    }
                }
            }
        }
    }

    void handleAcknowledgement(AckMessage msg) {
        String senderId = msg.getSenderId();
        updateHeartbeat(senderId);

        ackTracker.handleAck(msg.getAcknowledgedMessageId(), senderId);
    }

    public void updateHeartbeat(String senderId) {
        ConnectionHandler handler = connectionRegistry.getConnectionHandler(senderId);

        if (handler != null) {
            handler.updateHeartbeat();
        }
    }

    // ------------------------------------------------------------------
    // Heartbeat
    // ------------------------------------------------------------------

    private void startHeartbeat() {
        scheduler.scheduleAtFixedRate(this::sendHeartbeat, HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    public void sendHeartbeat() {
        HeartbeatMessage hm = new HeartbeatMessage(BROKER_ID, 0, System.currentTimeMillis());

        for (BrokerHandler handler : connectionRegistry.getConnectedBrokerHandlers()) {
            handler.send(hm);
        }
    }

    // ------------------------------------------------------------------
    // Heartbeat checker
    // ------------------------------------------------------------------

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

    // ------------------------------------------------------------------
    // Retry scheduler
    // ------------------------------------------------------------------

    private void startRetryScheduler() {
        scheduler.scheduleAtFixedRate(this::retryExpiredMessages, 0, RETRY_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void retryExpiredMessages() {
        List<PendingMessage> expiredMessages = ackTracker.getExpiredMessages(ACK_TIMEOUT_MS);

        // resend unacked messages to brokers and clients
        for (PendingMessage pending : expiredMessages) {
            // Skip messages already fully acknowledged
            if (pending.isFullyAcknowledged()) continue;

            pending.resetTimestamp(); // update message sent time

            for (String subscriberId : pending.getPendingSubscribers()) {
                ConnectionHandler handler = connectionRegistry.getConnectionHandler(subscriberId);
                if (handler != null) {
                    handler.send(pending.getMessage());
                    // If offline, the OfflineMessageStore already holds the message;
                    // deliverOfflineMessages() will push it on reconnect.
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // ACK helpers
    // ------------------------------------------------------------------

    public void sendAck(String nodeId, String msgId) {
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

    // ------------------------------------------------------------------
    // Client lifecycle
    // ------------------------------------------------------------------

    public void redirectClientToLeader(String clientId) {
        if (leaderHost == null || leaderClientPort <= 0) {
            System.err.println(BROKER_ID + ": cannot redirect client " + clientId + " — leader address unknown");
            return;
        }
        long seq = brokerCounter.incrementAndGet();
        RedirectToLeaderMessage redirect = new RedirectToLeaderMessage(
                BROKER_ID, seq, System.currentTimeMillis(),
                leaderHost, leaderPort, leaderClientPort);
        ConnectionHandler handler = connectionRegistry.getConnectionHandler(clientId);
        if (handler != null) {
            handler.send(redirect);
            System.out.println(BROKER_ID + " redirecting client " + clientId
                    + " to leader at " + leaderHost + ":" + leaderClientPort);
        }
    }

    /**
     * Called by ClientHandler when a client connects (first time or reconnect).
     * On reconnect: updates the handler and delivers any buffered offline messages.
     */
    public void addClientToRegistry(String clientId, ClientHandler handler) {
        boolean isReconnect = connectionRegistry.isKnownClient(clientId);
        connectionRegistry.registerClient(clientId, handler); // always replaces stale handler

        if (isReconnect) {
            System.out.println("Client " + clientId + " reconnected. Delivering offline messages…");
            deliverOfflineMessages(clientId);
        } else {
            System.out.println("Client " + clientId + " connected for the first time.");
        }

        // Redirect if in FOLLOWER state
        if (state.getRole() == BrokerRole.FOLLOWER) {
            redirectClientToLeader(clientId);
        }
    }

    /**
     * Drain the OfflineMessageStore for this client and push all buffered
     * messages down the freshly re-established connection.
     * Also resets AckTracker timestamps to prevent immediate double-delivery
     * from the retry scheduler.
     */
    private void deliverOfflineMessages(String clientId) {
        try {
            List<PublishMessage> backlog = offlineStore.drain(clientId);
            if (backlog.isEmpty()) return;

            System.out.println("[OfflineStore] Delivering " + backlog.size()
                    + " buffered messages to " + clientId);

            ClientHandler handler = connectionRegistry.getClientHandler(clientId);
            if (handler == null) return; // client disconnected again immediately

            for (PublishMessage msg : backlog)
                handler.send(msg);

            // Reset retry timers so the scheduler doesn't re-send before the client ACKs
            ackTracker.resetTimestampsForSubscriber(clientId);

        } catch (Exception e) {
            System.err.println("Error delivering offline messages to " + clientId + ": " + e.getMessage());
        }
    }

    public void addPeerToRegistry(String brokerId, BrokerHandler handler) {
        connectionRegistry.registerBroker(brokerId, handler);
    }

    // TODO: When a client is definitively disconnected, his subscriptions remains in TopicSubscribersTable
    // TODO: Broker will track ACK from a client that never exists

    // TODO: hint from Boglio: add inside disconnectClient an UnsubscribeMessage
    /**
     * Called by ClientHandler on TCP disconnection.
     * Marks the client offline (keeps the record for durable subscriptions)
     * and removes only non-durable subscriptions.
     */
    public void onClientDisconnection(String clientId) {
        // keep record, drop handler
        connectionRegistry.markClientOffline(clientId);

        // clean non-durable subs
        for (String topic : topicSubscribersTable.getNonDurableSubscriptions(clientId)) {
            boolean removedTopicOfInterest = topicSubscribersTable.removeClientSubscription(topic, clientId);

            if (removedTopicOfInterest) {
                forwardUnsubscribeMessage(topic, BROKER_ID, null);
            }
        }

        // topicSubscribersTable.removeNonDurableSubscriptions(clientId); // clean non-durable subs
    }

    /* TODO add updated topic subscriber table to snapshot and maybe
        On reconnection → immediately re-announce local subscriptions to the new neighbor
        Every 30s → re-announce local subscriptions to all neighbors
        On crash cleanup → removeBrokersByNextHop, let the refresh rebuild the paths naturally
     */
    public void onBrokerDisconnection(String brokerId) {
        if (connectionRegistry.isBrokerConnected(brokerId))
            connectionRegistry.disconnectBroker(brokerId);
    }

    // ------------------------------------------------------------------
    // Snapshot
    // ------------------------------------------------------------------

    // todo add brokers info in snapshot
    // todo takeSnapshot and onMessageReceived could run at the same time, so the snapshot could lose some message
    //      we could lock the structure needed in snapshot and message received
    private synchronized void takeSnapshot() throws Exception {
        List<Message> snapshotState = new java.util.ArrayList<>();
        long currentTime = System.currentTimeMillis();

        // Saving Subscribe Table
        for (String topic : topicSubscribersTable.getTopics()) {
            for (String clientId : topicSubscribersTable.getInterestedClients(topic)) {
                // Persist the durable flag so recovery restores it correctly
                boolean durable = topicSubscribersTable.isDurableSubscriber(clientId, topic);
                snapshotState.add(new SubscribeMessage(clientId, 0, currentTime, topic, durable));
            }
        }

        // Saving Pending Messages
        for (PendingMessage pending : ackTracker.getPendingMessages()) {
            snapshotState.add(pending.getMessage());
        }

        wal.snapshot(snapshotState);
    }

    // ------------------------------------------------------------------
    // Broker-to-broker topic sharing
    // ------------------------------------------------------------------

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

    // ------------------------------------------------------------------
    // Cluster lifecycle
    // ------------------------------------------------------------------

    /**
     * Il broker SOLO crea un nuovo cluster e diventa LEADER.
     */

    public void createCluster() {

        // Controllo se sono nello state SOLO

        if (state.getRole() != BrokerRole.SOLO) {
            System.err.println("createCluster() chiamato su un broker che non è SOLO (ruolo attuale: " + state.getRole() + ")");
            return;
        }

        // Creazione cluster

        this.clusterId = java.util.UUID.randomUUID().toString();
        this.currentTerm = 1;
        this.currentLeaderId = BROKER_ID;
        this.leaderHost = null;         // sono io il LEADER
        this.leaderPort = -1;           // sono io il LEADER
        this.leaderClientPort = -1;     // sono io il LEADER

        // Cambio state in LEADER

        this.state = new LeaderState(this);
        System.out.println(BROKER_ID + " ha creato il cluster " + clusterId + " — term=" + currentTerm + ", ruolo=LEADER");
    }

    /**
     * Il broker SOLO si connette al leader e diventa FOLLOWER.
     * Se il nodo contattato non è il leader, segue il redirect automaticamente.
     */

    public void joinCluster(String host, int port) throws Exception {

        // Controllo se sono nello state SOLO

        if (state.getRole() != BrokerRole.SOLO) {
            System.err.println("joinCluster() chiamato su un broker che non è SOLO (ruolo attuale: " + state.getRole() + ")");
            return;
        }

        // Apro connessione con il Broker LEADER

        BrokerHandler handler = connectToPeer(host, port);

        // Invia la richiesta di join al nodo appena connesso

        long seq = brokerCounter.incrementAndGet();
        JoinClusterMessage joinMsg = new JoinClusterMessage(BROKER_ID, seq, System.currentTimeMillis());
        handler.send(joinMsg);

        System.out.println(BROKER_ID + " ha inviato JoinClusterMessage a " + host + ":" + port);
    }

    /**
     * Chiamato da LeaderState quando arriva un JoinClusterMessage.
     */

    public void handleJoinClusterRequest(JoinClusterMessage msg) {

        System.out.println( "Handling Join request from:" + msg.getSenderId() + "recived by" + this.BROKER_ID);

        String requesterId = msg.getSenderId();

        // Se non sono il leader, mando un redirect (non dovrebbe succedere se joinCluster() è usato correttamente)

        if (!BROKER_ID.equals(currentLeaderId)) {
            long seq = brokerCounter.incrementAndGet();
            RedirectToLeaderMessage redirect = new RedirectToLeaderMessage(BROKER_ID, seq, System.currentTimeMillis(), leaderHost, leaderPort, leaderClientPort);
            sendAck(requesterId, msg.getMessageId());
            connectionRegistry.getConnectionHandler(requesterId).send(redirect);
            return;

            // TODO Manage redirect message on the sender broker

        }

        BrokerHandler requesterHandler = connectionRegistry.getBrokerHandler(requesterId);
        if (requesterHandler != null) requesterHandler.markAsClusterMember();

        // Se sono il LEADER rispondo con i dati del cluster

        // TODO doing this I will also send the brokers that are not in the cluster,
        //  is this right?

        List<BrokerAddress> peers = connectionRegistry.getConnectedBrokerHandlers()
                .stream()
                .filter(h -> !h.getNodeId().equals(requesterId))
                .map(h -> new BrokerAddress(h.getNodeId(), h.getRemoteHost(), h.getRemoteBrokerPort()))
                .collect(java.util.stream.Collectors.toList());

        peers.forEach(p -> System.out.println("Peer in ack: " + p.getBrokerId()
                + " -> " + p.getHost() + ":" + p.getBrokerPort()));

        long seq = brokerCounter.incrementAndGet();
        JoinClusterAckMessage joinAck = new JoinClusterAckMessage(
                BROKER_ID, seq, System.currentTimeMillis(),
                clusterId, currentTerm, peers,
                "localhost",  // o this.brokerHost se lo aggiungi come campo
                BROKER_PORT,
                CLIENT_PORT
        );
        connectionRegistry.getConnectionHandler(requesterId).send(joinAck);

        System.out.println(BROKER_ID + " ha accettato " + requesterId + " nel cluster " + clusterId);
    }

    /**
     * Chiamato da dispatchMessage() quando arriva un JoinClusterAckMessage.
     * Completa la transizione SOLO → FOLLOWER.
     */

    private void handleJoinClusterAck(JoinClusterAckMessage msg) {
        this.clusterId       = msg.getClusterId();
        this.currentTerm     = msg.getCurrentTerm();
        this.currentLeaderId = msg.getSenderId();
        this.state           = new FollowerState(this);

        this.leaderHost       = msg.getLeaderHost();
        this.leaderPort       = msg.getLeaderPort();
        this.leaderClientPort = msg.getLeaderClientPort();

        BrokerHandler leaderHandler = connectionRegistry.getBrokerHandler(msg.getSenderId());
        if (leaderHandler != null) leaderHandler.markAsClusterMember();

        executor.submit( () -> {
            for (BrokerAddress peer : msg.getPeerAddresses()) {
                if (!connectionRegistry.isBrokerConnected(peer.getBrokerId())) {
                    try {
                        BrokerHandler h = connectToPeer(peer.getHost(), peer.getBrokerPort());
                        h.markAsClusterMember();
                    } catch (Exception e) {
                        System.err.println("Impossibile connettersi al peer "
                                + peer.getBrokerId() + ": " + e.getMessage());
                    }
                }
            }
        });

        System.out.println(BROKER_ID + " joined cluster " + clusterId
                + " — term=" + currentTerm + ", leader=" + currentLeaderId + ", role=FOLLOWER");
    }

    /**
     * Chiamato da dispatchMessage() quando il nodo contattato non era il leader.
     * Riprova automaticamente sul leader reale.
     */

    private void handleRedirectToLeader(RedirectToLeaderMessage msg) throws Exception {
        System.out.println(BROKER_ID + " rediretto al leader " + msg.getLeaderHost() + ":" + msg.getLeaderPort());
        joinCluster(msg.getLeaderHost(), msg.getLeaderPort());
    }

}