package it.polimi.pubsub.broker;

import it.polimi.pubsub.broker.components.BrokerHandler;
import it.polimi.pubsub.broker.components.*;
import it.polimi.pubsub.broker.raft.Raft;
import it.polimi.pubsub.broker.storage.RaftMetadata;
import it.polimi.pubsub.broker.storage.WriteAheadLog;
import it.polimi.pubsub.messages.*;
import it.polimi.pubsub.messages.brokerMessages.*;
import it.polimi.pubsub.network.TcpConnection;
import it.polimi.pubsub.broker.states.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

    Cache<String, Boolean> messageCache = Caffeine.newBuilder()
            .maximumSize(10000)
            .expireAfterWrite(CACHE_LIFETIME, TimeUnit.MINUTES)
            .build();

    // Crash simulation
    private volatile boolean crashed = false;
    private volatile ServerSocket clientServerSocket;
    private volatile ServerSocket brokerServerSocket;

    private final ExecutorService executor;
    private final ScheduledExecutorService scheduler;
    private AtomicLong brokerCounter;

    private final int CLIENT_PORT;
    private final int BROKER_PORT;

    private static final long ACK_TIMEOUT_MS                = 5000;
    private static final long RETRY_INTERVAL_MS             = 2000;
    private static final long HEARTBEAT_TIMEOUT_MS          = 25000;
    private static final long HEARTBEAT_CHECK_INTERVAL_MS   = 3000;
    private static final long HEARTBEAT_INTERVAL_MS         = 3000;
    private static final long CACHE_LIFETIME                = 2;

    // Added in case of high traffic scenario (in this project is not needed, but hypothetically is good)
    private final AtomicInteger messagesSinceLastCheck = new AtomicInteger(0);
    private static final int SIZE_CHECK_INTERVAL            = 50;

    private static final long SNAPSHOT_SIZE_THRESHOLD_BYTES = 5 * 1024 * 1024; // 5MB

    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();

    private final Raft raftEngine;
    private final PubSubRouter pubSubRouter;
    private final List<BrokerAddress> restoredPeers;

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

        this.raftEngine   = new Raft(BROKER_ID, BROKER_PORT, CLIENT_PORT, this, connectionRegistry, brokerCounter);
        this.pubSubRouter = new PubSubRouter(BROKER_ID, this, topicSubscribersTable, offlineStore, connectionRegistry, ackTracker, brokerCounter);

        offlineStore.recover();

        RaftMetadata savedRaftState = wal.readRaftMetadata();
        if (savedRaftState != null) {
            raftEngine.restoreState(savedRaftState);
            this.restoredPeers = savedRaftState.getKnownPeers();
        } else {
            this.restoredPeers = Collections.emptyList();
        }

        recoverState();
    }

    public void run() {
        try {
            startHeartbeat();
            startHeartbeatChecker();
            startRetryScheduler();
            startElectionTimeoutChecker(); //todo check if works
            executor.submit(this::runClientListener);
            executor.submit(this::runBrokerListener);

            attemptClusterRejoin();
        } catch (Exception e) {
            System.err.println("Fatal error: " + e.getMessage());
        }
    }

    private void runClientListener() {
        try (ServerSocket ss = new ServerSocket(CLIENT_PORT)) {
            this.clientServerSocket = ss;
            System.out.println("Listening for clients on " + CLIENT_PORT);

            while (!crashed) {
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
            this.brokerServerSocket = ss;
            System.out.println("Listening for brokers on " + BROKER_PORT);

            while (!crashed) {
                Socket socket = ss.accept();
                socket.setSoTimeout(5000); // handshake must complete within 5s

                TcpConnection conn = new TcpConnection(socket);
                BrokerHandler handler = new BrokerHandler(conn, this, executor);

                try {
                    handler.performHandshake(BROKER_ID, BROKER_PORT);
                    socket.setSoTimeout(0);
                    addPeerToRegistry(handler.getNodeId(), handler);
                    handler.start();
                    pubSubRouter.shareTopicSubscribers(handler);

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

    private void attemptClusterRejoin() {
        if (restoredPeers == null || restoredPeers.isEmpty()) return;

        executor.submit(() -> {
            for (BrokerAddress peer : restoredPeers) {
                try {
                    System.out.println(BROKER_ID + ": trying to re-enter in the cluster through the help of: " + peer.getBrokerId());
                    joinCluster(peer.getHost(), peer.getBrokerPort());
                    return; // Sent request without exception of connection. Not "I'm in the cluster."
                } catch (Exception e) {
                    System.err.println(BROKER_ID + ": peer " + peer.getBrokerId()
                            + " not reachable (" + e.getMessage() + "), trying the next one.");
                }
            }
            System.err.println(BROKER_ID + ": no known peer reachable. "
                    + "Broker alone till external join.");
        });
    }

    public BrokerHandler connectToPeer(String brokerHost, int brokerPort) throws Exception {
        Socket socket = new Socket(brokerHost, brokerPort);
        socket.setSoTimeout(5000);

        TcpConnection conn = new TcpConnection(socket);
        BrokerHandler handler = new BrokerHandler(conn, this, executor);

        handler.performHandshake(BROKER_ID, BROKER_PORT);
        socket.setSoTimeout(0);
        addPeerToRegistry(handler.getNodeId(), handler);
        handler.start();
        pubSubRouter.shareTopicSubscribers(handler);

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

                    // Populate the connection registry with recovered durable clients.
                    if (sub.isDurable()) {
                        connectionRegistry.markClientOffline(sub.getSenderId());
                    }
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

                case SNAPSHOT_PENDING_DELIVERY:
                    PendingDeliveryMessage pdm = (PendingDeliveryMessage) msg;
                    if (!pdm.getPendingSubscribers().isEmpty()) {
                        ackTracker.trackDelivery(pdm.getOriginalMessage(), pdm.getPendingSubscribers());
                    }
                    break;

                case ACK:
                    AckMessage ack = (AckMessage) msg;
                    ackTracker.handleAck(ack.getAcknowledgedMessageId(), ack.getSenderId());
                    break;

                // TODO: BrokerPublishMessage and BrokerSubscribeMessage is not in here! Fix it.

                default:
                    // This is for HEARTBEAT
                    break;
            }
        }

        System.out.println("State recovery complete.");
    }

    public void onMessageReceived(Message message) throws Exception {
        if (crashed) return;

        stateLock.writeLock().lock();
        try {
            if (message.getType() != MessageType.HEARTBEAT
                    && message.getType() != MessageType.APPEND_ENTRY
                    && message.getType() != MessageType.APPEND_ENTRY_ACK
                    && message.getType() != MessageType.COMMIT_ENTRY
                    && message.getType() != MessageType.REQUEST_VOTE
                    && message.getType() != MessageType.REQUEST_VOTE_ACK) {

                wal.append(message);

                // TODO: restore after test
                /* System.out.println("Message received - " + "Broker: " + BROKER_ID +
                        ", message ID: " + message.getSequenceNumber() +
                        ", sender ID: " + message.getSenderId() +
                        ", message type: " + message.getType()); */
            }

            raftEngine.getState().onMessageReceived(message);
        } finally {
            stateLock.writeLock().unlock();
        }

        // Counter for extra "optimization"
        if (messagesSinceLastCheck.incrementAndGet() >= SIZE_CHECK_INTERVAL) {
            messagesSinceLastCheck.set(0);

            // Trigger size-based: check the dimension of the file on disk
            if (wal.getSize() >= SNAPSHOT_SIZE_THRESHOLD_BYTES) {
                takeSnapshot();
            }
        }
    }

    public void dispatchMessage(Message message) throws Exception {
        switch (message.getType()) {
            case SUBSCRIBE:
                pubSubRouter.handleSubscribe((SubscribeMessage) message);
                break;
            case BROKER_SUBSCRIBE:
                pubSubRouter.handleBrokerSubscribe((BrokerSubscribeMessage) message);
                break;
            case UNSUBSCRIBE:
                pubSubRouter.handleUnsubscribe((UnsubscribeMessage) message);
                break;
            case BROKER_UNSUBSCRIBE:
                pubSubRouter.handleBrokerUnsubscribe((BrokerUnsubscribeMessage) message);
                break;
            case PUBLISH:
                pubSubRouter.handlePublish((PublishMessage) message);
                break;
            case BROKER_PUBLISH:
                pubSubRouter.handleBrokerPublish((BrokerPublishMessage) message);
                break;
            case ACK:
                handleAcknowledgement((AckMessage) message);
                break;
            case HEARTBEAT:
                updateHeartbeat(message.getSenderId());
                break;
            case JOIN_CLUSTER:
                raftEngine.getState().onMessageReceived(message);
                break;
            case JOIN_CLUSTER_ACK:
                raftEngine.handleJoinClusterAck((JoinClusterAckMessage) message);
                break;
            case REDIRECT_TO_LEADER:
                handleRedirectToLeader((RedirectToLeaderMessage) message);
                break;
            case APPEND_ENTRY_ACK:
                raftEngine.handleAppendEntryAck((AppendEntryAckMessage) message);
                break;
            case COMMIT_ENTRY:
                raftEngine.commitEntry(((CommitEntryMessage) message).getLogIndex());
                break;
            case REQUEST_VOTE:
                raftEngine.handleRequestVote((RequestVoteMessage) message);
                break;
            case REQUEST_VOTE_ACK:
                raftEngine.handleRequestVoteAck((RequestVoteAckMessage) message);
                break;
            case INSTALL_SNAPSHOT:
                raftEngine.handleInstallSnapshot((InstallSnapshotMessage) message);
                break;
            case INSTALL_SNAPSHOT_ACK:
                raftEngine.handleInstallSnapshotAck((InstallSnapshotAckMessage) message);
                break;

            default:
                System.err.println("Unknown message type received.");
        }
    }

    void handleAcknowledgement(AckMessage msg) {
        String senderId = msg.getSenderId();
        updateHeartbeat(senderId);

        ackTracker.handleAck(msg.getAcknowledgedMessageId(), senderId);
    }

    public void updateHeartbeat(String senderId) {

        System.out.println(this.BROKER_ID + " received heartbeat from " + senderId);

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
        if (crashed) return;
        //if (raftEngine.getRole() != BrokerRole.LEADER)
        //    return;

        HeartbeatMessage hm = new HeartbeatMessage(BROKER_ID, 0, System.currentTimeMillis(), raftEngine.getCurrentTerm(), getCurrentLeaderId());
        for (BrokerHandler handler : connectionRegistry.getConnectedBrokerHandlers()) {
            handler.send(hm);
            handler.updateHeartbeat();
            raftEngine.sendAppendEntriesToFollower(handler.getNodeId());
        }
    }

    /*
    private void sendHeartbeat() {
        HeartbeatMessage hm = new HeartbeatMessage(BROKER_ID, 0, System.currentTimeMillis());

        for (BrokerHandler handler : connectionRegistry.getConnectedBrokerHandlers()) {
            handler.send(hm);
        }
    }
    */

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

    private void startElectionTimeoutChecker() {
        scheduler.scheduleAtFixedRate(this::checkElectionTimeout, raftEngine.getElectionTimeoutMs(), 10, TimeUnit.MILLISECONDS);
    }

    private void checkElectionTimeout() {
        raftEngine.checkElectionTimeout();
    }

    public void updateLeaderHeartbeat() {
        raftEngine.updateLeaderHeartbeat();
    }

    public synchronized void startElection() {
        raftEngine.startElection();
    }

    public synchronized void handleRequestVote(RequestVoteMessage msg) {
        raftEngine.handleRequestVote(msg);
    }

    public synchronized void handleRequestVoteAck(RequestVoteAckMessage msg) {
        raftEngine.handleRequestVoteAck(msg);
    }

    public synchronized void becomeFollower(int term, String leaderId) {
        raftEngine.becomeFollower(term, leaderId);
    }

    public synchronized void replicateToFollowers(Message msg) throws Exception {
        raftEngine.replicateToFollowers(msg);
    }

    public synchronized void sendAppendEntriesToFollower(String followerId) {
        raftEngine.sendAppendEntriesToFollower(followerId);
    }

    public synchronized void handleAppendEntry(AppendEntryMessage msg) throws Exception {
        raftEngine.handleAppendEntry(msg);
    }

    public synchronized void handleAppendEntryAck(AppendEntryAckMessage msg) throws Exception {
        raftEngine.handleAppendEntryAck(msg);
    }

    public synchronized void commitEntry(int logIndex) {
        raftEngine.commitEntry(logIndex);
    }

    public void applyToLocalState(Message message) {
        pubSubRouter.applyToLocalState(message);
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
        if (raftEngine.getLeaderHost() == null || raftEngine.getLeaderClientPort() <= 0) {
            System.err.println(BROKER_ID + ": cannot redirect client " + clientId + " — leader address unknown");
            return;
        }
        long seq = brokerCounter.incrementAndGet();
        RedirectToLeaderMessage redirect = new RedirectToLeaderMessage(
                BROKER_ID, seq, System.currentTimeMillis(),
                raftEngine.getLeaderHost(), raftEngine.getLeaderPort(), raftEngine.getLeaderClientPort());
        ConnectionHandler handler = connectionRegistry.getConnectionHandler(clientId);
        if (handler != null) {
            handler.send(redirect);
            System.out.println(BROKER_ID + " redirecting client " + clientId
                    + " to leader at " + raftEngine.getLeaderHost() + ":" + raftEngine.getLeaderClientPort());
        }
    }

    /**
     * Called by ClientHandler when a client connects (first time or reconnect).
     * On reconnect: updates the handler and delivers any buffered offline messages.
     */
    public void addClientToRegistry(String clientId, ClientHandler handler) {
        if (raftEngine.getRole() == BrokerRole.FOLLOWER) {
            redirectClientToLeader(clientId);
            return;
        }

        stateLock.readLock().lock();
        try {
            boolean isReconnect = connectionRegistry.isKnownClient(clientId);
            connectionRegistry.registerClient(clientId, handler); // always replaces stale handler

            if (isReconnect) {
                System.out.println("Client " + clientId + " reconnected. Delivering offline messages…");
                pubSubRouter.deliverOfflineMessages(clientId);
            } else {
                System.out.println("Client " + clientId + " connected for the first time.");
            }
        } finally {
            stateLock.readLock().unlock();
        }

    }

    public String getBrokerId() {
        return BROKER_ID;
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
        getStateLock().readLock().lock();
        try{
            // keep record, drop handler
            connectionRegistry.markClientOffline(clientId);
            pubSubRouter.onClientDisconnection(clientId);
            // topicSubscribersTable.removeNonDurableSubscriptions(clientId); // clean non-durable subs

        } finally {
            stateLock.readLock().unlock();
        }
    }

    private ReentrantReadWriteLock getStateLock() {
        return stateLock;
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

    public List<Message> gatherStateForSnapshot() {
        List<Message> snapshotState = new java.util.ArrayList<>();
        long currentTime = System.currentTimeMillis();

        // 1. Saves subscriptions
        for (String topic : topicSubscribersTable.getTopics()) {
            for (Map.Entry<String, String> entry : topicSubscribersTable.getInterestedBrokersInfo(topic).entrySet()) {
                snapshotState.add(new BrokerSubscribeMessage(entry.getValue(), 0, currentTime, entry.getKey(), topic));
            }
            for (String clientId : topicSubscribersTable.getInterestedClients(topic)) {
                boolean durable = topicSubscribersTable.isDurableSubscriber(clientId, topic);
                snapshotState.add(new SubscribeMessage(clientId, 0, currentTime, topic, durable));
            }
        }

        // 2. Saves pending messages
        for (PendingMessage pending : ackTracker.getPendingMessages()) {
            snapshotState.add(new PendingDeliveryMessage(
                    BROKER_ID, 0, currentTime,
                    pending.getMessage(),
                    new java.util.HashSet<>(pending.getPendingSubscribers())
            ));
        }
        return snapshotState;
    }

    private void takeSnapshot() throws Exception {
        stateLock.writeLock().lock();
        try {
            // Check is some other threads already did the snapshot.
            // The check is done by analyzing if the file is small enough.
            if (wal.getSize() < SNAPSHOT_SIZE_THRESHOLD_BYTES) {
                return;
            }

            List<Message> snapshotState = gatherStateForSnapshot();

            wal.snapshot(snapshotState);

            List<BrokerAddress> clusterPeers = connectionRegistry.getClusterMembers().stream()
                    .map(h -> new BrokerAddress(h.getNodeId(), h.getRemoteHost(), h.getRemoteBrokerPort()))
                    .toList();
            // Use of getClusterMembers() [filtered on isClusterMember() == true] because it saves only members o the
            // cluster raft instead of broker connections that are generic/volatile.

            RaftMetadata raftMetadata = new RaftMetadata(
                    raftEngine.getCurrentTerm(),
                    raftEngine.getVotedFor(),
                    raftEngine.getCommitIndex(),
                    raftEngine.getLastApplied(),
                    raftEngine.getClusterId(),
                    clusterPeers);
            wal.writeRaftMetadata(raftMetadata);

            // Empty the RAM memory or the ArrayList explodes.
            raftEngine.clearLogInMemory();
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    // Called by Raft when a Distributed Snapshot arrives from the Leader
    public void installStateFromSnapshot(List<Message> snapshotState) throws Exception {
        stateLock.writeLock().lock();
        try {
            topicSubscribersTable.clear();
            ackTracker.clear();

            // Rebuild memory using the Leader's snapshot
            for (Message msg : snapshotState) {
                if (msg.getType() == MessageType.SUBSCRIBE) {
                    SubscribeMessage sub = (SubscribeMessage) msg;
                    topicSubscribersTable.addClientSubscription(sub.getTopic(), sub.getSenderId(), sub.isDurable());
                    if (sub.isDurable()) {
                        connectionRegistry.markClientOffline(sub.getSenderId());
                    }
                } else if (msg.getType() == MessageType.BROKER_SUBSCRIBE) {
                    BrokerSubscribeMessage bSub = (BrokerSubscribeMessage) msg;
                    topicSubscribersTable.addBrokerSubscription(bSub.getTopic(), bSub.getInterestedBrokerId(), bSub.getSenderId());
                } else if (msg.getType() == MessageType.SNAPSHOT_PENDING_DELIVERY) {
                    PendingDeliveryMessage pdm = (PendingDeliveryMessage) msg;
                    if (!pdm.getPendingSubscribers().isEmpty()) {
                        ackTracker.trackDelivery(pdm.getOriginalMessage(), pdm.getPendingSubscribers());
                    }
                }
            }

            // Atomically replace the local WAL file with the new snapshot and update RaftMetadata
            wal.snapshot(snapshotState);

            List<BrokerAddress> clusterPeers = connectionRegistry.getClusterMembers().stream()
                    .map(h -> new BrokerAddress(h.getNodeId(), h.getRemoteHost(), h.getRemoteBrokerPort()))
                    .toList();

            RaftMetadata raftMetadata = new RaftMetadata(
                    raftEngine.getCurrentTerm(),
                    raftEngine.getVotedFor(),
                    raftEngine.getCommitIndex(),
                    raftEngine.getLastApplied(),
                    raftEngine.getClusterId(),
                    clusterPeers);
            wal.writeRaftMetadata(raftMetadata);

            messagesSinceLastCheck.set(0);

            System.out.println(BROKER_ID + " successfully installed distributed snapshot and synced with Leader.");
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    // ------------------------------------------------------------------
    // Cluster lifecycle
    // ------------------------------------------------------------------

    public void createCluster() {
        raftEngine.createCluster();
    }

    public void joinCluster(String host, int port) throws Exception {
        if (raftEngine.getRole() != BrokerRole.SOLO) {
            System.err.println("joinCluster() chiamato su un broker che non è SOLO (ruolo attuale: " + raftEngine.getRole() + ")");
            return;
        }

        BrokerHandler handler = connectToPeer(host, port);

        long seq = brokerCounter.incrementAndGet();
        JoinClusterMessage joinMsg = new JoinClusterMessage(BROKER_ID, seq, System.currentTimeMillis());
        handler.send(joinMsg);

        //System.out.println(BROKER_ID + " ha inviato JoinClusterMessage a " + host + ":" + port);
    }

    public void handleJoinClusterRequest(JoinClusterMessage msg) {
        raftEngine.handleJoinClusterRequest(msg);
    }

    private void handleRedirectToLeader(RedirectToLeaderMessage msg) throws Exception {
        System.out.println(BROKER_ID + " rediretto al leader " + msg.getLeaderHost() + ":" + msg.getLeaderPort());
        joinCluster(msg.getLeaderHost(), msg.getLeaderPort());
    }

    public void appendToWal(Message msg) throws Exception {
        wal.append(msg);
    }

    public ExecutorService getExecutor() {
        return executor;
    }

    //----------------------------------------------------------
    // TEST FUNCTIONS
    //----------------------------------------------------------

    public int getCurrentTerm() {
        return raftEngine.getCurrentTerm();
    }

    public synchronized int getLastLogIndex() {
        return raftEngine.getLastLogIndex();
    }

    public synchronized int getLastLogTerm() {
        return raftEngine.getLastLogTerm();
    }

    public BrokerRole getRole() {
        return raftEngine.getRole();
    }

    public int getCommitIndex() {
        return raftEngine.getCommitIndex();
    }

    public int getLastApplied() {
        return raftEngine.getLastApplied();
    }

    public boolean isSubscribed(String clientId, String topic) {
        return topicSubscribersTable.getInterestedClients(topic).contains(clientId);
    }

    public synchronized void crash() {
        if (crashed) {
            System.out.println("[" + BROKER_ID + "] already crashed — ignoring duplicate crash()");
            return;
        }

        System.out.println("\n╔══════════════════════════════════════════╗");
        System.out.println(  "║  CRASH: " + BROKER_ID
                + " (term=" + raftEngine.getCurrentTerm() + ", role=" + raftEngine.getRole() + ")");
        System.out.println(  "╚══════════════════════════════════════════╝\n");

        crashed = true;
        raftEngine.setState(new SoloState(this));
        new ArrayList<>(connectionRegistry.getConnectedBrokerHandlers())
                .forEach(h -> h.forceDisconnect());
        try {
            if (clientServerSocket != null) clientServerSocket.close();
        } catch (IOException e) {

        }
        try {
            if (brokerServerSocket != null)
                brokerServerSocket.close();
        }
        catch (IOException e) {

        }

        System.out.println("[CRASH] " + BROKER_ID + " is now offline. " + "Remaining brokers will detect the absence and elect a new leader.");
    }

    public void resurrect(String peerHost, int peerBrokerPort) throws Exception {
        if (!crashed) {
            System.out.println("[" + BROKER_ID + "] not crashed — ignoring resurrect()");
            return;
        }

        System.out.println("\n╔══════════════════════════════════════════╗");
        System.out.println(  "║  RESURRECT: " + BROKER_ID + " coming back online...");
        System.out.println(  "╚══════════════════════════════════════════╝\n");

        crashed = false;
        raftEngine.reset();
        executor.submit(this::runClientListener);
        executor.submit(this::runBrokerListener);

        Thread.sleep(500);

        joinCluster(peerHost, peerBrokerPort);
        System.out.println("[RESURRECT] " + BROKER_ID + " sent join request — waiting for cluster acceptance...");
    }

    public String getCurrentLeaderId() {
        return raftEngine.getCurrentLeaderId();
    }
}