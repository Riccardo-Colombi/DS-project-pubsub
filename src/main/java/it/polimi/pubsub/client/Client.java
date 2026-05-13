package it.polimi.pubsub.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import it.polimi.pubsub.messages.*;
import it.polimi.pubsub.messages.brokerMessages.RedirectToLeaderMessage;
import it.polimi.pubsub.network.TcpConnection;

import java.io.IOException;
import java.net.Socket;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class Client implements Publisher, Subscriber {

    private static final long HEARTBEAT_INTERVAL_MS   = 3000;
    private static final long ACK_TIMEOUT_MS          = 5000;
    private static final long RETRY_INTERVAL_MS       = 2000;
    private static final int  MAX_RECONNECT_ATTEMPTS  = 10;

    private final String clientId;

    private final String bootstrapHost;
    private final int    bootstrapPort;

    private volatile String currentHost;
    private volatile int    currentPort;

    private final Set<String> durableTopics = ConcurrentHashMap.newKeySet();

    private TcpConnection            connection;
    private ExecutorService          executor;
    private ScheduledExecutorService scheduler;

    private final AtomicBoolean running         = new AtomicBoolean(false);
    private final AtomicBoolean disconnected    = new AtomicBoolean(false);
    private final AtomicLong sequenceCounter    = new AtomicLong(0);

    private final AtomicBoolean wasRedirected = new AtomicBoolean(false);

    private volatile boolean autoReconnect = false;

    private final BlockingQueue<Message> outQueue = new LinkedBlockingQueue<>();

    // tracks messages sent to the broker waiting for an ack
    private final ConcurrentHashMap<String, ClientPendingMessage> pendingMessages = new ConcurrentHashMap<>();

    public Client(String clientId, String brokerHost, int brokerPort) {
        this.clientId      = clientId;
        this.bootstrapHost = brokerHost;
        this.bootstrapPort = brokerPort;
        this.currentHost   = brokerHost;
        this.currentPort   = brokerPort;
    }

    // todo check if it will work during leader election

    public void enableAutoReconnect(boolean enable) {
        this.autoReconnect = enable;
    }

    // ------------------------------------------------------------------
    // Connection lifecycle
    // ------------------------------------------------------------------

    public void connect() throws Exception {
        Socket socket = new Socket(currentHost, currentPort);
        this.connection = new TcpConnection(socket);

        disconnected.set(false);
        running.set(true);

        // once is interrupted using .shutdownNow(); i have to recreate the object to reuse it
        this.executor = Executors.newCachedThreadPool();
        this.scheduler = Executors.newScheduledThreadPool(2);

        executor.submit(this::readLoop);
        executor.submit(this::writeLoop);

        // Send a heartbeat immediately so the broker registers this client's ID
        // without waiting for the first application message.
        sendHeartbeat();

        startHeartbeat();
        startRetryScheduler();

        // Re-subscribe durable topics on reconnect so the broker resumes direct
        // routing and knows the client is back online.
        resubscribeDurableTopics();

        System.out.println(clientId + " connected to broker at " + currentHost + ":" + currentPort);
    }

    public void disconnect() {
        running.set(false);

        executor.shutdownNow();
        scheduler.shutdownNow();

        try {
            connection.close();
        } catch (Exception e) {
            System.out.println("Error closing connection: " + e.getMessage());
        }
    }

    // ------------------------------------------------------------------
    // I/O loops
    // ------------------------------------------------------------------

    private void readLoop() {
        try {
            while (running.get()) {
                Message msg = connection.receive();

                if (msg == null) {
                    System.out.println("Broker disconnected");
                    break;
                }

                handleMessage(msg);
            }
        }  catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }  catch (JsonProcessingException e) {
            System.out.println("Deserialization error: " + e.getMessage());
        } catch (IOException e) {
            if (running.get())
                System.out.println("Connection lost: " + e.getMessage());
        } catch (Exception e) {
            System.out.println("Generic error: " + e.getMessage());
        } finally {
            handleLoopExit();
        }
    }

    private void writeLoop() {
        try {
            while (running.get()) {
                Message msg = outQueue.take();
                connection.send(msg);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            System.out.println("Write error: " + e.getMessage());
        } finally {
            handleLoopExit();
        }
    }

    private void handleLoopExit() {

        if (!disconnected.compareAndSet(false, true)){
            return;
        }

        boolean redirect = wasRedirected.getAndSet(false);
        disconnect();

        // reconnection if it was redirected or disconnected from the broker

        if (redirect) {
            scheduleReconnect(true);
        } else if (autoReconnect) {
            scheduleReconnect(false);
        }
    }

    // ------------------------------------------------------------------
    // Reconnection
    // ------------------------------------------------------------------

    private void scheduleReconnect(boolean quick) {
        Thread t = new Thread(() -> {
            if (quick) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    return;
                }
                try {
                    connect();
                    return;
                } catch (Exception e) {
                    System.out.println(clientId + ": redirect target " + currentHost + ":" + currentPort
                            + " unreachable (" + e.getMessage() + "), falling back to bootstrap");
                    currentHost = bootstrapHost;
                    currentPort = bootstrapPort;
                }
            }

            for (int attempt = 1; attempt <= MAX_RECONNECT_ATTEMPTS; attempt++) {
                try {
                    Thread.sleep(RETRY_INTERVAL_MS);
                } catch (InterruptedException e) {
                    return;
                }
                try {
                    connect();
                    return;
                } catch (Exception e) {
                    System.out.println(clientId + ": reconnect attempt " + attempt + "/" + MAX_RECONNECT_ATTEMPTS
                            + " to " + currentHost + ":" + currentPort + " failed: " + e.getMessage());

                    if (attempt == 1 && !(bootstrapHost.equals(currentHost) && bootstrapPort == currentPort)) {
                        currentHost = bootstrapHost;
                        currentPort = bootstrapPort;
                    }
                }
            }
            System.out.println(clientId + ": gave up reconnecting after " + MAX_RECONNECT_ATTEMPTS + " attempts");
        }, clientId + "-reconnect");
        t.setDaemon(true);
        t.start();
    }

    // ------------------------------------------------------------------
    // Heartbeat & retry
    // ------------------------------------------------------------------

    private void startHeartbeat() {
        scheduler.scheduleAtFixedRate(this::sendHeartbeat,
                HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void sendHeartbeat() {
        enqueue(new HeartbeatMessage(clientId, 0, System.currentTimeMillis()));
    }

    private void startRetryScheduler() {
        scheduler.scheduleAtFixedRate(this::retryExpiredMessages,
                RETRY_INTERVAL_MS, RETRY_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void retryExpiredMessages() {
        long currentTime = System.currentTimeMillis();
        for (Map.Entry<String, ClientPendingMessage> entry : pendingMessages.entrySet()) {
            ClientPendingMessage pending = entry.getValue();
            if (currentTime - pending.getTrackingTimestamp() > ACK_TIMEOUT_MS) {
                enqueue(pending.getMessage());
                pending.resetTimestamp();
            }
        }
    }

    // ------------------------------------------------------------------
    // Durable reconnect
    // ------------------------------------------------------------------

    private void resubscribeDurableTopics() {
        for (String topic : durableTopics) {
            System.out.println("Re-subscribing durable topic after reconnect: " + topic);
            sendSubscribeMessage(topic, true);
        }
    }

    // ------------------------------------------------------------------
    // Incoming message handling
    // ------------------------------------------------------------------

    private void handleMessage(Message msg) throws Exception {
        switch (msg.getType()) {
            case ACK:
                handleAcknowledgement((AckMessage) msg);
                break;
            case PUBLISH:
                handleIncomingPublish((PublishMessage) msg);
                break;
            case REDIRECT_TO_LEADER:
                handleRedirectToLeader((RedirectToLeaderMessage) msg);
                break;
            default:
                System.out.println("Unexpected message type: " + msg.getType());
        }
    }

    private void handleAcknowledgement(AckMessage msg) {
        // broker confirmed it received one of our messages
        pendingMessages.remove(msg.getAcknowledgedMessageId());
    }

    private void handleIncomingPublish(PublishMessage msg) throws Exception {
        System.out.println("Message received! Client: " + clientId +
                "Topic: " + msg.getTopic() +
                " Payload: " + new String(msg.getPayload()));

        // send ack back to broker
        long seqNum = sequenceCounter.incrementAndGet();
        AckMessage ack = new AckMessage(clientId, seqNum, System.currentTimeMillis(), msg.getMessageId());
        enqueue(ack);
    }

    private void handleRedirectToLeader(RedirectToLeaderMessage msg) {
        String newHost = msg.getLeaderHost();
        int    newPort = msg.getLeaderClientPort();
        System.out.println(clientId + ": redirected to leader at " + newHost + ":" + newPort);

        currentHost = newHost;
        currentPort = newPort;
        wasRedirected.set(true);

        // Close the socket so receive() throws and readLoop exits to its finally block.
        try { connection.close(); } catch (Exception ignored) {}
    }

    // ------------------------------------------------------------------
    // Outbound helpers
    // ------------------------------------------------------------------

    private void enqueue(Message msg) {
        try {
            outQueue.put(msg);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void sendSubscribeMessage(String topic, boolean durable) {
        long seqNum = sequenceCounter.incrementAndGet();
        SubscribeMessage msg = new SubscribeMessage(clientId, seqNum,
                System.currentTimeMillis(), topic, durable);
        enqueue(msg);
        pendingMessages.put(msg.getMessageId(), new ClientPendingMessage(msg, currentHost));
    }

    // ------------------------------------------------------------------
    // Publisher
    // ------------------------------------------------------------------

    @Override
    public void publish(String topic, byte[] payload) {
        long seqNum = sequenceCounter.incrementAndGet();
        PublishMessage msg = new PublishMessage(clientId, seqNum, System.currentTimeMillis(), topic, payload);
        enqueue(msg);
        pendingMessages.put(msg.getMessageId(), new ClientPendingMessage(msg, currentHost));
    }

    // ------------------------------------------------------------------
    // Subscriber
    // ------------------------------------------------------------------

    @Override
    public void subscribe(String topic) {
        sendSubscribeMessage(topic, false);
    }

    /**
     * Subscribe with durability: the broker buffers messages while this client
     * is offline and delivers them on reconnect.
     */
    public void subscribeDurable(String topic) {
        durableTopics.add(topic);
        sendSubscribeMessage(topic, true);
    }

    @Override
    public void unsubscribe(String topic) {
        durableTopics.remove(topic); // stop re-subscribing on future reconnects
        long seqNum = sequenceCounter.incrementAndGet();
        UnsubscribeMessage msg = new UnsubscribeMessage(clientId, seqNum,
                System.currentTimeMillis(), topic);
        enqueue(msg);
        pendingMessages.put(msg.getMessageId(), new ClientPendingMessage(msg, currentHost));
    }
}
