package it.polimi.pubsub.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import it.polimi.pubsub.messages.*;
import it.polimi.pubsub.messages.brokerMessages.RedirectToLeaderMessage;
import it.polimi.pubsub.network.TcpConnection;
import it.polimi.pubsub.utils.ConsoleAppender;

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

    private volatile boolean autoReconnect = true;

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

        ConsoleAppender.asyncMessage(String.format("[Client %s] Connected to broker at %s:%d", clientId, currentHost, currentPort));
    }

    public void disconnect() {
        running.set(false);

        executor.shutdownNow();
        scheduler.shutdownNow();

        try {
            connection.close();
        } catch (Exception e) {
            ConsoleAppender.asyncMessage(String.format("[Client %s] [ERROR] Socket close failed.", clientId));
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
                    ConsoleAppender.asyncMessage(String.format("[Client %s] Broker disconnected.", clientId));
                    break;
                }

                handleMessage(msg);
            }
        }  catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }  catch (JsonProcessingException e) {
            ConsoleAppender.asyncMessage(String.format("[Client %s] [ERROR] Deserialization failed.", clientId));
        } catch (IOException e) {
            if (running.get())
                ConsoleAppender.asyncMessage(String.format("[Client %s] [ERROR] Connection lost.", clientId));
        } catch (Exception e) {
            ConsoleAppender.asyncMessage(String.format("[Client %s] [ERROR] Unexpected read error.", clientId));
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
            ConsoleAppender.asyncMessage(String.format("[Client %s] [ERROR] Message write failed.", clientId));
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
                    ConsoleAppender.asyncMessage(String.format("[Client %s] Redirect target unreachable, falling back to bootstrap.",
                            clientId));
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
                    ConsoleAppender.asyncMessage(String.format("[Client %s] Reconnect attempt %d/%d failed.",
                            clientId, attempt, MAX_RECONNECT_ATTEMPTS));

                    if (attempt == 1 && !(bootstrapHost.equals(currentHost) && bootstrapPort == currentPort)) {
                        currentHost = bootstrapHost;
                        currentPort = bootstrapPort;
                    }
                }
            }
            ConsoleAppender.asyncMessage(String.format("[Client %s] [ERROR] Gave up reconnecting after %d attempts.",
                    clientId, MAX_RECONNECT_ATTEMPTS));
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
        // added term = 0 so that brokers ignore clients heartbeats for updates
        enqueue(new HeartbeatMessage(clientId, 0, System.currentTimeMillis(), 0, null, false, -1));
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
            ConsoleAppender.asyncMessage(String.format("[Client %s] Re-subscribing to durable topic: %s", clientId, topic));
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
                ConsoleAppender.asyncMessage(String.format("[Client %s] [ERROR] Unexpected message type: %s", clientId, msg.getType()));
        }
    }

    private void handleAcknowledgement(AckMessage msg) {
        // broker confirmed it received one of our messages
        pendingMessages.remove(msg.getAcknowledgedMessageId());
    }

    private void handleIncomingPublish(PublishMessage msg) throws Exception {
        ConsoleAppender.asyncMessage(String.format("[Client %s] [Topic: %s] %s",
                clientId, msg.getTopic(), new String(msg.getPayload())));

        // send ack back to broker
        long seqNum = sequenceCounter.incrementAndGet();
        AckMessage ack = new AckMessage(clientId, seqNum, System.currentTimeMillis(), msg.getMessageId());
        enqueue(ack);
    }

    private void handleRedirectToLeader(RedirectToLeaderMessage msg) {
        String newHost = msg.getLeaderHost();
        int    newPort = msg.getLeaderClientPort();

        ConsoleAppender.asyncMessage(String.format("[Client %s] Redirected to leader at %s:%d", clientId, newHost, newPort));

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
