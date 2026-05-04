package it.polimi.pubsub.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import it.polimi.pubsub.messages.*;
import it.polimi.pubsub.network.TcpConnection;

import java.io.IOException;
import java.net.Socket;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class Client implements Publisher, Subscriber {

    private static final long HEARTBEAT_INTERVAL_MS = 3000;
    private static final long ACK_TIMEOUT_MS        = 5000;
    private static final long RETRY_INTERVAL_MS     = 2000;

    private final String clientId;
    private final String brokerHost;
    private final int    brokerPort;

    // Topics for which this client has a durable subscription.
    // Survives connect/disconnect cycles so we can re-subscribe on reconnect.
    private final Set<String> durableTopics = ConcurrentHashMap.newKeySet();

    private TcpConnection            connection;
    private ExecutorService          executor;
    private ScheduledExecutorService scheduler;

    private final AtomicBoolean running         = new AtomicBoolean(false);
    private final AtomicBoolean disconnected    = new AtomicBoolean(false);
    private final AtomicLong sequenceCounter    = new AtomicLong(0);

    private final BlockingQueue<Message> outQueue = new LinkedBlockingQueue<>();

    // tracks messages sent to the broker waiting for an ack
    private final ConcurrentHashMap<String, ClientPendingMessage> pendingMessages = new ConcurrentHashMap<>();

    public Client(String clientId, String brokerHost, int brokerPort) {
        this.clientId   = clientId;
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
    }

    // ------------------------------------------------------------------
    // Connection lifecycle
    // ------------------------------------------------------------------

    public void connect() throws Exception {
        Socket socket = new Socket(brokerHost, brokerPort);
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

        System.out.println(clientId + " connected to broker at " + brokerHost + ":" + brokerPort);
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
            if (disconnected.compareAndSet(false, true))
                disconnect();
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
            if (disconnected.compareAndSet(false, true))
                disconnect();
        }
    }

    // ------------------------------------------------------------------
    // Heartbeat & retry
    // ------------------------------------------------------------------

    private void startHeartbeat() {
        scheduler.scheduleAtFixedRate(this::sendHeartbeat, HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
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
        pendingMessages.put(msg.getMessageId(), new ClientPendingMessage(msg, brokerHost));
    }

    // ------------------------------------------------------------------
    // Publisher
    // ------------------------------------------------------------------

    @Override
    public void publish(String topic, byte[] payload) {
        long seqNum = sequenceCounter.incrementAndGet();
        PublishMessage msg = new PublishMessage(clientId, seqNum, System.currentTimeMillis(), topic, payload);
        enqueue(msg);
        pendingMessages.put(msg.getMessageId(), new ClientPendingMessage(msg, brokerHost));
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
        pendingMessages.put(msg.getMessageId(), new ClientPendingMessage(msg, brokerHost));
    }
}
