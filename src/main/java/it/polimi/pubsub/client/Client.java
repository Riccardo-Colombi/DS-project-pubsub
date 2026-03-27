package it.polimi.pubsub.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import it.polimi.pubsub.messages.*;
import it.polimi.pubsub.network.TcpConnection;

import java.io.IOException;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class Client implements Publisher, Subscriber {

    private static final long HEARTBEAT_INTERVAL_MS = 3000;
    private static final long ACK_TIMEOUT_MS = 5000;
    private static final long RETRY_INTERVAL_MS = 2000;

    private final String clientId;
    private final String brokerHost;
    private final int brokerPort;

    private TcpConnection connection;
    private ExecutorService executor;
    private ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean disconnected = new AtomicBoolean(false);
    private final AtomicLong sequenceCounter = new AtomicLong(0);

    private final BlockingQueue<Message> outQueue = new LinkedBlockingQueue<>();

    // tracks messages sent to the broker waiting for an ack
    private final ConcurrentHashMap<String, ClientPendingMessage> pendingMessages = new ConcurrentHashMap<>();

    public Client(String clientId, String brokerHost, int brokerPort) {
        this.clientId = clientId;
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
    }

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
        startHeartbeat();
        startRetryScheduler();

        System.out.println("Connected to broker at " + brokerHost + ":" + brokerPort);
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
            System.out.println("Thread interrupted: " + e.getMessage());
        }  catch (JsonProcessingException e) {
            System.out.println("Deserialization error: " + e.getMessage());
        } catch (IOException e) {
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

    private void startHeartbeat() {
        scheduler.scheduleAtFixedRate(this::sendHeartbeat, HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    // todo maybe remove sequence number from heartbeat
    private void sendHeartbeat() {
        HeartbeatMessage heartbeat = new HeartbeatMessage(clientId, 0, System.currentTimeMillis());
        enqueue(heartbeat);
    }

    private void startRetryScheduler() {
        scheduler.scheduleAtFixedRate(this::retryExpiredMessages, RETRY_INTERVAL_MS, RETRY_INTERVAL_MS, TimeUnit.MILLISECONDS);
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
        System.out.println("Message received! Topic: " + msg.getTopic() + " Payload: " + new String(msg.getPayload()));

        // send ack back to broker
        long seqNum = sequenceCounter.incrementAndGet();
        AckMessage ack = new AckMessage(clientId, seqNum, System.currentTimeMillis(), msg.getMessageId());
        enqueue(ack);
    }

    private void enqueue(Message msg) {
        try {
            outQueue.put(msg);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // --- Publisher ---

    @Override
    public void publish(String topic, byte[] payload) {
        long seqNum = sequenceCounter.incrementAndGet();
        PublishMessage msg = new PublishMessage(clientId, seqNum, System.currentTimeMillis(), topic, payload);
        enqueue(msg);
        pendingMessages.put(msg.getMessageId(), new ClientPendingMessage(msg, brokerHost));
    }

    // --- Subscriber ---

    @Override
    public void subscribe(String topic) {
        long seqNum = sequenceCounter.incrementAndGet();
        SubscribeMessage msg = new SubscribeMessage(clientId, seqNum, System.currentTimeMillis(), topic);
        enqueue(msg);
        pendingMessages.put(msg.getMessageId(), new ClientPendingMessage(msg, brokerHost));
    }

    @Override
    public void unsubscribe(String topic) {
        long seqNum = sequenceCounter.incrementAndGet();
        UnsubscribeMessage msg = new UnsubscribeMessage(clientId, seqNum, System.currentTimeMillis(), topic);
        enqueue(msg);
        pendingMessages.put(msg.getMessageId(), new ClientPendingMessage(msg, brokerHost));
    }
}
