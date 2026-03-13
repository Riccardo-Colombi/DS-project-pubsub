package it.polimi.pubsub.broker;

import it.polimi.pubsub.broker.components.*;
import it.polimi.pubsub.broker.storage.WriteAheadLog;
import it.polimi.pubsub.messages.*;
import it.polimi.pubsub.network.TcpConnection;

import java.util.List;

public class Broker {

    private final TopicSubscribersTable topicSubscribersTable;
    private final ClientRegistry clientRegistry;
    private final AckTracker ackTracker;
    private final WriteAheadLog wal;

    private long brokerCounter;

    public Broker(String walFilePath) throws Exception {
        this.topicSubscribersTable = new TopicSubscribersTable();
        this.clientRegistry = new ClientRegistry();
        this.ackTracker = new AckTracker();
        this.wal = new WriteAheadLog(walFilePath);

        recoverState();
    }

    private void recoverState() throws Exception {

        // Updating brokerCounter during the recovery
        List<Message> history = wal.recover();

        for (Message msg : wal.recover()) {
            if (msg.getSequenceNumber() > this.brokerCounter) {
                this.brokerCounter = msg.getSequenceNumber();
            }
            // Replay the historical messages to rebuild RAM state
            dispatchMessage(msg, null);
        }
    }

    public void onMessageReceived(Message message, TcpConnection connection) throws Exception {
        if (connection != null) {
            // TODO: check if HEARTBEAT
            wal.append(message);

            // If dim(wal) > 10MB -> take snapshot

            if (wal.getSize() > 10 * 1024 * 1024) {
                System.out.println("Starting snapshot");
                takeSnapshot();
            }
        }
        dispatchMessage(message, connection);
    }

    private void dispatchMessage(Message message, TcpConnection connection) throws Exception {
        switch (message.getType()) {
            case SUBSCRIBE:
                handleSubscribe((SubscribeMessage) message, connection);
                break;
            case PUBLISH:
                handlePublish((PublishMessage) message, connection);
                break;
            case UNSUBSCRIBE:
                // TODO: handleUnsubscribe(...)
                break;
            case ACK:
                // TODO: handleAcknowledgement(...)
                break;
            case HEARTBEAT:
                // TODO: handleHeartbeat(...)
                break;
            default:
                System.err.println("Unknown message type received.");
        }
    }

    private void handleSubscribe(SubscribeMessage msg, TcpConnection connection) throws Exception {
        String clientId = msg.getSenderId();

        if (connection != null) {
            clientRegistry.registerClient(clientId, connection);
        }

        topicSubscribersTable.addSubscription(msg.getTopic(), clientId);

        if (connection != null) {
            long brokerSeqNum = ++this.brokerCounter;
            long currentTime = System.currentTimeMillis();

            Message ack = new AckMessage("Broker", brokerSeqNum, currentTime, msg.getMessageId());

            connection.send(ack);
        }
    }

    private void handlePublish(PublishMessage msg, TcpConnection connection) throws Exception {
        String topic = msg.getTopic();
        java.util.Set<String> interestedClients = topicSubscribersTable.getSubscribers(topic);

        if (!interestedClients.isEmpty()) {
            ackTracker.trackDelivery(msg, interestedClients);

            for (String clientId : interestedClients) {
                ClientSession session = clientRegistry.getSession(clientId);

                if (session != null && session.getStatus() == ClientStatus.CONNECTED) {
                    session.sendMessage(msg);
                }
            }
        }

        if (connection != null) {
            long brokerSeqNum = ++this.brokerCounter;
            long currentTime = System.currentTimeMillis();

            Message ack = new AckMessage("Broker", brokerSeqNum, currentTime, msg.getMessageId());

            connection.send(ack);
        }
    }

    private synchronized void takeSnapshot() throws Exception {
        List<Message> snapshotState = new java.util.ArrayList<>();
        long currentTime = System.currentTimeMillis();

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

        for (PendingMessage pending : ackTracker.getPendingMessages()) {
            snapshotState.add(pending.getMessage());
        }

        wal.snapshot(snapshotState);
        System.out.println("Snapshot completato: " + snapshotState.size() + " elementi salvati.");
    }
}
