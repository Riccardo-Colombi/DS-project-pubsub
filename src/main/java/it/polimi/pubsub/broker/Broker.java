package it.polimi.pubsub.broker;

import it.polimi.pubsub.broker.components.*;
import it.polimi.pubsub.broker.storage.WriteAheadLog;
import it.polimi.pubsub.messages.*;
import it.polimi.pubsub.network.TcpConnection;

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
        for (Message msg : wal.recover()) {
            // Replay the historical messages to rebuild RAM state
            dispatchMessage(msg, null);
        }
    }

    public void onMessageReceived(Message message, TcpConnection connection) throws Exception {
        if (connection != null) {
            // TODO: check if HEARTBEAT
            wal.append(message);
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
}
