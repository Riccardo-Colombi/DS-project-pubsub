package it.polimi.pubsub.broker.components;

import it.polimi.pubsub.broker.Broker;
import it.polimi.pubsub.messages.Message;
import it.polimi.pubsub.messages.PublishMessage;
import it.polimi.pubsub.messages.SubscribeMessage;
import it.polimi.pubsub.messages.UnsubscribeMessage;
import it.polimi.pubsub.messages.brokerMessages.BrokerPublishMessage;
import it.polimi.pubsub.messages.brokerMessages.BrokerSubscribeMessage;
import it.polimi.pubsub.messages.brokerMessages.BrokerUnsubscribeMessage;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import it.polimi.pubsub.utils.ConsoleAppender;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class PubSubRouter {

    private final String BROKER_ID;
    private final Broker broker;
    private final TopicSubscribersTable topicSubscribersTable;
    private final OfflineMessageStore offlineStore;
    private final ConnectionRegistry connectionRegistry;
    private final AckTracker ackTracker;

    private final Cache<String, Boolean> messageCache = Caffeine.newBuilder()
            .maximumSize(10000)
            .expireAfterWrite(2, TimeUnit.MINUTES)
            .build();

    public PubSubRouter(String brokerId, Broker broker,
                        TopicSubscribersTable topicSubscribersTable,
                        OfflineMessageStore offlineStore,
                        ConnectionRegistry connectionRegistry,
                        AckTracker ackTracker) {

        this.BROKER_ID = brokerId;
        this.broker = broker;
        this.topicSubscribersTable = topicSubscribersTable;
        this.offlineStore = offlineStore;
        this.connectionRegistry = connectionRegistry;
        this.ackTracker = ackTracker;
    }

    public void handleSubscribe(SubscribeMessage msg) {
        String clientId = msg.getSenderId();
        broker.updateHeartbeat(clientId);

        String subscribeMessageId = msg.getMessageId();
        boolean newMessage = messageCache.asMap().putIfAbsent(subscribeMessageId, true) == null;

        if (newMessage) {

            // Pass durable flag through to the table
            boolean newTopicOfInterest = topicSubscribersTable.addClientSubscription(msg.getTopic(), clientId, msg.isDurable());

            if (newTopicOfInterest) {
                forwardSubscribeMessage(msg.getTopic(), BROKER_ID, null);
            }
        }
        broker.sendAck(clientId, msg.getMessageId());
    }

    public void handleBrokerSubscribe(BrokerSubscribeMessage msg) {
        String senderId = msg.getSenderId();
        broker.updateHeartbeat(senderId);

        String messageId = msg.getMessageId();
        boolean newMessage = messageCache.asMap().putIfAbsent(messageId, true) == null;

        if (newMessage) {
            String topic = msg.getTopic();
            String subbedBroker = msg.getInterestedBrokerId();

            if (subbedBroker.equals(BROKER_ID)) {
                broker.sendAck(senderId, msg.getMessageId());
                return;
            }
            boolean newBrokerSubscription = topicSubscribersTable.addBrokerSubscription(topic, subbedBroker, senderId);

            if (newBrokerSubscription) {
                forwardSubscribeMessage(topic, subbedBroker, senderId);
            }
        }
        broker.sendAck(senderId, msg.getMessageId());
    }

    private void forwardSubscribeMessage(String topic, String subbedBrokerId, String previousHopId) {
        long brokerSeqNum = broker.getMessageCounter();
        long currentTime = System.currentTimeMillis();
        BrokerSubscribeMessage sm = new BrokerSubscribeMessage(BROKER_ID, brokerSeqNum, currentTime, subbedBrokerId, topic);

        for (BrokerHandler handler : connectionRegistry.getConnectedBrokerHandlers()) {
            // don't send back the message to the sender
            if (!handler.getNodeId().equals(previousHopId)) {
                handler.send(sm);
            }
        }
    }

    public void handleUnsubscribe(UnsubscribeMessage msg) {
        String clientId = msg.getSenderId();
        broker.updateHeartbeat(clientId);

        String unsubscribeMessageId = msg.getMessageId();
        boolean newMessage = messageCache.asMap().putIfAbsent(unsubscribeMessageId, true) == null;

        if (newMessage) {
            String topic = msg.getTopic();

            boolean removedTopicOfInterest = topicSubscribersTable.removeClientSubscription(topic, clientId);

            if (removedTopicOfInterest) {
                forwardUnsubscribeMessage(topic, BROKER_ID, null);
            }
        }

        // send acknowledgement to the client
        broker.sendAck(clientId, msg.getMessageId());
    }

    public void handleBrokerUnsubscribe(BrokerUnsubscribeMessage msg) {
        String senderId = msg.getSenderId();
        broker.updateHeartbeat(senderId);

        String messageId = msg.getMessageId();
        boolean newMessage = messageCache.asMap().putIfAbsent(messageId, true) == null;

        if (newMessage) {
            String topic = msg.getTopic();
            String unsubscribedBrokerId = msg.getInterestedBrokerId(); // original broker which unsubscribed

            // broker received its unsubscription message back
            if (msg.getInterestedBrokerId().equals(BROKER_ID)) {
                broker.sendAck(senderId, msg.getMessageId());
                return;
            }

            boolean removedBrokerSubscription = topicSubscribersTable.removeBrokerSubscription(topic, unsubscribedBrokerId);

            if (removedBrokerSubscription) {
                forwardUnsubscribeMessage(topic, unsubscribedBrokerId, senderId);
            }
        }

        // send acknowledgement to the broker
        broker.sendAck(senderId, msg.getMessageId());
    }

    private void forwardUnsubscribeMessage(String topic, String unsubscribedBrokerId, String previousHopId) {
        long brokerSeqNum = broker.getMessageCounter();
        long currentTime = System.currentTimeMillis();
        BrokerUnsubscribeMessage sm = new BrokerUnsubscribeMessage(BROKER_ID, brokerSeqNum, currentTime, unsubscribedBrokerId, topic);

        for (BrokerHandler handler : connectionRegistry.getConnectedBrokerHandlers()) {
            // don't send back the message to the sender
            if (!handler.getNodeId().equals(previousHopId)) {
                handler.send(sm);
            }
        }
    }

    public void handlePublish(PublishMessage msg) {
        String publisherId = msg.getSenderId();
        broker.updateHeartbeat(publisherId);

        String publishMessageId = msg.getMessageId();
        boolean newMessage = messageCache.asMap().putIfAbsent(publishMessageId, true) == null;

        if (newMessage) {
            // forward the message to interested clients and brokers
            forwardPublishMessage(msg);
        }

        // send acknowledgment to the publisher
        broker.sendAck(publisherId, msg.getMessageId());
    }

    public void handleBrokerPublish(BrokerPublishMessage msg) {
        String brokerId = msg.getSenderId();
        broker.updateHeartbeat(brokerId);

        String publishMessageId = msg.getOriginalMessageId();
        boolean newMessage = messageCache.asMap().putIfAbsent(publishMessageId, true) == null;

        if (newMessage) {
            forwardPublishMessage(msg.getOriginalMessage());
        }

        // send acknowledgment to the broker which forwarded the message
        broker.sendAck(brokerId, msg.getMessageId());
    }

    /**
     * Deliver a publish message to all interested clients and brokers.
     *
     * For clients:
     *   - ONLINE -> send directly via their handler
     *   - OFFLINE + durable -> buffer in OfflineMessageStore for delivery on reconnect
     *   - OFFLINE + non-durable -> drop (original behaviour)
     */
    private void forwardPublishMessage(PublishMessage msg) {
        String topic = msg.getTopic();
        Set<String> interestedClients = topicSubscribersTable.getInterestedClients(topic);
        Set<String> interestedBrokersNextHops = topicSubscribersTable.getNextHopOfInterestedBrokers(topic);

        // Forward to peer brokers
        if (!interestedBrokersNextHops.isEmpty()) {
            long brokerSeqNum = broker.getMessageCounter();
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
            Set<String> onlineClients = new HashSet<>();

            for (String clientId : interestedClients) {
                ClientHandler handler = connectionRegistry.getClientHandler(clientId);

                if (handler != null) {
                    // Client is online: deliver directly
                    onlineClients.add(clientId);
                    handler.send(msg);
                } else if (topicSubscribersTable.isDurableSubscriber(clientId, topic)) {
                    // Client is offline but durable: buffer for later delivery
                    ConsoleAppender.asyncMessage(String.format("[PubSubRouter] Buffering message %s for the offline client %s",
                            msg.getMessageId(), clientId));
                    try {
                        offlineStore.enqueue(clientId, msg);
                    } catch (Exception e) {
                        ConsoleAppender.asyncMessage(String.format("[PubSubRouter] [Error] Failed to buffer message for %s. Error: %s",
                                clientId, e.getMessage()));
                    }
                }
                // Non-durable offline client: message is dropped (original behavior)
            }

            // Track only online clients, because they are the only one that received the message.
            if (!onlineClients.isEmpty()) {
                ackTracker.trackDelivery(msg, onlineClients);
            }
        }
    }

    public void handleLeaderModification(String oldLeaderId, String newLeaderId) {
        for(String topic : topicSubscribersTable.getTopics()){
            // Modify for each topic the NextHop with the new Leader
            if (topicSubscribersTable.getNextHopOfInterestedBrokers(topic).contains(oldLeaderId)){
                topicSubscribersTable.modifyNextHopForBroker(topic, oldLeaderId, newLeaderId);
            }

            // Modify for each topic the interested Broker with the new Leader
            if (topicSubscribersTable.getInterestedBrokers(topic).contains(oldLeaderId)) {
                /*
                can't directly use handleBrokerSubscribe and handleBrokerUnsubscribe due to wrong and unnecessary steps,
                the concept is similar to these methods, but adapted for the current situation,
                there's no actual sender of a Subscribe/Unsubscribe Broker Message, so to use the already existing methods we have to forge it
                */

                // concept similar to handleBrokerSubscribe
                // forge the senderId as the newLeader
                boolean newBrokerSubscription = topicSubscribersTable.addBrokerSubscription(topic, newLeaderId, newLeaderId);
                if (newBrokerSubscription) forwardSubscribeMessage(topic, newLeaderId, newLeaderId);

                // concept similar to handleBrokerUnsubscribe
                // forge the senderId as the oldLeader
                boolean removedBrokerSubscription = topicSubscribersTable.removeBrokerSubscription(topic, oldLeaderId);
                if (removedBrokerSubscription) forwardUnsubscribeMessage(topic, oldLeaderId, oldLeaderId);
            }
        }
    }

    public void shareTopicSubscribers(BrokerHandler handler) {
        topicSubscribersTable.getTopics().forEach(topic -> {
            if (!topicSubscribersTable.getInterestedClients(topic).isEmpty()) {
                long brokerSeqNum = broker.getMessageCounter();
                long currentTime = System.currentTimeMillis();
                BrokerSubscribeMessage sm = new BrokerSubscribeMessage(BROKER_ID, brokerSeqNum, currentTime, BROKER_ID, topic);

                ackTracker.trackDelivery(sm, Collections.singleton(handler.getNodeId()));
                handler.send(sm);
            }

            topicSubscribersTable.getInterestedBrokers(topic).forEach(interestedBroker -> {
                long brokerSeqNum = broker.getMessageCounter();
                long currentTime = System.currentTimeMillis();
                BrokerSubscribeMessage sm = new BrokerSubscribeMessage(BROKER_ID, brokerSeqNum, currentTime, interestedBroker, topic);

                ackTracker.trackDelivery(sm, Collections.singleton(handler.getNodeId()));
                handler.send(sm);
            });
        });
    }

    public void applyToLocalState(Message message) {
        switch (message.getType()) {
            case SUBSCRIBE:
                SubscribeMessage subMsg = (SubscribeMessage) message;
                topicSubscribersTable.addClientSubscription(subMsg.getTopic(), subMsg.getSenderId(), subMsg.isDurable());
                break;
            case UNSUBSCRIBE:
                UnsubscribeMessage unsubMsg = (UnsubscribeMessage) message;
                topicSubscribersTable.removeClientSubscription(unsubMsg.getTopic(), unsubMsg.getSenderId());
                break;
            default:
                break;
        }
    }

    public void deliverOfflineMessages(String clientId) {
        try {
            List<PublishMessage> backlog = offlineStore.drain(clientId);
            if (backlog.isEmpty()) return;

            ConsoleAppender.asyncMessage(String.format("[OfflineStore] Delivering %d buffered messages to the offline client %s",
                    backlog.size(), clientId));

            ClientHandler handler = connectionRegistry.getClientHandler(clientId);
            if (handler == null) return;

            // For each message set the tracking and then send
            for (PublishMessage msg : backlog) {
                ackTracker.trackDelivery(msg, Collections.singleton(clientId));
                handler.send(msg);
            }

            // Not needed anymore because tracking have been just created
            // ackTracker.resetTimestampsForSubscriber(clientId);

        } catch (Exception e) {
            ConsoleAppender.asyncMessage(String.format("[OfflineStore] [Error] Error delivering offline messages to %s. Error: %s",
                    clientId, e.getMessage()));
        }
    }

    public void onClientDisconnection(String clientId) {
        for (String topic : topicSubscribersTable.getNonDurableSubscriptions(clientId)) {
            boolean removedTopicOfInterest = topicSubscribersTable.removeClientSubscription(topic, clientId);

            if (removedTopicOfInterest) {
                forwardUnsubscribeMessage(topic, BROKER_ID, null);
            }
        }
    }


}