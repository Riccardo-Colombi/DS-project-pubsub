package it.polimi.pubsub.broker.state;

import it.polimi.pubsub.message.Message;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

// This is used to guarantee a "at-least-once delivery guarantee". When the broker forwards a PublishMessage to some subscribers it needs an
// AckMessage from each one of them. As done before, map a message to a tracking object.

public class AckTracker {

    private final ConcurrentHashMap<String, PendingMessage> unackedMessages;

    // TODO

    public void trackDelivery(Message message, Set<String> targetSubscribers) {
        // Create PendingMessage and put it in the map
    }

    public void handleAck(String messageId, String subscriberId) {
        // TODO
    }

    public List<Message> getExpiredMessages(long timeoutMillis) {
        // TODO
    }
}
