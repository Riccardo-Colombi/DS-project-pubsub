package it.polimi.pubsub.broker.components;

import it.polimi.pubsub.messages.Message;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class PendingMessage {

    private final Message message;
    private final long timestamp;
    private final Set<String> pendingSubscribers;

    public PendingMessage(Message message, Set<String> targetSubscribers) {
        this.message = message;
        this.timestamp = System.currentTimeMillis();
        this.pendingSubscribers = ConcurrentHashMap.newKeySet();
        this.pendingSubscribers.addAll(targetSubscribers);
    }

    public void acknowledge(String subscriberId) {
        pendingSubscribers.remove(subscriberId);
    }

    public boolean isFullyAcknowledged() {
        return pendingSubscribers.isEmpty();
    }

    public Set<String> getPendingSubscribers() {
        return pendingSubscribers;
    }

    public Message getMessage() {
        return message;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
