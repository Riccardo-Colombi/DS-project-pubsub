package it.polimi.pubsub.broker.components;

import it.polimi.pubsub.messages.Message;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class PendingMessage {

    private final Message message;
    private volatile long trackingTimestamp;
    private final Set<String> pendingSubscribers;

    public PendingMessage(Message message, Set<String> targetSubscribers) {
        this.message = message;
        this.trackingTimestamp = System.currentTimeMillis();
        this.pendingSubscribers = ConcurrentHashMap.newKeySet();
        this.pendingSubscribers.addAll(targetSubscribers);
    }

    public void acknowledge(String subscriberId) {
        pendingSubscribers.remove(subscriberId);
    }

    public boolean isFullyAcknowledged() {
        return pendingSubscribers.isEmpty();
    }

    public void resetTimestamp() {
        trackingTimestamp = System.currentTimeMillis();
    }

    public Set<String> getPendingSubscribers() {
        return pendingSubscribers;
    }

    public Message getMessage() {
        return message;
    }

    public long getTrackingTimestamp() {
        return trackingTimestamp;
    }
}
