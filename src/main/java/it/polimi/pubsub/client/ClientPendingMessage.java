package it.polimi.pubsub.client;

import it.polimi.pubsub.messages.Message;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ClientPendingMessage {

    private final Message message;
    private volatile long trackingTimestamp;
    private final String broker;

    public ClientPendingMessage(Message message, String broker) {
        this.message = message;
        this.trackingTimestamp = System.currentTimeMillis();
        this.broker = broker;
    }

    public void resetTimestamp() {
        trackingTimestamp = System.currentTimeMillis();
    }

    public String getBroker() {
        return broker;
    }

    public Message getMessage() {
        return message;
    }

    public long getTrackingTimestamp() {
        return trackingTimestamp;
    }
}
