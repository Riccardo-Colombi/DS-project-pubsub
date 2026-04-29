package it.polimi.pubsub.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SubscribeMessage extends Message {

    private final String topic;

    /**
     * If durable=true the broker will buffer messages for this client while it is offline
     * and deliver them when it reconnects. The subscription itself also survives disconnections.
     *
     * If durable=false the broker behaves as following (I think as it was in the past):
     * no buffering, subscription removed on disconnect.
     */
    private final boolean durable;

    @JsonCreator
    public SubscribeMessage(
            @JsonProperty("senderId") String senderId,
            @JsonProperty("sequenceNumber") long sequenceNumber,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("topic") String topic,
            @JsonProperty("durable") boolean durable) {

        super(senderId, sequenceNumber, timestamp);
        this.topic   = topic;
        this.durable = durable;
    }

    /** Convenience constructor. Keeps existing callers working (durable = false by default) */
    public SubscribeMessage(String senderId, long sequenceNumber, long timestamp, String topic) {
        this(senderId, sequenceNumber, timestamp, topic, false);
    }

    public String getTopic() {
        return topic;
    }

    public boolean isDurable() {
        return durable;
    }

    @Override
    @JsonIgnore
    public MessageType getType() {
        return MessageType.SUBSCRIBE;
    }
}
