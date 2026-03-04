package it.polimi.pubsub.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SubscribeMessage extends Message {

    private final String topic;

    @JsonCreator
    public SubscribeMessage(
            @JsonProperty("senderId") String senderId,
            @JsonProperty("sequenceNumber") long sequenceNumber,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("topic") String topic) {

        super(senderId, sequenceNumber, timestamp);
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }

    @Override
    public MessageType getType() {
        return MessageType.SUBSCRIBE;
    }
}
