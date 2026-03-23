package it.polimi.pubsub.messages;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class PublishMessage extends Message {

    private final String topic;
    private final byte[] payload;

    public PublishMessage(
            @JsonProperty("senderId") String senderId,
            @JsonProperty("sequenceNumber") long sequenceNumber,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("topic") String topic,
            @JsonProperty("payload") byte[] payload) {

        super(senderId, sequenceNumber, timestamp);
        this.topic = topic;
        this.payload = payload;
    }

    @Override
    @JsonIgnore
    public MessageType getType() {
        return MessageType.PUBLISH;
    }

    public String getTopic() {
        return topic;
    }

    public byte[] getPayload() {
        return payload;
    }
}
