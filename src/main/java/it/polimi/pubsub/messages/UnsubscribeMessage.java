package it.polimi.pubsub.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class UnsubscribeMessage extends Message {

    private final String topic;

    @JsonCreator
    public UnsubscribeMessage(
            @JsonProperty("senderId") String senderId,
            @JsonProperty("sequenceNumber") long sequenceNumber,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("topic") String topic) {

        super(senderId, sequenceNumber, timestamp);
        this.topic = topic;
    }

    public String getTopic() { return topic; }

    @Override
    @JsonIgnore
    public MessageType getType() {
        return MessageType.UNSUBSCRIBE;
    }
}
