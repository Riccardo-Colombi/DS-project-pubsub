package it.polimi.pubsub.messages;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AckMessage extends Message {

    public AckMessage(
            @JsonProperty("senderId") String senderId,
            @JsonProperty("sequenceNumber") long sequenceNumber,
            @JsonProperty("timestamp") long timestamp) {

        super(senderId, sequenceNumber, timestamp);
    }

    @Override
    public MessageType getType() {
        return MessageType.SUBSCRIBE;
    }
}
