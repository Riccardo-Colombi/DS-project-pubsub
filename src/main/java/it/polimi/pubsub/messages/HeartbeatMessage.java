package it.polimi.pubsub.messages;

import com.fasterxml.jackson.annotation.JsonProperty;

public class HeartbeatMessage extends Message {


    public HeartbeatMessage(
            @JsonProperty("senderId") String senderId,
            @JsonProperty("sequenceNumber") long sequenceNumber,
            @JsonProperty("timestamp") long timestamp) {

        super(senderId, sequenceNumber, timestamp);
    }

    @Override
    public MessageType getType() {
        return MessageType.HEARTBEAT;
    }
}
