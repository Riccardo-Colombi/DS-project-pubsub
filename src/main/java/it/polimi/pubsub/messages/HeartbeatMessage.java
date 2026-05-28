package it.polimi.pubsub.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class HeartbeatMessage extends Message {

    private final int term;

    @JsonCreator
    public HeartbeatMessage(
            @JsonProperty("senderId") String senderId,
            @JsonProperty("sequenceNumber") long sequenceNumber,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("term") int term) {

        super(senderId, sequenceNumber, timestamp);
        this.term = term;
    }

    public int getTerm() {
        return term;
    }

    @Override
    @JsonIgnore
    public MessageType getType() {
        return MessageType.HEARTBEAT;
    }
}
