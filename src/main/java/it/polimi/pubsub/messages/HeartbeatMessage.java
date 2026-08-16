package it.polimi.pubsub.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class HeartbeatMessage extends Message {

    private final int term;
    private final String leaderId;

    @JsonCreator
    public HeartbeatMessage(
            @JsonProperty("senderId") String senderId,
            @JsonProperty("sequenceNumber") long sequenceNumber,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("term") int term,
            @JsonProperty("leaderId") String leaderId) {

        super(senderId, sequenceNumber, timestamp);
        this.term = term;
        this.leaderId = leaderId;
    }

    public int getTerm() {
        return term;
    }

    public String getLeaderId() {
        return leaderId;
    }

    @Override
    @JsonIgnore
    public MessageType getType() {
        return MessageType.HEARTBEAT;
    }
}
