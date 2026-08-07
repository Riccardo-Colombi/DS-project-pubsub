package it.polimi.pubsub.messages.brokerMessages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.polimi.pubsub.messages.Message;
import it.polimi.pubsub.messages.MessageType;

public class AppendEntryAckMessage extends Message {

    private final boolean success;
    private final int acknowledgedLogIndex;

    @JsonCreator
    public AppendEntryAckMessage(
            @JsonProperty("senderId") String senderId,
            @JsonProperty("sequenceNumber") long sequenceNumber,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("success") boolean success,
            @JsonProperty("acknowledgedLogIndex") int acknowledgedLogIndex) {

        super(senderId, sequenceNumber, timestamp);
        this.success = success;
        this.acknowledgedLogIndex = acknowledgedLogIndex;
    }

    public boolean isSuccess() {
        return success;
    }

    public int getAcknowledgedLogIndex() {
        return acknowledgedLogIndex;
    }

    @Override
    @JsonIgnore
    public MessageType getType() {
        return MessageType.APPEND_ENTRY_ACK;
    }
}