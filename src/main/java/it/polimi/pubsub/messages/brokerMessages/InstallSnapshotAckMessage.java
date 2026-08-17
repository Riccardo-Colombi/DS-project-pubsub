package it.polimi.pubsub.messages.brokerMessages;

import com.fasterxml.jackson.annotation.JsonIgnore;
import it.polimi.pubsub.messages.Message;
import it.polimi.pubsub.messages.MessageType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class InstallSnapshotAckMessage extends Message {

    private final int term;
    private final int lastIncludedIndex;

    @JsonCreator
    public InstallSnapshotAckMessage(
            @JsonProperty("senderId") String senderId,
            @JsonProperty("sequenceNumber") long sequenceNumber,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("term") int term,
            @JsonProperty("lastIncludedIndex") int lastIncludedIndex) {

        super(senderId, sequenceNumber, timestamp);
        this.term = term;
        this.lastIncludedIndex = lastIncludedIndex;
    }

    public int getTerm() {
        return term;
    }

    public int getLastIncludedIndex() {
        return lastIncludedIndex;
    }

    @Override
    @JsonIgnore
    public MessageType getType() {
        return MessageType.INSTALL_SNAPSHOT_ACK;
    }
}