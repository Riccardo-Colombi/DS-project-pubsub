package it.polimi.pubsub.messages.brokerMessages;

import com.fasterxml.jackson.annotation.JsonIgnore;
import it.polimi.pubsub.messages.Message;
import it.polimi.pubsub.messages.MessageType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class InstallSnapshotMessage extends Message {

    private final int leaderTerm;
    private final int lastIncludedIndex;
    private final int lastIncludedTerm;
    private final List<Message> stateMachineState;

    @JsonCreator
    public InstallSnapshotMessage(
            @JsonProperty("senderId") String senderId,
            @JsonProperty("sequenceNumber") long sequenceNumber,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("leaderTerm") int leaderTerm,
            @JsonProperty("lastIncludedIndex") int lastIncludedIndex,
            @JsonProperty("lastIncludedTerm") int lastIncludedTerm,
            @JsonProperty("stateMachineState") List<Message> stateMachineState) {

        super(senderId, sequenceNumber, timestamp);
        this.leaderTerm = leaderTerm;
        this.lastIncludedIndex = lastIncludedIndex;
        this.lastIncludedTerm = lastIncludedTerm;
        this.stateMachineState = stateMachineState;
    }

    public int getLeaderTerm() {
        return leaderTerm;
    }

    public int getLastIncludedIndex() {
        return lastIncludedIndex;
    }

    public int getLastIncludedTerm() {
        return lastIncludedTerm;
    }

    public List<Message> getStateMachineState() {
        return stateMachineState;
    }


    @Override
    @JsonIgnore
    public MessageType getType() {
        return MessageType.INSTALL_SNAPSHOT;
    }
}
