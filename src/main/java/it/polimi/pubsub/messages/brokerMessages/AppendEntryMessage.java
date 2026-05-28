package it.polimi.pubsub.messages.brokerMessages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.polimi.pubsub.messages.Message;
import it.polimi.pubsub.messages.MessageType;

public class AppendEntryMessage extends Message {

    private final Message message;
    private final int prevLogIndex;
    private final int prevLogTerm;
    private final int leaderTerm;
    private final int leaderCommit;

    @JsonCreator
    public AppendEntryMessage(
            @JsonProperty("senderId") String senderId,
            @JsonProperty("sequenceNumber") long sequenceNumber,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("message") Message message,
            @JsonProperty("prevLogIndex") int prevLogIndex,
            @JsonProperty("prevLogTerm") int prevLogTerm,
            @JsonProperty("leaderTerm") int leaderTerm,
            @JsonProperty("leaderCommit") int leaderCommit) {

        super(senderId, sequenceNumber, timestamp);

        // Message to replicate to followers
        this.message = message;

        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.leaderTerm = leaderTerm;
        this.leaderCommit = leaderCommit;
    }

    public Message getMessage() {
        return message;
    }

    public int getPrevLogIndex() {
        return prevLogIndex;
    }

    public int getPrevLogTerm() {
        return prevLogTerm;
    }

    public int getLeaderTerm() {
        return leaderTerm;
    }

    public int getLeaderCommit() {
        return leaderCommit;
    }

    @Override
    @JsonIgnore
    public MessageType getType() {
        return MessageType.APPEND_ENTRY;
    }
}
