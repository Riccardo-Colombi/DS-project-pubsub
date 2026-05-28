package it.polimi.pubsub.messages.brokerMessages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.polimi.pubsub.messages.Message;
import it.polimi.pubsub.messages.MessageType;

public class RequestVoteMessage extends Message {

    private final int term;
    private final int lastLogIndex;
    private final int lastLogTerm;

    @JsonCreator
    public RequestVoteMessage(
            @JsonProperty("senderId") String senderId,
            @JsonProperty("sequenceNumber") long sequenceNumber,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("term") int term,
            @JsonProperty("lastLogIndex") int lastLogIndex,
            @JsonProperty("lastLogTerm") int lastLogTerm) {

        super(senderId, sequenceNumber, timestamp);
        this.term = term;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm  = lastLogTerm;
    }

    public int getTerm() {
        return term;
    }

    public int getLastLogIndex() {
        return lastLogIndex;
    }

    public int getLastLogTerm() {
        return lastLogTerm;
    }

    @Override
    @JsonIgnore
    public MessageType getType() {
        return MessageType.REQUEST_VOTE;
    }
}