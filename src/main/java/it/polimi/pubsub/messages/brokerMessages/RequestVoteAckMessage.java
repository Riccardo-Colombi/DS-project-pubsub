package it.polimi.pubsub.messages.brokerMessages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.polimi.pubsub.messages.Message;
import it.polimi.pubsub.messages.MessageType;

public class RequestVoteAckMessage extends Message {

    private final int term;
    private final boolean voteGranted;

    @JsonCreator
    public RequestVoteAckMessage(
            @JsonProperty("senderId") String senderId,
            @JsonProperty("sequenceNumber") long sequenceNumber,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("term") int term,
            @JsonProperty("voteGranted") boolean voteGranted) {

        super(senderId, sequenceNumber, timestamp);
        this.term = term;
        this.voteGranted = voteGranted;
    }

    public int getTerm() {
        return term;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }

    @Override
    @JsonIgnore
    public MessageType getType() {
        return MessageType.REQUEST_VOTE_ACK;
    }

}