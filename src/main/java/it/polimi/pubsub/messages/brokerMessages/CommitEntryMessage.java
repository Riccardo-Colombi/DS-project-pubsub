package it.polimi.pubsub.messages.brokerMessages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.polimi.pubsub.messages.Message;
import it.polimi.pubsub.messages.MessageType;

public class CommitEntryMessage extends Message {

    private final Message message;

    private final int logIndex;

    @JsonCreator
    public CommitEntryMessage(
            @JsonProperty("senderId") String senderId,
            @JsonProperty("sequenceNumber") long sequenceNumber,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("message") Message message,
            @JsonProperty("logIndex") int logIndex) {

        super(senderId, sequenceNumber, timestamp);

        this.message = message;
        this.logIndex = logIndex;
    }

    public Message getMessage() {
        return message;
    }

    public int getLogIndex() {
        return logIndex;
    }

    @Override
    @JsonIgnore
    public MessageType getType() {
        return MessageType.COMMIT_ENTRY;
    }
}
