package it.polimi.pubsub.messages.brokerMessages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.polimi.pubsub.messages.Message;
import it.polimi.pubsub.messages.MessageType;

public class AppendEntryMessage extends Message {

    private final Message message;

    @JsonCreator
    public AppendEntryMessage(
            @JsonProperty("senderId") String senderId,
            @JsonProperty("sequenceNumber") long sequenceNumber,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("entry") Message message) {
        super(senderId, sequenceNumber, timestamp);

        // Message to replicate to followers

        this.message = message;
    }

    public Message getEntry() {
        return message;
    }

    @Override
    @JsonIgnore
    public MessageType getType() {
        return MessageType.APPEND_ENTRY;
    }
}
