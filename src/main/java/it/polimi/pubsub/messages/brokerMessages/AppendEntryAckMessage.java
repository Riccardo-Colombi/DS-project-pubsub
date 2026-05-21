package it.polimi.pubsub.messages.brokerMessages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.polimi.pubsub.messages.Message;
import it.polimi.pubsub.messages.MessageType;

public class AppendEntryAckMessage extends Message {

    private final String acknowledgeEntryMessageId;

    @JsonCreator
    public AppendEntryAckMessage(
            @JsonProperty("senderId") String senderId,
            @JsonProperty("sequenceNumber") long sequenceNumber,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("acknowledgedEntryId") String acknowledgeEntryMessageId) {
        super(senderId, sequenceNumber, timestamp);
        this.acknowledgeEntryMessageId = acknowledgeEntryMessageId;
    }

    public String getAcknowledgedEntryId() {
        return acknowledgeEntryMessageId;
    }

    @Override
    @JsonIgnore
    public MessageType getType() {
        return MessageType.APPEND_ENTRY_ACK;
    }
}
