package it.polimi.pubsub.messages.brokerMessages;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.polimi.pubsub.messages.Message;
import it.polimi.pubsub.messages.MessageType;
import it.polimi.pubsub.messages.PublishMessage;

public class BrokerPublishMessage extends Message {

    private final PublishMessage originalMessage;

    public BrokerPublishMessage(
            @JsonProperty("senderId") String senderId,
            @JsonProperty("sequenceNumber") long sequenceNumber,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("originalMessage") PublishMessage originalMessage) {

        super(senderId, sequenceNumber, timestamp);
        this.originalMessage = originalMessage;
    }

    @Override
    @JsonIgnore
    public MessageType getType() {
        return MessageType.BROKER_PUBLISH;
    }

    @JsonIgnore
    public String getOriginalMessageId() {
        return originalMessage.getMessageId();
    }

    @JsonIgnore
    public String getTopic() {
        return originalMessage.getTopic();
    }

    public PublishMessage getOriginalMessage() {
        return originalMessage;
    }
}