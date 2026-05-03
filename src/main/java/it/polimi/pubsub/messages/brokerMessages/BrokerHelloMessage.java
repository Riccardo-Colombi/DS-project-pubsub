package it.polimi.pubsub.messages.brokerMessages;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.polimi.pubsub.messages.Message;
import it.polimi.pubsub.messages.MessageType;

public class BrokerHelloMessage extends Message {
    public BrokerHelloMessage(
            @JsonProperty("senderId") String senderId,
            @JsonProperty("sequenceNumber") long sequenceNumber,
            @JsonProperty("timestamp") long timestamp) {

        super(senderId, sequenceNumber, timestamp);
    }

    @Override
    @JsonIgnore
    public MessageType getType() {
        return MessageType.BROKER_HELLO;
    }
}