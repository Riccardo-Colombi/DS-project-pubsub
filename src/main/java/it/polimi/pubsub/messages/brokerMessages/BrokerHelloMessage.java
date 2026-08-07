package it.polimi.pubsub.messages.brokerMessages;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.polimi.pubsub.messages.Message;
import it.polimi.pubsub.messages.MessageType;

public class BrokerHelloMessage extends Message {

    private final int brokerPort;

    public BrokerHelloMessage(
            @JsonProperty("senderId") String senderId,
            @JsonProperty("sequenceNumber") long sequenceNumber,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("brokerPort") int brokerPort) {

        super(senderId, sequenceNumber, timestamp);
        this.brokerPort = brokerPort;
    }

    @JsonProperty("brokerPort")
    public int getBrokerPort() { return brokerPort; }

    @Override
    @JsonIgnore
    public MessageType getType() {
        return MessageType.BROKER_HELLO;
    }


}