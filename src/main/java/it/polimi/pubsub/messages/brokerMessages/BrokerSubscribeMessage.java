package it.polimi.pubsub.messages.brokerMessages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.polimi.pubsub.messages.Message;
import it.polimi.pubsub.messages.MessageType;

public class BrokerSubscribeMessage extends Message {

    private final String interestedBrokerId;
    private final String topic;

    @JsonCreator
    public BrokerSubscribeMessage(
            @JsonProperty("senderId") String senderId,
            @JsonProperty("sequenceNumber") long sequenceNumber,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("interestedBrokerId") String interestedBrokerId,
            @JsonProperty("topic") String topic) {

        super(senderId, sequenceNumber, timestamp);
        this.interestedBrokerId = interestedBrokerId;
        this.topic = topic;
    }

    public String getInterestedBrokerId() {
        return interestedBrokerId;
    }

    public String getTopic() {
        return topic;
    }

    @Override
    @JsonIgnore
    public MessageType getType() {
        return MessageType.BROKER_SUBSCRIBE;
    }
}