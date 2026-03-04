package it.polimi.pubsub.messages;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * - Why do I added acknowledgedMessageId?
 *      If Publisher A sends two messages (sequenceNumber 1 and 2), and the Broker sends back an
 *      AckMessage that only contains senderId="Broker" and sequenceNumber=1,
 *      how does Publisher A know which of its two messages was just acknowledged?
 */

public class AckMessage extends Message {

    // This is the payload: the ID of the message we are confirming
    private final String acknowledgedMessageId;

    public AckMessage(
            @JsonProperty("senderId") String senderId,
            @JsonProperty("sequenceNumber") long sequenceNumber,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("acknowledgedMessageId") String acknowledgedMessageId) {

        super(senderId, sequenceNumber, timestamp);
        this.acknowledgedMessageId = acknowledgedMessageId;
    }

    @Override
    public MessageType getType() {
        return MessageType.SUBSCRIBE;
    }

    public String getAcknowledgedMessageId() {
        return acknowledgedMessageId;
    }
}
