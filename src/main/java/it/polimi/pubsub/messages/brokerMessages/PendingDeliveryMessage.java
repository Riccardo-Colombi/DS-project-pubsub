package it.polimi.pubsub.messages.brokerMessages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.polimi.pubsub.messages.Message;
import it.polimi.pubsub.messages.MessageType;

import java.util.Set;

/**
 * Wrapper used only inside snapshot/WAL
 *
 * Represent a message that, at the moment of the snapshot, was still in
 * wait of an ACK from a group. Is is never sent through the "network":
 * it is only used from recoverState() that can reconstruct
 * the AckTracker with the exact group that needed to do the ack.
 */
public class PendingDeliveryMessage extends Message {

    private final Message originalMessage;
    private final Set<String> pendingSubscribers;

    @JsonCreator
    public PendingDeliveryMessage(
            @JsonProperty("senderId") String senderId,
            @JsonProperty("sequenceNumber") long sequenceNumber,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("originalMessage") Message originalMessage,
            @JsonProperty("pendingSubscribers") Set<String> pendingSubscribers) {

        super(senderId, sequenceNumber, timestamp);
        this.originalMessage = originalMessage;
        this.pendingSubscribers = pendingSubscribers;
    }

    public Message getOriginalMessage() {
        return originalMessage;
    }

    public Set<String> getPendingSubscribers() {
        return pendingSubscribers;
    }

    @Override
    @JsonIgnore
    public MessageType getType() {
        return MessageType.SNAPSHOT_PENDING_DELIVERY;
    }
}
