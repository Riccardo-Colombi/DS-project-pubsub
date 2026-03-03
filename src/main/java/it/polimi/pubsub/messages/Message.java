package it.polimi.pubsub.messages;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type"
)

@JsonSubTypes({
        @JsonSubTypes.Type(value = PublishMessage.class, name = "PUBLISH"),
        @JsonSubTypes.Type(value = SubscribeMessage.class, name = "SUBSCRIBE"),
        @JsonSubTypes.Type(value = UnsubscribeMessage.class, name = "UNSUBSCRIBE"),
        @JsonSubTypes.Type(value = HeartbeatMessage.class, name = "HEARTBEAT"),
        @JsonSubTypes.Type(value = AckMessage.class, name = "ACK")
})

public abstract class Message {

    private final String senderId;
    private final long sequenceNumber;
    private final long timestamp;

    protected Message(String senderId, long sequenceNumber, long timestamp) {
        this.sequenceNumber = sequenceNumber;
        this.senderId = senderId;
        this.timestamp = timestamp;
    }

    public abstract MessageType getType();

    public String getSenderId() {
        return senderId;
    }

    public long getSequenceNumber() { return sequenceNumber; }

    public long getTimestamp() {
        return timestamp;
    }

    @JsonIgnore
    public String getMessageId() {
        return senderId + ":" + sequenceNumber;
    }
}
