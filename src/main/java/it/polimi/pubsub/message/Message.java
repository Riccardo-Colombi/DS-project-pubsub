package it.polimi.pubsub.message;

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
        @JsonSubTypes.Type(value = HeartbeatMessage.class, name = "HEARTBEAT"),
        @JsonSubTypes.Type(value = AckMessage.class, name = "ACK")
})
public interface Message {

    MessageType getType();
    String getMessageId();
    String getSenderId();

    long getTimestamp();
}
