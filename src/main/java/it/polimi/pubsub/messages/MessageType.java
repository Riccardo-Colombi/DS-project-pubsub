package it.polimi.pubsub.messages;

public enum MessageType {
    PUBLISH,
    BROKER_PUBLISH,
    SUBSCRIBE,
    BROKER_SUBSCRIBE,
    UNSUBSCRIBE,
    BROKER_UNSUBSCRIBE,
    BROKER_HELLO,
    ACK,
    HEARTBEAT
}
