package it.polimi.pubsub.message;

public class HeartbeatMessage implements Message {

    private final String messageId;
    private final String senderId;
    private final long timestamp;

    public HeartbeatMessage(String senderId, long sequenceNumber) {
        this.senderId = senderId;
        this.messageId = senderId + ":" + sequenceNumber;
        this.timestamp = System.currentTimeMillis();
    }

    @Override
    public MessageType getType() {
        return MessageType.HEARTBEAT;
    }

    @Override
    public String getMessageId() {
        return messageId;
    }

    @Override
    public String getSenderId() {
        return senderId;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }
}
