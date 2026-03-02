package it.polimi.pubsub.message;

public class AckMessage implements Message {

    private final String messageId;
    private final String senderId;
    private final long timestamp;

    public AckMessage(String senderId, long sequenceNumber) {
        this.senderId = senderId;
        this.messageId = senderId + ":" + sequenceNumber;
        this.timestamp = System.currentTimeMillis();
    }

    @Override
    public MessageType getType() {
        return MessageType.SUBSCRIBE;
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
