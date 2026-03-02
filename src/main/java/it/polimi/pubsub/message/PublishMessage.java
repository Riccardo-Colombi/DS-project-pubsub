package it.polimi.pubsub.message;

public class PublishMessage implements Message {

    private final String messageId;
    private final String senderId;
    private final long timestamp;
    private final String topic;
    private final byte[] payload;

    public PublishMessage(String senderId, long sequenceNumber, String topic, byte[] payload) {
        this.senderId = senderId;
        this.messageId = senderId + ":" + sequenceNumber;
        this.timestamp = System.currentTimeMillis();
        this.topic = topic;
        this.payload = payload;
    }

    @Override
    public MessageType getType() {
        return MessageType.PUBLISH;
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

    public String getTopic() {
        return topic;
    }

    public byte[] getPayload() {
        return payload;
    }
}
