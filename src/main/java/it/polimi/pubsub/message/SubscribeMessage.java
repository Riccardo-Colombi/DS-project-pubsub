package it.polimi.pubsub.message;

public class SubscribeMessage implements Message {

    private final String messageId;
    private final String senderId;
    private final long timestamp;
    private final String topic;

    public SubscribeMessage(String senderId, long sequenceNumber, String topic) {
        this.senderId = senderId;
        this.messageId = senderId + ":" + sequenceNumber;
        this.timestamp = System.currentTimeMillis();
        this.topic = topic;
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

    public String getTopic() {
        return topic;
    }

}
