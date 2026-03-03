package it.polimi.pubsub.client;

public interface Publisher {
    public void publish(String topic, byte[] payload);
}
