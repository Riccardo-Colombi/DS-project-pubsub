package it.polimi.pubsub.client;

public interface Subscriber {
    public void subscribe(String topic);
    public void unsubscribe(String topic);
}
