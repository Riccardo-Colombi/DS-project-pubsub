package it.polimi.pubsub.broker.state;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

// Broker problem: since multiple network threads are reading/writing things to the structure in the broker simultaneously, thread safety is a concern.
// I've read that it's better to not use a HashMap (problem of ConcurrentModificationException) but use java.util.concurrent
// Also read that the normal Set inside it's not that great for thread safety. Use the function ConcurrentHashMap.newKeySet()

public class RoutingTable {

    private final ConcurrentHashMap<String, Set<String>> topicSubscribers;

    public RoutingTable() {
        this.topicSubscribers = new ConcurrentHashMap<>();
    }

    public void addSubscription(String topic, String subscriberId) {
        topicSubscribers.computeIfAbsent(topic, k -> ConcurrentHashMap.newKeySet())
                .add(subscriberId);
    }

    public void removeSubscription(String topic, String subscriberId) {
        // TODO
    }

    public Set<String> getSubscribers(String topic) {
        // TODO
        return Set.of();
    }
}
