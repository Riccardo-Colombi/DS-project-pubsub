package it.polimi.pubsub.broker.components;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TopicSubscribersTable {

    // Do not use HashMap for multiple network threads reading/writing things to the structure. Risk of ConcurrentModificationException.
    // Use java.util.concurrent and in particular ConcurrentHashMap.newKeySet()
    private final ConcurrentHashMap<String, Set<String>> topicSubscribers;

    public TopicSubscribersTable() {
        this.topicSubscribers = new ConcurrentHashMap<>();
    }

    public void addSubscription(String topic, String subscriberId) {
        topicSubscribers.computeIfAbsent(topic, k -> ConcurrentHashMap.newKeySet())
                .add(subscriberId);
    }

    public void removeSubscription(String topic, String subscriberId) {
        Set<String> subscribers = topicSubscribers.get(topic);
        if (subscribers != null) {
            subscribers.remove(subscriberId);

            if (subscribers.isEmpty()) {
                topicSubscribers.remove(topic, subscribers);
            }
        }
    }

    public Set<String> getSubscribers(String topic) {
        Set<String> subscribers = topicSubscribers.get(topic);

        // If there are no subscribers to a topic we can return a null, causing a NullPointerException risk
        if (subscribers == null) {
            return Collections.emptySet();
        }
        // Collections.unmodifiableSet() to not modify the set. Otherwise we are passing the actual set
        return Collections.unmodifiableSet(subscribers);
    }
}
