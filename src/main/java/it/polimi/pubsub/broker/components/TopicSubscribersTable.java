package it.polimi.pubsub.broker.components;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Maps topics to subscriber IDs.
 *
 * After updated of client recover: distinguishes between durable and non-durable subscribers:
 *  - durable: subscription survives disconnection; messages are buffered while offline.
 *  - non-durable: subscription is removed when the client disconnects (as before the update).
 */
public class TopicSubscribersTable {

    // TODO: decide to leave this reminder or not.
    // Do not use HashMap for multiple network threads reading/writing things to the structure.
    // Risk of ConcurrentModificationException.
    // Use java.util.concurrent and in particular ConcurrentHashMap.newKeySet()

    // topic → set of all subscriber IDs (durable + non-durable)
    private final ConcurrentHashMap<String, Set<String>> topicSubscribers;

    // clientId → set of topics for which this client has a DURABLE subscription
    // Used to know: (a) which subscriptions to keep on disconnect,
    //               (b) for which clients to buffer messages.
    private final ConcurrentHashMap<String, Set<String>> durableSubscriptions;


    public TopicSubscribersTable() {
        this.topicSubscribers = new ConcurrentHashMap<>();
        this.durableSubscriptions = new ConcurrentHashMap<>();
    }

    // -------------------------------------------------------------------------
    // Write operations
    // -------------------------------------------------------------------------
    /** New addSubscription method. */
    public void addSubscription(String topic, String subscriberId, boolean durable) {
        topicSubscribers.computeIfAbsent(topic, k -> ConcurrentHashMap.newKeySet())
                .add(subscriberId);

        if (durable) {
            durableSubscriptions.computeIfAbsent(subscriberId, k -> ConcurrentHashMap.newKeySet())
                    .add(topic);
        }
    }

    /** Backward-compatible overload (non-durable). */
    public void addSubscription(String topic, String subscriberId) {
        addSubscription(topic, subscriberId, false);
    }

    /**
     * Fully remove a subscription (called on explicit UNSUBSCRIBE).
     * Removes both the durable record and the topic entry.
     */
    public void removeSubscription(String topic, String subscriberId) {
        Set<String> subscribers = topicSubscribers.get(topic);
        if (subscribers != null) {
            subscribers.remove(subscriberId);

            if (subscribers.isEmpty()) {
                topicSubscribers.remove(topic, subscribers);
            }
        }

        Set<String> durableTopics = durableSubscriptions.get(subscriberId);
        if (durableTopics != null) {
            durableTopics.remove(topic);

            if (durableTopics.isEmpty()) {
                durableSubscriptions.remove(subscriberId);
            }
        }
    }

    /**
     * Remove only NON-DURABLE subscriptions for a client that has disconnected.
     * Durable subscriptions are left intact.
     */
    public void removeNonDurableSubscriptions(String subscriberId) {
        Set<String> durableTopics = durableSubscriptions.getOrDefault(
                subscriberId, Collections.emptySet());

        for (String topic : topicSubscribers.keySet()) {
            if (durableTopics.contains(topic)) continue; // keep durable.

            Set<String> subscribers = topicSubscribers.get(topic);
            if (subscribers != null) {
                subscribers.remove(subscriberId);
                if (subscribers.isEmpty()) {
                    topicSubscribers.remove(topic, subscribers);
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Query operations
    // -------------------------------------------------------------------------
    public Set<String> getSubscribers(String topic) {
        Set<String> subscribers = topicSubscribers.get(topic);

        // If there are no subscribers to a topic we can return a null, causing a NullPointerException risk
        if (subscribers == null) {
            return Collections.emptySet();
        }

        // Collections.unmodifiableSet() to not modify the set. Otherwise we are passing the actual set
        return Collections.unmodifiableSet(subscribers);
    }

    public Set<String> getTopics() {
        return topicSubscribers.keySet();
    }

    public boolean isDurableSubscriber(String clientId, String topic) {
        Set<String> durableTopics = durableSubscriptions.get(clientId);
        return durableTopics != null && durableTopics.contains(topic);
    }

    /** All topics for which clientId has a durable subscription. */
    public Set<String> getDurableTopics(String clientId) {
        Set<String> topics = durableSubscriptions.get(clientId);
        if (topics == null) return Collections.emptySet();
        return Collections.unmodifiableSet(topics);
    }
}
