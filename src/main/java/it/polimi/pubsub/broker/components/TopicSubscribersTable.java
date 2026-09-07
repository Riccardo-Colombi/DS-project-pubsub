package it.polimi.pubsub.broker.components;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Maps topics to subscriber IDs, now with durable-subscription tracking.
 *
 * A durable subscription survives client disconnections: when the client
 * reconnects, it still receives messages for that topic and any messages
 * buffered while it was offline.
 *
 * A non-durable subscription is removed as soon as the client disconnects.
 */
public class TopicSubscribersTable {

    // Do not use HashMap for multiple network threads reading/writing things to the structure. Risk of ConcurrentModificationException.
    // Use java.util.concurrent and in particular ConcurrentHashMap.newKeySet()
    private final ConcurrentHashMap<String, TopicSubscriptions> topicSubscribers;

    // clientId → set of topics for which this client has a DURABLE subscription.
    // Kept separately so we can answer "is this subscription durable?" in O(1)
    // and so we know which subscriptions to preserve on disconnection.
    private final ConcurrentHashMap<String, Set<String>> durableSubscriptions;

    public TopicSubscribersTable() {
        this.topicSubscribers = new ConcurrentHashMap<>();
        this.durableSubscriptions = new ConcurrentHashMap<>();
    }

    // ------------------------------------------------------------------
    // Client subscriptions
    // ------------------------------------------------------------------

    /**
     * Add a client subscription.
     */
    public boolean addClientSubscription(String topic, String subscriberId, boolean durable) {
        AtomicBoolean firstClient = new AtomicBoolean(false);

        topicSubscribers.compute(topic, (t, subscribers) -> {
            if (subscribers == null)
                subscribers = new TopicSubscriptions();

            // First local client regardless of broker entries already present
            if (subscribers.getClients().isEmpty())
                firstClient.set(true);

            subscribers.addClient(subscriberId);
            return subscribers;
        });

        if (durable) {
            durableSubscriptions
                    .computeIfAbsent(subscriberId, k -> ConcurrentHashMap.newKeySet())
                    .add(topic);
        }

        return firstClient.get();
    }

    /** Backward-compatible overload — non-durable by default. */
    public boolean addClientSubscription(String topic, String subscriberId) {
        return addClientSubscription(topic, subscriberId, false);
    }

    /**
     * Fully remove a subscription (called on explicit UNSUBSCRIBE).
     * Removes both the durable record and the topic entry.
     * @return true if this was the last local client for the topic.
     */
    public boolean removeClientSubscription(String topic, String subscriberId) {
        AtomicBoolean lastClient = new AtomicBoolean(false);

        topicSubscribers.computeIfPresent(topic, (t, subscribers) -> {
            subscribers.removeClient(subscriberId);

            if (subscribers.getClients().isEmpty())
                lastClient.set(true); // no more local clients, even if brokers remain

            if (subscribers.isEmpty())
                return null; // topic fully empty, remove the entry

            return subscribers;
        });

        // Remove from durable index regardless of whether it was durable
        Set<String> durableTopics = durableSubscriptions.get(subscriberId);
        if (durableTopics != null) {
            durableTopics.remove(topic);
            if (durableTopics.isEmpty())
                durableSubscriptions.remove(subscriberId);
        }

        return lastClient.get();
    }

    /**
     * Called on client TCP disconnection.
     * Removes ONLY non-durable subscriptions; durable ones are preserved so that
     * the broker keeps routing awareness and can buffer offline messages.
     */
    public void removeNonDurableSubscriptions(String subscriberId) {
        Set<String> durableTopics = durableSubscriptions.getOrDefault(
                subscriberId, Collections.emptySet());

        for (String topic : topicSubscribers.keySet()) {
            if (durableTopics.contains(topic)) continue; // keep durable

            topicSubscribers.computeIfPresent(topic, (t, subscribers) -> {
                subscribers.removeClient(subscriberId);
                return subscribers.isEmpty() ? null : subscribers;
            });
        }
    }

    // ------------------------------------------------------------------
    // Broker subscriptions
    // ------------------------------------------------------------------

    // return true if the interestedBroker wasn't already subscribed to the topic
    public boolean addBrokerSubscription(String topic, String interestedBrokerId, String nextHopId) {
        AtomicBoolean isNew = new AtomicBoolean(false);

        topicSubscribers.compute(topic, (t, subscribers) -> {
            if (subscribers == null)
                subscribers = new TopicSubscriptions();

            // putIfAbsent returns null if the key was not already present
            if (subscribers.addBroker(interestedBrokerId, nextHopId))
                isNew.set(true);

            return subscribers;
        });

        return isNew.get();
    }

    // return true if the interestedBroker wasn't already removed
    public boolean removeBrokerSubscription(String topic, String interestedBrokerId) {
        AtomicBoolean wasPresent = new AtomicBoolean(false);

        topicSubscribers.computeIfPresent(topic, (t, subscribers) -> {
            if (subscribers.removeBroker(interestedBrokerId))
                wasPresent.set(true);

            if (subscribers.isEmpty())
                return null; // atomically remove empty topic entry

            return subscribers;
        });

        return wasPresent.get();
    }

    public void modifyNextHopForBroker(String topic, String oldBrokerId, String newBrokerId) {
        topicSubscribers.computeIfPresent(topic, (t, subscribers) -> {
            // Iterate over a copy of the map to prevent ConcurrentModificationException
            Map<String, String> brokers = subscribers.getBrokers();
            for (Map.Entry<String, String> entry : brokers.entrySet()) {
                if (entry.getValue().equals(oldBrokerId)) {
                    // Since TopicSubscriptions.addBroker uses putIfAbsent, we must remove first
                    subscribers.removeBroker(entry.getKey());
                    subscribers.addBroker(entry.getKey(), newBrokerId);
                }
            }
            return subscribers;
        });
    }

    // ------------------------------------------------------------------
    // Query methods
    // ------------------------------------------------------------------

    public Set<String> getTopics() {
        return topicSubscribers.keySet();
    }

    public Set<String> getInterestedClients(String topic) {
        TopicSubscriptions subscribers = topicSubscribers.get(topic);

        if (subscribers != null)
            return subscribers.getClients();

        return Collections.emptySet();
    }

    public Set<String> getInterestedBrokers(String topic) {
        TopicSubscriptions subscribers = topicSubscribers.get(topic);

        if (subscribers != null)
            return subscribers.getInterestedBrokers();

        return Collections.emptySet();
    }

    public Set<String> getNextHopOfInterestedBrokers(String topic) {
        TopicSubscriptions subscribers = topicSubscribers.get(topic);

        if (subscribers != null)
            return subscribers.getInterestedBrokersNextHop();

        return Collections.emptySet();
    }

    public String getNextHopOfInterestedBroker(String brokerId, String topic) {
        String nextHop = null;
        TopicSubscriptions subscribers = topicSubscribers.get(topic);

        if (subscribers != null)
            nextHop = subscribers.getBrokers().get(brokerId);

        return nextHop;
    }

    public Map<String, String> getInterestedBrokersInfo(String topic) {
        TopicSubscriptions subscribers = topicSubscribers.get(topic);

        if (subscribers != null)
            return subscribers.getBrokers();

        return Collections.emptyMap();
    }

    public boolean isDurableSubscriber(String clientId, String topic) {
        Set<String> topics = durableSubscriptions.get(clientId);
        return topics != null && topics.contains(topic);
    }

    public Set<String> getNonDurableSubscriptions(String subscriberId) {
        Set<String> durableTopics = durableSubscriptions.getOrDefault(subscriberId, Collections.emptySet());
        Set<String> nonDurableTopics = new java.util.HashSet<>();

        // Search for a topic in which the client is present, but it's not durable
        for (Map.Entry<String, TopicSubscriptions> entry : topicSubscribers.entrySet()) {
            if (entry.getValue().getClients().contains(subscriberId) && !durableTopics.contains(entry.getKey())) {
                nonDurableTopics.add(entry.getKey());
            }
        }
        return nonDurableTopics;
    }

    // ------------------------------------------------------------------
    // Inner class
    // ------------------------------------------------------------------

    private class TopicSubscriptions {
        private final Set<String> clients = ConcurrentHashMap.newKeySet();
        //private final Set<String> brokers = ConcurrentHashMap.newKeySet();
        // Map Interested brokers with corresponding next hop to reach them
        private final ConcurrentHashMap<String, String> brokers = new ConcurrentHashMap<>();

        public void addClient(String id) {
            clients.add(id);
        }

        public void removeClient(String id) {
            clients.remove(id);
        }

        // returns true if this origin was not already tracked
        public boolean addBroker(String interestedBrokerId, String nextHopId) {
            return brokers.putIfAbsent(interestedBrokerId, nextHopId) == null;
        }

        // return true if something gets actually deleted
        public boolean removeBroker(String interestedBrokerId) {
            return brokers.remove(interestedBrokerId) != null;
        }

        // Topic is globally gone when both sets are empty
        public boolean isEmpty() { return clients.isEmpty() && brokers.isEmpty(); }

        Set<String> getClients() {
            return Collections.unmodifiableSet(clients);
        }

        Map<String, String> getBrokers() {
            return Collections.unmodifiableMap(new HashMap<>(brokers));
        }

        Set<String> getInterestedBrokers() {
            return Collections.unmodifiableSet(brokers.keySet());
        }

        Set<String> getInterestedBrokersNextHop() {
            return brokers.values().stream().collect(Collectors.toUnmodifiableSet());
        }
    }

    // ------------------------------------------------------------------
    // Extras
    // ------------------------------------------------------------------

    public void clear() {
        topicSubscribers.clear();
        durableSubscriptions.clear();
    }
}
