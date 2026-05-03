package it.polimi.pubsub.broker.components;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class TopicSubscribersTable {

    // Do not use HashMap for multiple network threads reading/writing things to the structure. Risk of ConcurrentModificationException.
    // Use java.util.concurrent and in particular ConcurrentHashMap.newKeySet()
    private final ConcurrentHashMap<String, TopicSubscriptions> topicSubscribers;

    public TopicSubscribersTable() {
        this.topicSubscribers = new ConcurrentHashMap<>();
    }

    // return true if the client is the first to subscribe to the topic
    public boolean addClientSubscription(String topic, String subscriberId) {
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

        return firstClient.get();
    }

    // return true if the client removed was the last subscribed to the topic
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

        return lastClient.get();
    }

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

        ConcurrentHashMap<String, String> getBrokers() {
            return brokers;
        }

        Set<String> getInterestedBrokers() {
            return Collections.unmodifiableSet(brokers.keySet());
        }

        Set<String> getInterestedBrokersNextHop() {
            return brokers.values().stream().collect(Collectors.toUnmodifiableSet());
        }
    }
}
