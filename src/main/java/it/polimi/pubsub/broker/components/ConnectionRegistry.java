package it.polimi.pubsub.broker.components;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// The broker needs to know the components of a logical SubscriberID. A TCP disconnection doesn't mean that the client is gone forever.
// Maybe the problem is a network partition. This class needs to map a SubscriberID in a way to hold connection details and status for each client

public class ConnectionRegistry {

    private final ConcurrentHashMap<String, ClientHandler> clients;
    private final ConcurrentHashMap<String, BrokerHandler> brokers;

    public ConnectionRegistry() {
        this.clients = new ConcurrentHashMap<>();
        this.brokers = new ConcurrentHashMap<>();
    }

    public void registerClient(String clientId, ClientHandler handler) {
        clients.put(clientId, handler);
    }

    public void registerBroker(String brokerId, BrokerHandler handler) {
        brokers.put(brokerId, handler);
    }

    public void disconnectClient(String clientId) {
        clients.remove(clientId);
    }

    public void disconnectBroker(String brokerId) {
        brokers.remove(brokerId);
    }

    // check if the client is already registered and connected in the registry
    public boolean isClientConnected(String clientId) {
        return clients.containsKey(clientId);
    }

    // check if the broker is already registered and connected in the registry
    public boolean isBrokerConnected(String brokerId) {
        return brokers.containsKey(brokerId);
    }

    public Set<String> getConnectedClients() {
        return Collections.unmodifiableSet(clients.keySet());
    }

    public Set<String> getConnectedBrokers() {
        return Collections.unmodifiableSet(brokers.keySet());
    }


    public Collection<ClientHandler> getConnectedClientHandlers() {
        return Collections.unmodifiableCollection(clients.values());
    }

    public Collection<BrokerHandler> getConnectedBrokerHandlers() {
        return Collections.unmodifiableCollection(brokers.values());
    }

    public Collection<ConnectionHandler> getConnectedHandlers() {
        return Stream.concat(
                clients.values().stream(),
                brokers.values().stream()
        ).collect(Collectors.toUnmodifiableList());
    }

    public ClientHandler getClientHandler(String clientId) {
        return clients.get(clientId);
    }

    public BrokerHandler getBrokerHandler(String brokerId) {
        return brokers.get(brokerId);
    }

    public ConnectionHandler getConnectionHandler(String nodeId) {
        ConnectionHandler handler = brokers.get(nodeId);

        if (handler != null)
            return handler;

        return clients.get(nodeId);
    }
}
