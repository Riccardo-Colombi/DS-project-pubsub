package it.polimi.pubsub.broker.components;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Tracks every node (client or broker) the broker has ever seen.
 *
 * Key design decision for clients: a TCP disconnection does NOT erase a client's
 * record. The entry stays so that durable subscriptions and offline message
 * buffering survive the disconnection. The handler is removed (-> client is
 * unreachable) but the clientId remains in `clientStatuses` so the broker knows
 * the client exists and may reconnect.
 *
 * Brokers are still fully removed on disconnection (no durable semantics needed).
 */

public class ConnectionRegistry {

    private final ConcurrentHashMap<String, ClientHandler> clients;
    private final ConcurrentHashMap<String, BrokerHandler> brokers;

    // Tracks ALL clients ever seen, even when offline.
    // Value is true = CONNECTED, false = DISCONNECTED.
    private final ConcurrentHashMap<String, Boolean> clientStatuses;

    public ConnectionRegistry() {
        this.clients        = new ConcurrentHashMap<>();
        this.brokers        = new ConcurrentHashMap<>();
        this.clientStatuses = new ConcurrentHashMap<>();
    }

    // ------------------------------------------------------------------
    // Client registration
    // ------------------------------------------------------------------

    /**
     * Called on every connect (first connection or reconnect).
     * Always replaces the old handler so the broker never writes to a dead socket.
     */
    public void registerClient(String clientId, ClientHandler handler) {
        clients.put(clientId, handler);
        clientStatuses.put(clientId, true);
    }

    /**
     * Called on TCP disconnection.
     * Removes the live handler but keeps the clientId record so durable
     * subscriptions and offline buffering continue to work.
     */
    public void markClientOffline(String clientId) {
        clients.remove(clientId);
        clientStatuses.put(clientId, false);
    }

    // ------------------------------------------------------------------
    // Broker registration
    // ------------------------------------------------------------------

    public void registerBroker(String brokerId, BrokerHandler handler) {
        brokers.put(brokerId, handler);
    }

    public void disconnectBroker(String brokerId) {
        brokers.remove(brokerId);
    }

    // ------------------------------------------------------------------
    // Query methods
    // ------------------------------------------------------------------

    /** True only if the client is currently connected (has a live handler). */
    public boolean isClientConnected(String clientId) {
        return clients.containsKey(clientId);
    }

    /**
     * True if the broker has ever seen this clientId (connected or offline).
     * Used to detect reconnects vs first-time connections.
     */
    public boolean isKnownClient(String clientId) {
        return clientStatuses.containsKey(clientId);
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

        if (handler != null) {
            return handler;
        }

        return clients.get(nodeId);
    }

    public Collection<BrokerHandler> getClusterMembers() {
        return brokers.values().stream()
                .filter(BrokerHandler::isClusterMember)
                .collect(Collectors.toList());
    }

}
