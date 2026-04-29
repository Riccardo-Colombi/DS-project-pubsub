package it.polimi.pubsub.broker.components;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

// TODO: discuss if this is still the case
// The broker needs to know the components of a logical SubscriberID. A TCP disconnection doesn't mean that the client is gone forever.
// Maybe the problem is a network partition. This class needs to map a SubscriberID in a way to hold connection details and status for each client

/**
 * Tracks every client the broker has ever seen and whether it is currently online.
 *
 * What have I changed? A TCP disconnection does NOT erase a client's record.
 * The client entry stays so that durable subscriptions and offline message buffering
 * can work correctly. The entry is only fully removed when the client sends an
 * explicit UNSUBSCRIBE for all its topics (handled by the Broker).
 */
public class ClientRegistry {

    // clientId → live handler (null when the client is offline)
    private final ConcurrentHashMap<String, ClientHandler> handlers; // only if the client is online

    // clientId → online/offline status (kept separately so we can track known clients
    // even when their handler is null)
    private final ConcurrentHashMap<String, ClientStatus> statuses; // all clients saw

    public ClientRegistry() {
        this.handlers  = new ConcurrentHashMap<>();
        this.statuses  = new ConcurrentHashMap<>();
    }

    /**
     * Called every time a client connects (first time or reconnect).
     * Always replaces the old handler so the broker never writes to a dead socket.
     */
    public void registerClient(String clientId, ClientHandler handler) {
        handlers.put(clientId, handler);
        statuses.put(clientId, ClientStatus.CONNECTED);
    }

    /**
     * Mark the client as offline and drop the handler reference.
     * The client remains in `statuses` so durable data is not lost.
     */
    public void markOffline(String clientId) {
        handlers.remove(clientId);
        statuses.put(clientId, ClientStatus.DISCONNECTED);
    }

    /**
     * Completely forget a client (called only on explicit, permanent removal).
     */
    public void removeClient(String clientId) {
        handlers.remove(clientId);
        statuses.remove(clientId);
    }

    public boolean isClientConnected(String clientId) {
        return statuses.get(clientId) == ClientStatus.CONNECTED;
    }

    public boolean isKnownClient(String clientId) {
        return statuses.containsKey(clientId);
    }

    /** Returns the live handler, or null if the client is offline. */
    public ClientHandler getClientHandler(String clientId) {
        return handlers.get(clientId);
    }

    public Set<String> getConnectedClients() {
        return Collections.unmodifiableSet(handlers.keySet());
    }

    public Collection<ClientHandler> getConnectedClientHandlers() {
        return Collections.unmodifiableCollection(handlers.values());
    }

    public Set<String> getAllKnownClientIds() {
        return Collections.unmodifiableSet(statuses.keySet());
    }

}
