package it.polimi.pubsub.broker.components;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

// The broker needs to know the components of a logical SubscriberID. A TCP disconnection doesn't mean that the client is gone forever.
// Maybe the problem is a network partition. This class needs to map a SubscriberID in a way to hold connection details and status for each client

public class ClientRegistry {

    private final ConcurrentHashMap<String, ClientHandler> clients;

    public ClientRegistry() {
        this.clients = new ConcurrentHashMap<>();
    }

    public void registerClient(String clientId, ClientHandler handler) {
        clients.put(clientId, handler);
    }

    public void disconnectClient(String clientId) {
        clients.remove(clientId);
    }

    // check if the client is already registered and connected in the registry
    public boolean isClientConnected(String clientId) {
        return clients.containsKey(clientId);
    }

    public Set<String> getConnectedClients() {
        return Collections.unmodifiableSet(clients.keySet());
    }

    public Collection<ClientHandler> getConnectedClientHandlers() {
        return Collections.unmodifiableCollection(clients.values());
    }

    public ClientHandler getClientHandler(String clientId) {
        return clients.get(clientId);
    }
}
