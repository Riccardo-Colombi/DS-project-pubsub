package it.polimi.pubsub.broker.components;

import it.polimi.pubsub.network.TcpConnection;

import java.util.concurrent.ConcurrentHashMap;

// The broker needs to know the components of a logical SubscriberID. A TCP disconnection doesn't mean that the client is gone forever.
// Maybe the problem is a network partition. This class needs to map a SubscriberID in a way to hold connection details and status for each client

public class ClientRegistry {

    private final ConcurrentHashMap<String, ClientSession> clients;

    public ClientRegistry() {
        this.clients = new ConcurrentHashMap<>();
    }

    public void registerClient(String clientId, TcpConnection connection) {
        // ConcurrentHashMap.compute(...) locks only the specific key it's being modified
        // if(clients.containsKey(id)) { ... } can cause race conditions
        clients.compute(clientId, (id, existingSession) -> {
            if (existingSession == null) {
                // Brand new client
                return new ClientSession(clientId, connection);
            } else {
                // Existing client reconnecting: update the physical socket
                existingSession.updateConnection(connection);
                return existingSession;
            }
        });
    }

    public ClientSession getSession(String clientId) {
        return clients.get(clientId);
    }

    public void markDisconnected(String clientId) {
        ClientSession session = clients.get(clientId);
        if (session != null) {
            session.setStatus(ClientStatus.DISCONNECTED);
        }
    }

    public void removeClient(String clientId) {
        clients.remove(clientId);
    }
}
