package it.polimi.pubsub.broker.state;

import it.polimi.pubsub.network.TcpConnection;

import java.util.concurrent.ConcurrentHashMap;

// The broker needs to know the state of a logical SubscriberID. A TCP disconnection doesn't mean that the client is gone forever.
// Maybe the problem is a network partition. This class needs to map a SubscriberID in a way to hold connection details and status for each client

public class ClientRegistry {
    private final ConcurrentHashMap<String, ClientSession> clients;

    // TODO


    public void registerClient(String subscriberId, TcpConnection connection) {
        // TODO
    }

    public void markDisconnected(String subscriberId) {
        // TODO
    }

    public ClientSession getSession(String subscriberId) {
        return clients.get(subscriberId);
    }
}
