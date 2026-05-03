package it.polimi.pubsub.broker.components;

import it.polimi.pubsub.broker.Broker;
import it.polimi.pubsub.network.TcpConnection;

import java.util.concurrent.ExecutorService;

public class ClientHandler extends ConnectionHandler {

    public ClientHandler(TcpConnection connection, Broker broker, ExecutorService executor) {
        super(connection, broker, executor);
    }

    protected void registerNode(String clientId) {
        broker.addClientToRegistry(clientId, this);
    }

    protected void nodeDisconnection(String clientId) {
        broker.onClientDisconnection(clientId);
    }
}
