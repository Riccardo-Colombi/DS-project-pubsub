package it.polimi.pubsub.broker.components;

import it.polimi.pubsub.broker.Broker;
import it.polimi.pubsub.messages.Message;
import it.polimi.pubsub.messages.MessageType;
import it.polimi.pubsub.messages.brokerMessages.BrokerHelloMessage;
import it.polimi.pubsub.network.TcpConnection;

import java.util.concurrent.ExecutorService;

public class BrokerHandler extends ConnectionHandler {

    public BrokerHandler(TcpConnection connection, Broker broker, ExecutorService executor) {
        super(connection, broker, executor);
    }

    protected void registerNode(String brokerId) {
        broker.addPeerToRegistry(brokerId, this);
    }

    protected void nodeDisconnection(String brokerId) {
        broker.onBrokerDisconnection(brokerId);
    }

    public void performHandshake(String selfId) throws Exception {
        // 1. Announce ourselves synchronously — before the write loop exists
        connection.send(new BrokerHelloMessage(selfId, 0, System.currentTimeMillis()));

        // 2. Block until we read their identity
        Message hello = connection.receive();
        if (hello == null || hello.getType() != MessageType.BROKER_HELLO) {
            connection.close();
            throw new Exception("Handshake failed: expected BROKER_HELLO, got "
                    + (hello == null ? "null" : hello.getType()));
        }

        // 3. nodeId is now known before any loop starts
        this.nodeId = hello.getSenderId();
        registerNode(this.nodeId);
    }

}
