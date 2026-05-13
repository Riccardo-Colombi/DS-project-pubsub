package it.polimi.pubsub.broker.components;

import it.polimi.pubsub.broker.Broker;
import it.polimi.pubsub.messages.Message;
import it.polimi.pubsub.messages.MessageType;
import it.polimi.pubsub.messages.brokerMessages.BrokerHelloMessage;
import it.polimi.pubsub.network.TcpConnection;

import java.util.concurrent.ExecutorService;

public class BrokerHandler extends ConnectionHandler {

    // Information about other Brokers

    private String remoteHost;
    private int remoteBrokerPort;
    private volatile boolean clusterMember = false;

    public BrokerHandler(TcpConnection connection, Broker broker, ExecutorService executor) {
        super(connection, broker, executor);
    }

    protected void registerNode(String brokerId) {
        broker.addPeerToRegistry(brokerId, this);
    }

    protected void nodeDisconnection(String brokerId) {
        broker.onBrokerDisconnection(brokerId);
    }

    public void performHandshake(String selfId, int selfBrokerPort) throws Exception {
        // 1. Announce ourselves synchronously — before the write loop exists
        connection.send(new BrokerHelloMessage(selfId, 0, System.currentTimeMillis(), selfBrokerPort));

        // 2. Block until we read their identity
        Message hello = connection.receive();
        if (hello == null || hello.getType() != MessageType.BROKER_HELLO) {
            connection.close();
            throw new Exception("Handshake failed: expected BROKER_HELLO, got "
                    + (hello == null ? "null" : hello.getType()));
        }

        // 3. nodeId is now known before any loop starts
        this.nodeId = hello.getSenderId();

        // Legge host/port dalla connessione TCP sottostante
        this.remoteHost = connection.getRemoteHost();
        this.remoteBrokerPort = ((BrokerHelloMessage) hello).getBrokerPort();

        //registerNode(this.nodeId);
    }

    public String getRemoteHost() {
        return remoteHost;
    }

    public int getRemoteBrokerPort() {
        return remoteBrokerPort;
    }

    public void markAsClusterMember() {
        this.clusterMember = true;
    }

    public boolean isClusterMember() {
        return clusterMember;
    }

}
