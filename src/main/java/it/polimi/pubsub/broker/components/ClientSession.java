package it.polimi.pubsub.broker.components;

import it.polimi.pubsub.messages.Message;
import it.polimi.pubsub.network.TcpConnection;

/**
 * - E' più dispendioso tenere aperta una connessione TCP permanente o continuare ad aprirla e chiuderla?
 */

public class ClientSession {

    private final String clientId;
    // So other threads can see this
    private volatile ClientStatus status;
    private TcpConnection connection;

    public ClientSession(String clientId, TcpConnection connection) {
        this.clientId = clientId;
        this.connection = connection;
        this.status = ClientStatus.CONNECTED;
    }

    public String getClientId() {
        return clientId;
    }

    public ClientStatus getStatus() {
        return status;
    }

    public synchronized void setStatus(ClientStatus status) {
        this.status = status;
    }

    public synchronized TcpConnection getConnection() {
        return connection;
    }

    // Used by the broker when a client crash and try to re-connect with a new java.net.Socket
    public synchronized void updateConnection(TcpConnection newConnection) {
        this.connection = newConnection;
        this.status = ClientStatus.CONNECTED;
    }

    // Used by the broker as a safety measure. This ensure the broker to not send data over a disconnected socket
    public synchronized void sendMessage(Message message) throws Exception {
        if (status == ClientStatus.CONNECTED && connection != null) {
            connection.send(message);
        } else {
            throw new IllegalStateException("Client is currently disconnected: " + clientId);
        }
    }

}
