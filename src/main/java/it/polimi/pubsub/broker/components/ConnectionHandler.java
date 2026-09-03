package it.polimi.pubsub.broker.components;

import com.fasterxml.jackson.core.JsonProcessingException;
import it.polimi.pubsub.broker.Broker;
import it.polimi.pubsub.messages.Message;
import it.polimi.pubsub.network.TcpConnection;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class ConnectionHandler {

    protected final TcpConnection connection;
    protected final Broker broker;
    private final ExecutorService executor;
    private volatile boolean running = true;
    private final AtomicBoolean disconnected;
    private volatile long lastHeartbeatTime = System.currentTimeMillis();
    protected volatile String nodeId;
    private final BlockingQueue<Message> outQueue = new LinkedBlockingQueue<>();
    private Future<?> readerTask;
    private Future<?> writerTask;

    public ConnectionHandler(TcpConnection connection, Broker broker, ExecutorService executor) {
        this.connection = connection;
        this.broker = broker;
        this.executor = executor;
        this.disconnected = new AtomicBoolean(false);
    }

    public void start() {
        readerTask = executor.submit(this::readLoop);
        writerTask = executor.submit(this::writeLoop);
    }

    protected abstract void registerNode(String nodeId);

    protected abstract void nodeDisconnection(String nodeId);

    private void readLoop() {
        try {
            while (running) {

                Message msg = connection.receive();

                if (msg == null) {
                    // client disconnected gracefully
                    System.out.println("[Connection Handler] Client disconnected");
                    break;
                }

                broker.onMessageReceived(msg);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("[Connection Handler] Thread interrupted: " + e.getMessage());
        }  catch (JsonProcessingException e) {
            System.out.println("[Connection Handler] Deserialization error: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("[Connection Handler] Connection lost: " + e.getMessage());
        } catch (Exception e) {
            System.out.println("[Connection Handler] Generic error: " + e.getMessage());
        } finally {
            if (disconnected.compareAndSet(false, true))
                shutdown();
        }
    }

    private void writeLoop() {
        try {
            while (running) {
                Message msg = outQueue.take();
                connection.send(msg);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("[Connection Handler] Write loop stopped (shutdown).");
        } catch (Exception e) {
            System.out.println("[Connection Handler] Serialization error: " + e.getMessage());
        } finally {
            if (disconnected.compareAndSet(false, true))
                shutdown();
        }
    }

    // add to the queue the message that has to be sent to the client
    public void send(Message msg) {
        if (!running) {
            return; // Do not attempt to queue if the handler is already shutting down
        }

        try {
            outQueue.put(msg);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("[Connection Handler] Interrupted while queuing message: " + e.getMessage());
        }
    }

    public void updateHeartbeat() {
        lastHeartbeatTime = System.currentTimeMillis();
    }

    public boolean isTimedOut(long timeoutMs) {
        return System.currentTimeMillis() - lastHeartbeatTime > timeoutMs;
    }

    public void forceDisconnect() {
        if (disconnected.compareAndSet(false, true))
            shutdown();
    }

    private void shutdown() {
        running = false;

        try {
            connection.close();
        } catch (IOException e) {
            System.out.println("Error closing connection: " + e.getMessage());
        }

        if (readerTask != null)
            readerTask.cancel(true);

        if (writerTask != null)
            writerTask.cancel(true);

        // Notify broker that client disconnected
        if (nodeId != null)
            nodeDisconnection(nodeId);
    }

    public String getNodeId() {
        return nodeId;
    }
}