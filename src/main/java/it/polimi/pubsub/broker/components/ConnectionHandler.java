package it.polimi.pubsub.broker.components;

import com.fasterxml.jackson.core.JsonProcessingException;
import it.polimi.pubsub.broker.Broker;
import it.polimi.pubsub.messages.Message;
import it.polimi.pubsub.network.TcpConnection;
import it.polimi.pubsub.utils.ConsoleAppender;

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
                    // Client disconnected gracefully
                    ConsoleAppender.asyncMessage(String.format("[Connection] Node disconnected: %s",
                            (nodeId != null ? nodeId : "unknown")));
                    break;
                }

                // Needed it is the first time for a client connecting to a broker
                if (nodeId == null) {
                    nodeId = msg.getSenderId();
                    registerNode(nodeId);
                }

                broker.onMessageReceived(msg);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            ConsoleAppender.asyncMessage("[Connection] Read thread interrupted.");
        }  catch (JsonProcessingException e) {
            ConsoleAppender.asyncMessage("[Connection] [ERROR] Deserialization failed.");
        } catch (IOException e) {
            ConsoleAppender.asyncMessage(String.format("[Connection] [ERROR] Connection lost for node: %s",
                    (nodeId != null ? nodeId : "unknown")));
        } catch (Exception e) {
            ConsoleAppender.asyncMessage(String.format("[Connection] [ERROR] Unexpected read error: %s",
                    e.getMessage()));
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
            // Silent catch: thread interruption during shutdown is physiological
        } catch (Exception e) {
            ConsoleAppender.asyncMessage("[Connection] [ERROR] Message serialization failed.");
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
            ConsoleAppender.asyncMessage("[Connection] [ERROR] Queuing interrupted.");
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
            ConsoleAppender.asyncMessage("[Connection] [ERROR] Socket close failed.");
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