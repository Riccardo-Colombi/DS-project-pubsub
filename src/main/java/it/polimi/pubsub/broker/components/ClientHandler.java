package it.polimi.pubsub.broker.components;

import com.fasterxml.jackson.core.JsonProcessingException;
import it.polimi.pubsub.broker.Broker;
import it.polimi.pubsub.messages.Message;
import it.polimi.pubsub.network.TcpConnection;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClientHandler implements Runnable {

    private final TcpConnection connection;
    private final Broker broker;
    private final ExecutorService executor;
    private volatile boolean running = true;
    private final AtomicBoolean disconnected;
    private volatile long lastHeartbeatTime = System.currentTimeMillis();
    private String clientId;
    private final BlockingQueue<Message> outQueue = new LinkedBlockingQueue<>();
    private Future<?> readerTask;
    private Future<?> writerTask;

    public ClientHandler(TcpConnection connection, Broker broker, ExecutorService executor) {
        this.connection = connection;
        this.broker = broker;
        this.executor = executor;
        this.disconnected = new AtomicBoolean(false);
    }

    @Override
    public void run() {
        readerTask = executor.submit(this::readLoop);
        writerTask = executor.submit(this::writeLoop);
    }

    private void readLoop() {
        try {
            while (running) {

                Message msg = connection.receive();

                if (msg == null) {
                    // client disconnected gracefully
                    System.out.println("Client disconnected");
                    break;
                }

                if (clientId == null) {
                    clientId = msg.getSenderId();
                    broker.addClientToRegistry(clientId, this);
                }

                broker.onMessageReceived(msg, connection);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Thread interrupted: " + e.getMessage());
        }  catch (JsonProcessingException e) {
            System.out.println("Deserialization error: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("Connection lost: " + e.getMessage());
        } catch (Exception e) {
            System.out.println("Generic error: " + e.getMessage());
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
            System.out.println("Thread interrupted: " + e.getMessage());
        } catch (Exception e) {
            System.out.println("Serialization error: " + e.getMessage());
        } finally {
            if (disconnected.compareAndSet(false, true))
                shutdown();
        }
    }

    // add to the queue the message that has to be sent to the client
    public void send(Message msg) {
        try {
            outQueue.put(msg);
        } catch (InterruptedException e) {
            // todo maybe recall send(msg) if the thread is still running
            System.out.println("Interrupted while queuing message: " + e.getMessage());
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
        if (clientId != null)
            broker.onClientDisconnection(clientId);
    }

    public String getClientId() {
        return clientId;
    }
}