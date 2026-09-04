package it.polimi.pubsub.broker.components;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import it.polimi.pubsub.messages.PublishMessage;
import it.polimi.pubsub.network.MessageSerializer;
import it.polimi.pubsub.utils.ConsoleAppender;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Stores PublishMessages that could not be delivered because the target subscriber
 * was offline at delivery time. Messages are kept until the subscriber reconnects
 * and ACKs them.
 *
 * Persistence strategy:
 *   One JSON file per client: <baseDir>/<clientId>.json
 *   Writes are atomic (write to .tmp, then rename) to survive broker crashes.
 *
 * Thread-safety: all public methods synchronise on a per-client lock to avoid
 * a global bottleneck while still being safe under concurrent access.
 */
public class OfflineMessageStore {

    private final File baseDir;
    private final ObjectMapper mapper;
    private final MessageSerializer serializer;

    // In-memory queue: clientId -> ordered list of pending messages
    private final ConcurrentHashMap<String, LinkedList<PublishMessage>> queues;
    // Per-client locks to avoid synchronising on `this` globally
    private final ConcurrentHashMap<String, Object> locks;

    public OfflineMessageStore(String baseDirPath) throws IOException {
        this.baseDir = new File(baseDirPath);
        this.mapper  = new ObjectMapper();
        this.serializer = new MessageSerializer();
        this.queues  = new ConcurrentHashMap<>();
        this.locks   = new ConcurrentHashMap<>();

        if (!baseDir.exists() && !baseDir.mkdirs()) {
            throw new IOException("Cannot create offline store directory: " + baseDirPath);
        }
    }

    // ------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------

    /**
     * Buffer a message for a client that is currently offline.
     * The message is appended to the in-memory queue and immediately flushed to disk.
     */
    public void enqueue(String clientId, PublishMessage message) throws IOException {
        synchronized (lockFor(clientId)) {
            queueFor(clientId).addLast(message);
            flush(clientId);
        }
    }

    /**
     * Returns (and removes from the store) all messages queued for this client.
     * Call this when the client reconnects to deliver the backlog.
     */
    public List<PublishMessage> drain(String clientId) throws IOException {
        synchronized (lockFor(clientId)) {
            LinkedList<PublishMessage> queue = queueFor(clientId);
            List<PublishMessage> backlog = new ArrayList<>(queue);
            queue.clear();
            deleteFile(clientId); // deletion of the file that has the elements
            return backlog;
        }
    }

    /** True if there are buffered messages for this client. */
    public boolean hasPending(String clientId) {
        synchronized (lockFor(clientId)) {
            LinkedList<PublishMessage> q = queues.get(clientId);
            return ((q != null) && (!q.isEmpty()));
        }
    }

    /**
     * Load all per-client queues from disk.
     * Must be called once at broker startup before accepting connections.
     */
    public void recover() throws IOException {
        File[] files = baseDir.listFiles((dir, name) -> name.endsWith(".json"));
        if (files == null) return;

        for (File file : files) {
            String clientId = file.getName().replace(".json", "");
            try {
                java.util.List<String> lines = mapper.readValue(file, new TypeReference<java.util.List<String>>() {});
                LinkedList<PublishMessage> queue = new LinkedList<>();

                for (String line : lines) {
                    queue.add((PublishMessage) serializer.deserialize(line));
                }

                queues.put(clientId, queue);
                ConsoleAppender.asyncMessage(String.format("[OfflineStore] Recovered %d messages for client %s",
                        queue.size(), clientId));
            } catch (IOException e) {
                ConsoleAppender.asyncMessage(String.format("[OfflineStore] [Error] Corrupted file for client %s. Error: %s",
                        clientId, e.getMessage()));
            }
        }
    }

    // ------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------

    private LinkedList<PublishMessage> queueFor(String clientId) {
        return queues.computeIfAbsent(clientId, k -> new LinkedList<>());
    }

    private Object lockFor(String clientId) {
        return locks.computeIfAbsent(clientId, k -> new Object());
    }

    /**
     * Atomically write the current in-memory queue for clientId to disk.
     * Must be called while holding the per-client lock.
     */
    private void flush(String clientId) throws IOException {
        LinkedList<PublishMessage> queue = queueFor(clientId);
        File target = fileFor(clientId);
        File temp   = new File(target.getAbsolutePath() + ".tmp");

        try {
            java.util.List<String> serializedMessages = new java.util.ArrayList<>();

            for (PublishMessage pm : queue) {
                serializedMessages.add(serializer.serialize(pm));
            }
            mapper.writeValue(temp, serializedMessages);

            Files.move(temp.toPath(), target.toPath(),
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (Exception e) {
            if (temp.exists()) temp.delete();
            throw new IOException(e);
        }
    }

    private void deleteFile(String clientId) {
        File f = fileFor(clientId);
        if (f.exists()) f.delete();
    }

    private File fileFor(String clientId) {
        // Sanitise clientId to avoid path traversal
        String safeName = clientId.replaceAll("[^a-zA-Z0-9_\\-]", "_");
        return new File(baseDir, safeName + ".json");
    }
}

