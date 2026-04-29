package it.polimi.pubsub.broker.components;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import it.polimi.pubsub.messages.PublishMessage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
 *   The file contains a JSON array of serialised PublishMessage objects.
 *   Writes are atomic (write to .tmp, then rename) to survive broker crashes.
 *
 * This class is thread-safe: all public methods synchronise on the per-client lock
 * stored in `locks`. This avoids a global lock while still being safe under concurrent
 * access from the retry scheduler and the message-dispatch thread.
 */
public class OfflineMessageStore {

    private final File baseDir;
    private final ObjectMapper mapper;

    // In-memory queue: clientId -> ordered list of pending messages
    private final ConcurrentHashMap<String, LinkedList<PublishMessage>> queues;
    // Per-client locks to avoid synchronising on `this` globally
    private final ConcurrentHashMap<String, Object> locks;

    public OfflineMessageStore(String baseDirPath) throws IOException {
        this.baseDir = new File(baseDirPath);
        this.mapper  = new ObjectMapper();
        this.queues  = new ConcurrentHashMap<>();
        this.locks   = new ConcurrentHashMap<>();

        if (!baseDir.exists() && !baseDir.mkdirs()) {
            throw new IOException("Cannot create offline store directory: " + baseDirPath);
        }
    }

    // Public methods
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

    /**
     * Remove a specific message from the queue (e.g. after the broker receives the ACK
     * while the client is reconnecting and the message was already re-sent).
     */
    public void remove(String clientId, String messageId) throws IOException {
        synchronized (lockFor(clientId)) {
            LinkedList<PublishMessage> queue = queueFor(clientId);
            boolean changed = queue.removeIf(m -> m.getMessageId().equals(messageId));
            if (changed) flush(clientId);
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
     * Load all queues from disk. Call once at broker startup (before accepting connections).
     */
    public void recover() throws IOException {
        File[] files = baseDir.listFiles((dir, name) -> name.endsWith(".json"));
        if (files == null) return;

        for (File file : files) {
            String clientId = file.getName().replace(".json", "");
            try {
                List<PublishMessage> messages = mapper.readValue(
                        file,
                        new TypeReference<List<PublishMessage>>() {}
                );
                LinkedList<PublishMessage> queue = new LinkedList<>(messages);
                queues.put(clientId, queue);
                System.out.println("[OfflineStore] Recovered " + messages.size()
                        + " messages for client: " + clientId);
            } catch (IOException e) {
                System.err.println("[OfflineStore] Corrupted file for client "
                        + clientId + ", skipping: " + e.getMessage());
            }
        }
    }

    // Private helpers
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
            mapper.writeValue(temp, new ArrayList<>(queue));
            Files.move(temp.toPath(), target.toPath(),
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            if (temp.exists()) temp.delete();
            throw e;
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

