package it.polimi.pubsub.broker.components;

import it.polimi.pubsub.messages.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Come faccio a garantire at least once in caso di crash totale di un server? Devo comunque garantirla?
 *      Metodo per farlo?
 */

// This is used to guarantee a "at-least-once delivery guarantee". When the broker forwards a PublishMessage to some subscribers it needs an
// AckMessage from each one of them. As done before, map a message to a tracking object.
public class AckTracker {

    private final ConcurrentHashMap<String, PendingMessage> unackedMessages;

    public AckTracker() {
        this.unackedMessages = new ConcurrentHashMap<>();
    }

    public void trackDelivery(Message message, Set<String> targetSubscribers) {
        if (targetSubscribers == null || targetSubscribers.isEmpty()) {
            return;
        }
        PendingMessage pending = new PendingMessage(message, targetSubscribers);
        unackedMessages.put(message.getMessageId(), pending);
    }

    public void handleAck(String messageId, String subscriberId) {
        PendingMessage pending = unackedMessages.get(messageId);
        if (pending != null) {
            pending.acknowledge(subscriberId);

            if (pending.isFullyAcknowledged()) {
                unackedMessages.remove(messageId);
            }
        }
    }

    public List<PendingMessage> getExpiredMessages(long timeoutMillis) {
        long currentTime = System.currentTimeMillis();
        List<PendingMessage> expired = new ArrayList<>();

        for (PendingMessage pending : unackedMessages.values()) {
            if (currentTime - pending.getTrackingTimestamp() > timeoutMillis) {
                expired.add(pending);
            }
        }
        return expired;
    }

    /**
     * Reset the retry timer for every pending message that still expects an ACK
     * from the given subscriber.
     *
     * Called right after delivering the offline backlog to a reconnected client
     * so the retry scheduler does not fire again before the client has had a
     * chance to ACK the freshly delivered messages (which would cause double delivery).
     */
    public void resetTimestampsForSubscriber(String subscriberId) {
        for (PendingMessage pending : unackedMessages.values()) {
            if (pending.getPendingSubscribers().contains(subscriberId))
                pending.resetTimestamp();
        }
    }

    public java.util.Collection<PendingMessage> getPendingMessages() {
        return unackedMessages.values();
    }

    /**
     * Remove a client disconnected correctly from all messages awaiting an ACK.
     * If a message has no more subscribers waiting, it's removed from memory.
     */
    public void removeOfflineClientReferences(String clientId) {
        for (java.util.Map.Entry<String, PendingMessage> entry : unackedMessages.entrySet()) {
            PendingMessage pending = entry.getValue();

            pending.acknowledge(clientId);

            if (pending.isFullyAcknowledged()) {
                unackedMessages.remove(entry.getKey());
            }
        }
    }

    public void clear() {
        unackedMessages.clear();
    }
}
