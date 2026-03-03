package it.polimi.pubsub.broker.state;

import it.polimi.pubsub.message.Message;

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
            if (currentTime - pending.getTimestamp() > timeoutMillis) {
                expired.add(pending);
            }
        }
        return expired;
    }
}
