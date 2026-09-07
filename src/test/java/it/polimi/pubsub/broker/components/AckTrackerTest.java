package it.polimi.pubsub.broker.components;

import it.polimi.pubsub.messages.PublishMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class AckTrackerTest {

    private AckTracker ackTracker;

    @BeforeEach
    public void setUp() {
        ackTracker = new AckTracker();
    }

    @Test
    public void testTrackAndHandleAck() {
        PublishMessage msg = new PublishMessage("pub1", 1, System.currentTimeMillis(), "TopicA", "Payload".getBytes());
        String messageId = msg.getMessageId();

        // tracciamento che richiede l'oggetto Message completo
        ackTracker.trackDelivery(msg, Set.of("sub1", "sub2"));

        assertEquals(1, ackTracker.getPendingMessages().size(), "Dovrebbe esserci 1 messaggio in sospeso");

        // Primo client invia l'ACK
        ackTracker.handleAck(messageId, "sub1");
        assertEquals(1, ackTracker.getPendingMessages().size(), "Il messaggio deve rimanere in sospeso aspettando sub2");

        // Secondo client invia l'ACK
        ackTracker.handleAck(messageId, "sub2");

        // Verifica indiretta del fullyAcked: il messaggio viene rimosso dalla mappa
        assertEquals(0, ackTracker.getPendingMessages().size(), "Il messaggio deve essere rimosso dai pending una volta confermato da tutti");
    }
}