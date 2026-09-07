package it.polimi.pubsub.broker.components;

import it.polimi.pubsub.messages.PublishMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class OfflineMessageStoreTest {

    private OfflineMessageStore store;
    private final String TEST_DIR = "data/test_offline";

    @BeforeEach
    public void setUp() throws Exception {
        store = new OfflineMessageStore(TEST_DIR);
    }

    @AfterEach
    public void tearDown() {
        File dir = new File(TEST_DIR);
        if (dir.exists()) {
            for (File file : dir.listFiles()) {
                file.delete();
            }
            dir.delete();
        }
    }

    @Test
    public void testEnqueueAndDrainMessages() throws Exception {
        PublishMessage msg1 = new PublishMessage("pub1", 1, System.currentTimeMillis(), "TopicA", "Payload1".getBytes());
        PublishMessage msg2 = new PublishMessage("pub1", 2, System.currentTimeMillis(), "TopicA", "Payload2".getBytes());

        // Inserimento tramite enqueue
        store.enqueue("client-1", msg1);
        store.enqueue("client-1", msg2);

        assertTrue(store.hasPending("client-1"), "Lo store dovrebbe segnalare messaggi in sospeso");

        // Lettura ed eliminazione simultanea tramite drain
        List<PublishMessage> retrieved = store.drain("client-1");

        assertEquals(2, retrieved.size(), "Dovrebbero essere stati recuperati 2 messaggi");
        assertArrayEquals("Payload1".getBytes(), retrieved.get(0).getPayload(), "Il payload del primo messaggio deve corrispondere");

        assertFalse(store.hasPending("client-1"), "La coda deve risultare vuota dopo il drain");
    }
}