package it.polimi.pubsub.broker.storage;

import it.polimi.pubsub.messages.Message;
import it.polimi.pubsub.messages.PublishMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class WriteAheadLogTest {

    @TempDir
    Path tempDir;

    @Test
    public void testAppendAndRecover() throws Exception {
        String logPath = tempDir.resolve("wal_test.log").toString();
        WriteAheadLog wal = new WriteAheadLog(logPath);

        PublishMessage msg = new PublishMessage("pub1", 1, System.currentTimeMillis(), "TopicA", "Payload".getBytes());
        wal.append(msg);
        wal.close();

        WriteAheadLog walRecovery = new WriteAheadLog(logPath);
        List<Message> recovered = walRecovery.recover();

        assertEquals(1, recovered.size());
        assertEquals(msg.getMessageId(), recovered.get(0).getMessageId());

        walRecovery.close();
    }

    @Test
    public void testCorruptedLogRecovery() throws Exception {
        String logPath = tempDir.resolve("wal_corrupt.log").toString();
        WriteAheadLog wal = new WriteAheadLog(logPath);

        PublishMessage msg1 = new PublishMessage("pub1", 1, System.currentTimeMillis(), "TopicA", "Data1".getBytes());
        wal.append(msg1);
        wal.close();

        File logFile = new File(logPath);
        try (FileWriter fw = new FileWriter(logFile, true)) {
            fw.write("INVALID_JSON_CORRUPTED_LINE\n");
        }

        WriteAheadLog walRecovery = new WriteAheadLog(logPath);
        PublishMessage msg2 = new PublishMessage("pub1", 2, System.currentTimeMillis(), "TopicA", "Data2".getBytes());
        walRecovery.append(msg2);

        List<Message> recovered = walRecovery.recover();

        assertEquals(2, recovered.size());
        assertEquals(msg1.getMessageId(), recovered.get(0).getMessageId());
        assertEquals(msg2.getMessageId(), recovered.get(1).getMessageId());

        walRecovery.close();
    }
}