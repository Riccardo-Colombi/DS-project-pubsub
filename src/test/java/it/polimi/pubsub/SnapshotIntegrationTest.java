package it.polimi.pubsub;

import it.polimi.pubsub.broker.Broker;
import it.polimi.pubsub.broker.components.BrokerRole;
import it.polimi.pubsub.client.Client;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

import static org.junit.jupiter.api.Assertions.*;

public class SnapshotIntegrationTest {

    private static final String ROOT = "data/snapshotTest";

    @BeforeEach
    public void setUp() throws IOException {
        cleanDataDirectory(ROOT);
    }

    @AfterEach
    public void tearDown() throws IOException {
        cleanDataDirectory(ROOT);
    }

    @Test
    public void testSizeBasedTrigger() throws Exception {
        String walPath = ROOT + "/B/wal.log";
        String offlinePath = ROOT + "/B/offline";

        Broker broker = new Broker("brokerB", 9190, 9180, walPath, offlinePath);
        broker.run();
        Thread.sleep(500);
        broker.createCluster();

        Client publisher = new Client("pubB", "localhost", 9180);
        publisher.connect();
        Thread.sleep(500);
        publisher.subscribe("topicB");
        Thread.sleep(300);

        int payloadSize = 20_000;
        int numPublish = 400;

        for (int i = 0; i < numPublish; i++) {
            publisher.publish("topicB", makePayload(payloadSize));
            if (i % 50 == 0) Thread.sleep(50);
        }
        Thread.sleep(1500);

        long finalSize = walSize(walPath);
        long theoreticalSizeWithoutSnapshot = (long) numPublish * payloadSize;

        assertTrue(finalSize < theoreticalSizeWithoutSnapshot,
                "Snapshot did not trigger: WAL file size is too large.");

        publisher.disconnect();
        broker.crash();
    }

    @Test
    public void testRaftMetadataRestore() throws Exception {
        String walPath = ROOT + "/C/wal.log";
        String offlinePath = ROOT + "/C/offline";

        Broker broker = new Broker("brokerC", 9191, 9181, walPath, offlinePath);
        broker.run();
        Thread.sleep(500);
        broker.createCluster();

        Client client = new Client("clientC", "localhost", 9181);
        client.connect();
        Thread.sleep(300);
        client.subscribeDurable("topicC");
        Thread.sleep(300);

        for (int i = 0; i < 500; i++) {
            client.publish("topicC", makePayload(20_000));
        }
        Thread.sleep(4000);

        int termBeforeCrash = broker.getCurrentTerm();
        int commitIndexBeforeCrash = broker.getCommitIndex();
        int lastAppliedBeforeCrash = broker.getLastApplied();

        client.disconnect();
        broker.crash();
        Thread.sleep(500);

        Broker restarted = new Broker("brokerC", 9191, 9181, walPath, offlinePath);
        restarted.run();
        Thread.sleep(500);

        assertTrue(restarted.getCurrentTerm() >= termBeforeCrash, "Term should be restored or higher");
        assertTrue(restarted.getCommitIndex() >= commitIndexBeforeCrash, "Commit index should be restored");
        assertTrue(restarted.getLastApplied() >= lastAppliedBeforeCrash, "Last applied should be restored");

        restarted.crash();
    }

    @Test
    public void testConcurrentTrafficDuringSnapshots() {
        assertDoesNotThrow(() -> {
            String walPath = ROOT + "/A/wal.log";
            String offlinePath = ROOT + "/A/offline";

            Broker broker = new Broker("brokerA", 9193, 9183, walPath, offlinePath);
            broker.run();
            Thread.sleep(500);
            broker.createCluster();

            int numClients = 6;
            Thread[] threads = new Thread[numClients];

            for (int c = 0; c < numClients; c++) {
                final int id = c;
                threads[c] = new Thread(() -> {
                    try {
                        Client client = new Client("conc-client-" + id, "localhost", 9183);
                        client.connect();
                        Thread.sleep(100);

                        for (int i = 0; i < 40; i++) {
                            String topic = "topic-" + (i % 3);
                            client.subscribeDurable(topic);
                            client.publish(topic, makePayload(20_000));
                        }
                        Thread.sleep(200);
                        client.disconnect();
                    } catch (Exception ignored) {}
                });
            }

            for (Thread t : threads) t.start();
            for (Thread t : threads) t.join();

            Thread.sleep(1000);
            broker.crash();
        }, "Concurrent traffic caused an exception during snapshotting.");
    }

    @Test
    public void testDistributedSnapshot() throws Exception {
        String leaderWal = ROOT + "/DSNAP/wal_leader.log";
        String leaderOffline = ROOT + "/DSNAP/offline_leader";
        String followerWal = ROOT + "/DSNAP/wal_follower.log";
        String followerOffline = ROOT + "/DSNAP/offline_follower";

        Broker leader = new Broker("leadDS", 9196, 9186, leaderWal, leaderOffline);
        leader.run();
        Thread.sleep(300);
        leader.createCluster();

        Broker follower = new Broker("follDS", 9197, 9187, followerWal, followerOffline);
        follower.run();
        Thread.sleep(300);
        follower.joinCluster("localhost", 9196);
        Thread.sleep(800);

        follower.crash();
        Thread.sleep(500);

        Client publisher = new Client("pubDS", "localhost", 9186);
        publisher.connect();
        Thread.sleep(300);
        publisher.subscribeDurable("topicDS");

        for (int i = 0; i < 500; i++) {
            publisher.publish("topicDS", makePayload(20_000));
        }
        Thread.sleep(4000);
        publisher.disconnect();

        Broker restartedFollower = new Broker("follDS", 9197, 9187, followerWal, followerOffline);
        restartedFollower.run();
        restartedFollower.joinCluster("localhost", 9196);

        int leaderCommitIndex = leader.getCommitIndex();

        int maxRetries = 10;
        int followerCommit = 0;
        while (maxRetries-- > 0 && (followerCommit = restartedFollower.getCommitIndex()) < leaderCommitIndex) {
            Thread.sleep(500);
        }
        assertEquals(followerCommit, leaderCommitIndex,
                "Follower commitIndex (" + followerCommit + ") should be at least leader's (" + leaderCommitIndex + ")");

        restartedFollower.crash();
        leader.crash();
    }

    private byte[] makePayload(int size) {
        StringBuilder sb = new StringBuilder(size);
        String chunk = "PAYLOAD-DATA-";
        while (sb.length() < size) sb.append(chunk);
        return sb.substring(0, size).getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private long walSize(String path) {
        try {
            return Files.size(Paths.get(path));
        } catch (IOException e) {
            return -1;
        }
    }

    private void cleanDataDirectory(String dirPath) throws IOException {
        Path root = Paths.get(dirPath);
        if (!Files.exists(root)) return;
        Files.walkFileTree(root, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }
            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                if (exc != null) throw exc;
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}