package it.polimi.pubsub;

import it.polimi.pubsub.broker.Broker;
import it.polimi.pubsub.client.Client;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class ClusterIntegrationTest {

    private static final String ROOT = "data/raftTest";

    @BeforeEach
    public void setUp() throws IOException {
        cleanDataDirectory(ROOT);
    }

    @AfterEach
    public void tearDown() throws IOException {
        cleanDataDirectory(ROOT);
    }

    @Test
    public void testLargeClusterFormationAndRouting() {
        assertDoesNotThrow(() -> {
            Broker broker1 = new Broker("broker1", 8090, 8080, ROOT + "/wal_1.log", ROOT + "/off1");
            Broker broker2 = new Broker("broker2", 8091, 8081, ROOT + "/wal_2.log", ROOT + "/off2");
            Broker broker3 = new Broker("broker3", 8092, 8082, ROOT + "/wal_3.log", ROOT + "/off3");
            Broker broker4 = new Broker("broker4", 8093, 8083, ROOT + "/wal_4.log", ROOT + "/off4");

            broker1.run();
            broker2.run();
            broker3.run();
            broker4.run();
            Thread.sleep(500);

            broker1.createCluster();
            Thread.sleep(500);

            broker2.joinCluster("localhost", 8090);
            broker3.joinCluster("localhost", 8090);
            broker4.joinCluster("localhost", 8090);
            Thread.sleep(1000);

            Client subscriber = new Client("client-1", "localhost", 8080);
            Client publisher = new Client("client-2", "localhost", 8083);

            subscriber.connect();
            publisher.connect();
            Thread.sleep(500);

            subscriber.subscribe("Topic1");
            Thread.sleep(500);

            publisher.publish("Topic1", "Test Payload".getBytes());
            Thread.sleep(500);

            subscriber.disconnect();
            publisher.disconnect();

            broker1.crash();
            broker2.crash();
            broker3.crash();
            broker4.crash();
        }, "Large cluster setup and message routing should not throw exceptions.");
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