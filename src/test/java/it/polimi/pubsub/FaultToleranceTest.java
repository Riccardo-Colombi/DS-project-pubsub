package it.polimi.pubsub;

import it.polimi.pubsub.client.Client;
import it.polimi.pubsub.utils.DockerUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class FaultToleranceTest {

    @BeforeEach
    public void setUp() throws Exception {
        DockerUtils.executeCommand("docker compose down -v");
        DockerUtils.executeCommand("docker compose up --build -d");
        Thread.sleep(8000);
    }

    @AfterEach
    public void tearDown() throws Exception {
        DockerUtils.executeCommand("docker compose down -v");
    }

    @Test
    public void testNetworkPartitionSurvival() {
        assertDoesNotThrow(() -> {
            Client subscriber = new Client("sub-auto-1", "localhost", 8080);
            subscriber.connect();
            subscriber.subscribeDurable("SystemStatus");
            Thread.sleep(1000);

            DockerUtils.executeCommand("docker exec broker1 tc qdisc replace dev eth0 root netem loss 100%");
            subscriber.disconnect();
            Thread.sleep(8000);

            Client publisher = new Client("pub-auto-1", "localhost", 8081);
            publisher.connect();
            publisher.publish("SystemStatus", "Payload".getBytes());
            publisher.disconnect();
            Thread.sleep(1000);

            DockerUtils.executeCommand("docker exec broker1 tc qdisc del dev eth0 root");
            Thread.sleep(8000);

            subscriber.connect();
            Thread.sleep(2000);
            subscriber.disconnect();
        });
    }

    @Test
    public void testHighLatencyElectionStorm() {
        assertDoesNotThrow(() -> {
            DockerUtils.executeCommand("docker exec broker1 tc qdisc replace dev eth0 root netem delay 8000ms 5000ms distribution normal");
            Thread.sleep(15000);

            Client publisher = new Client("pub-auto-2", "localhost", 8081);
            publisher.connect();
            publisher.publish("Metrics", "Data".getBytes());
            publisher.disconnect();
        });
    }
}