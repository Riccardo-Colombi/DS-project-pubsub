package it.polimi.pubsub;

import it.polimi.pubsub.broker.Broker;
import it.polimi.pubsub.client.Client;

/**
 * End-to-end demo of the durable-subscriber / reconnect flow:
 *
 *  1. sub1 connects and makes a DURABLE subscription to "news".
 *  2. sub1 disconnects (simulates a crash / network partition).
 *  3. pub1 publishes two messages while sub1 is offline.
 *     → the broker buffers them in the OfflineMessageStore.
 *  4. sub1 reconnects.
 *     → the broker delivers the two buffered messages immediately.
 *  5. sub1 explicitly unsubscribes and disconnects cleanly.
 */
public class firstRun {
    public static void main(String[] args) throws Exception {

        // Start broker in background
        Thread brokerThread = new Thread(() -> {
            try {
                new Broker("broker1", 9090, 8080, "data/wal.log", "data/offline").run();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        brokerThread.setDaemon(true);
        brokerThread.start();
        Thread.sleep(1000);

        Client subscriber = new Client("sub1", "localhost", 8080);
        Client publisher  = new Client("pub1", "localhost", 8080);
        subscriber.connect();
        publisher.connect();
        Thread.sleep(500);

        // Step 1 – durable subscription
        subscriber.subscribeDurable("news");
        Thread.sleep(1000);

        // Step 2 – subscriber goes offline
        System.out.println("=== sub1 disconnecting ===");
        subscriber.disconnect();
        Thread.sleep(500);

        // Step 3 – messages published while sub1 is offline → buffered by broker
        publisher.publish("news", "hello1".getBytes());
        publisher.publish("news", "hello2".getBytes());
        Thread.sleep(1000);

        // Step 4 – subscriber reconnects → broker delivers the backlog
        System.out.println("=== sub1 reconnecting ===");
        subscriber.connect();
        Thread.sleep(2000);

        // Step 5 – clean shutdown
        subscriber.unsubscribe("news");
        Thread.sleep(500);
        subscriber.disconnect();
        publisher.disconnect();
        Thread.sleep(500);

        System.out.println("=== Done ===");
    }
}