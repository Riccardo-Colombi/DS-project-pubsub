package it.polimi.pubsub;

import it.polimi.pubsub.broker.Broker;
import it.polimi.pubsub.client.Client;

public class firstRun {
    public static void main(String[] args) throws Exception {

        // Broker
        Thread brokerThread = new Thread(() -> {
            try {
                new Broker("data/wal.log", "data/offline").run();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        brokerThread.setDaemon(true);
        brokerThread.start();
        Thread.sleep(1_000);   // wait for broker to start

        // Clients
        Client subscriber = new Client("sub1", "localhost", 8080);
        Client publisher  = new Client("pub1", "localhost", 8080);

        subscriber.connect();
        publisher.connect();
        Thread.sleep(500);

        // Step 1 – durable subscription
        subscriber.subscribeDurable("news");
        Thread.sleep(1_000);   // wait for broker to ACK the subscription

        // Step 2 – subscriber goes offline
        System.out.println("=== sub1 disconnecting ===");
        subscriber.disconnect();
        Thread.sleep(500);

        // Step 3 – messages published while sub1 is offline
        publisher.publish("news", "hello1".getBytes());
        publisher.publish("news", "hello2".getBytes());
        Thread.sleep(1_000);   // give broker time to buffer them

        // Step 4 – subscriber reconnects; should receive the two buffered messages
        System.out.println("=== sub1 reconnecting ===");
        subscriber.connect();
        Thread.sleep(2_000);   // give broker time to deliver the backlog

        // Step 5 – clean shutdown
        subscriber.unsubscribe("news");
        Thread.sleep(500);
        subscriber.disconnect();
        publisher.disconnect();
        Thread.sleep(500);

        System.out.println("=== Done ===");

    }
}