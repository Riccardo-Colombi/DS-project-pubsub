package it.polimi.pubsub;

import it.polimi.pubsub.broker.Broker;
import it.polimi.pubsub.client.Client;

public class firstRun {
    public static void main(String[] args) throws Exception {

        // start broker in background
        Thread brokerThread = new Thread(() -> {
            try {
                new Broker("data/wal.log").run();
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

        subscriber.subscribe("news");
        Thread.sleep(1000);

        subscriber.disconnect();
        Thread.sleep(1000);

        publisher.publish("news", "hello1".getBytes());
        publisher.publish("news", "hello2".getBytes());
        Thread.sleep(1000);

        subscriber.connect();

        subscriber.unsubscribe("news");
        Thread.sleep(500);
        subscriber.disconnect();
        Thread.sleep(1000);




    }
}