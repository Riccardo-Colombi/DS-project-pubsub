package it.polimi.pubsub.demos;

import it.polimi.pubsub.broker.Broker;
import it.polimi.pubsub.client.Client;

public class multipleBrokerRingNetworkRun {
    public static void main(String[] args) throws Exception {

        Broker broker1 = new Broker("broker1", 8090 , 8080, "data/wal_1.log", "data/offlineBroker1");
        Broker broker2 = new Broker("broker2", 8091, 8081, "data/wal_2.log", "data/offlineBroker2");
        Broker broker3 = new Broker("broker3", 8092, 8082, "data/wal_3.log", "data/offlineBroker3");

        broker1.run();
        broker2.run();
        broker3.run();

        // Give listeners a moment to bind
        Thread.sleep(500);

        // Only one side initiates — the other accepts passively
        broker1.connectToPeer("localhost", 8091);
        broker2.connectToPeer("localhost", 8092);
        broker3.connectToPeer("localhost", 8090);

        Client subscriber = new Client("sub1", "localhost", 8080);
        subscriber.connect();

        Client publisher  = new Client("pub1", "localhost", 8082);
        publisher.connect();

        Thread.sleep(2000);

        subscriber.subscribe("news");
        Thread.sleep(500);

        publisher.publish("news", "hello1".getBytes());
        Thread.sleep(500);

        subscriber.unsubscribe("news");
        Thread.sleep(500);

        publisher.publish("news", "hello2".getBytes());
    }
}
