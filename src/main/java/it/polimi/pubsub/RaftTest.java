package it.polimi.pubsub;

import it.polimi.pubsub.broker.Broker;
import it.polimi.pubsub.client.Client;

public class RaftTest {
    public static void main(String[] args) throws Exception {

        Broker broker1 = new Broker("broker1", 8090 , 8080, "data/wal_1.log", "data/offlineBroker1");
        Broker broker2 = new Broker("broker2", 8091, 8081, "data/wal_2.log", "data/offlineBroker2");
        //Broker broker3 = new Broker("broker3", 8092, 8082, "data/wal_3.log", "data/offlineBroker3");

        broker1.run();
        broker2.run();
        //broker3.run();

        Thread.sleep(500);

        broker1.createCluster();

        Thread.sleep(500);

        broker2.joinCluster("localhost", 8090);

        Thread.sleep(500);

        //broker3.joinCluster("localhost", 8090);

        Thread.sleep(500);

        //Broker broker4 = new Broker("broker-4", 8093, 8083, "data/wal_4.log", "data/offlineBroker4");
        //broker4.run();

        Thread.sleep(500);

        //broker4.joinCluster("localhost", 8090);

        Client subscriber = new Client("client-1", "localhost", 8081);
        //Client publisher = new Client("client-2", "localhost", 8080);
        subscriber.connect();
        //publisher.connect();

        Thread.sleep(500);

        subscriber.subscribe("Topic1");

    }
}