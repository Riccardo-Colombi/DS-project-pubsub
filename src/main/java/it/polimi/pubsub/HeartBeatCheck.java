package it.polimi.pubsub;

import it.polimi.pubsub.broker.Broker;
import it.polimi.pubsub.client.Client;

import static it.polimi.pubsub.RaftTest.cleanDataDirectory;

public class HeartBeatCheck {
    public static void main(String[] args) throws Exception {

        cleanDataDirectory("data");

        System.out.println("\n--------------------\nCleaned directories\n--------------------\n");

        Thread.sleep(500);


        Broker broker1 = new Broker("broker1", 8090 , 8080, "data/wal_1.log", "data/offlineBroker1");
        Broker broker2 = new Broker("broker2", 8091, 8081, "data/wal_2.log", "data/offlineBroker2");
        Broker broker3 = new Broker("broker3", 8092, 8082, "data/wal_3.log", "data/offlineBroker3");
        Broker broker4 = new Broker("broker4", 8093, 8083, "data/wal_4.log", "data/offlineBroker4");

        broker1.run();
        broker2.run();
        broker3.run();
        broker4.run();

        Thread.sleep(2000);

        broker1.createCluster();

        Thread.sleep(2000);

        broker2.joinCluster("broker1", 8090);
        broker3.joinCluster("broker1", 8090);
        broker4.joinCluster("broker2", 8090);

        Thread.sleep(2000);

        broker2.crash();

        Thread.sleep(200000);

    }
}
