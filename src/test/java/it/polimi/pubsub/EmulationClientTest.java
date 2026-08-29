package it.polimi.pubsub;

import it.polimi.pubsub.client.Client;

public class EmulationClientTest {
    public static void main(String[] args) throws Exception {

        System.out.println("Connecting external clients to Docker cluster...");

        // Connects to broker1 via the port exposed by Docker
        Client subscriber = new Client("sub-docker", "localhost", 8080);
        subscriber.connect();
        Thread.sleep(1000);

        subscriber.subscribeDurable("SystemStatus");
        System.out.println("Subscribed to SystemStatus on broker1");

        // We will pause the execution here.
        // This is your window to run the 'tc' crash commands in the terminal!
        System.out.println("\n>>> NOW OPEN TERMINAL AND CRASH BROKER1 <<<");
        System.out.println("Waiting 20 seconds for you to inject the fault...\n");
        Thread.sleep(20000);

        // Connect publisher to broker2 (since broker1 should be "dead" now)
        Client publisher = new Client("pub-docker", "localhost", 8081);
        publisher.connect();
        Thread.sleep(1000);

        System.out.println("Publishing message to broker2...");
        publisher.publish("SystemStatus", "Alert: Broker1 is down, but PubSub survives!".getBytes());

        System.out.println("\n>>> NOW OPEN TERMINAL AND RESTORE BROKER1 <<<");
        System.out.println("Waiting 15 seconds for you to restore the network...\n");
        Thread.sleep(15000);

        // Subscriber reconnects to broker1 to get offline messages
        subscriber.connect();

        Thread.sleep(5000);
        System.exit(0);
    }
}