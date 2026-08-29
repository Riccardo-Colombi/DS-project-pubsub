package it.polimi.pubsub;

import it.polimi.pubsub.broker.Broker;
import java.util.concurrent.CountDownLatch;

public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Usage: java -cp app.jar it.polimi.pubsub.Main <brokerId> <brokerPort> <clientPort> <walPath> <offlinePath>");
            System.exit(1);
        }

        String brokerId = args[0];
        int brokerPort = Integer.parseInt(args[1]);
        int clientPort = Integer.parseInt(args[2]);
        String walPath = args[3];
        String offlinePath = args[4];

        System.out.println("Starting " + brokerId + "...");
        Broker broker = new Broker(brokerId, brokerPort, clientPort, walPath, offlinePath);
        broker.run();

        // Prevent the main thread from exiting, keeping the Docker container alive
        new CountDownLatch(1).await();
    }
}