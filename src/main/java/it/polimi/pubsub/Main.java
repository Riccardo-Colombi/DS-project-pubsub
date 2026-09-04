package it.polimi.pubsub;

import it.polimi.pubsub.broker.Broker;
import java.util.concurrent.CountDownLatch;

public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("[Main] Usage: java -cp app.jar it.polimi.pubsub.Main <brokerId> <brokerPort> <clientPort> <walPath> <offlinePath>");
            System.exit(1);
        }

        String brokerId = args[0];
        int brokerPort = Integer.parseInt(args[1]);
        int clientPort = Integer.parseInt(args[2]);
        String walPath = args[3];
        String offlinePath = args[4];

        System.out.println("[Main] Starting " + brokerId + "...");
        Broker broker = new Broker(brokerId, brokerPort, clientPort, walPath, offlinePath);
        broker.run();

        if (args.length >= 6) {
            String action = args[5];
            if ("create".equals(action)) {
                Thread.sleep(2000);
                broker.createCluster();
                System.out.println("[Main] " + brokerId + " created the cluster.");
            } else if ("join".equals(action) && args.length >= 8) {
                Thread.sleep(4000);
                String targetHost = args[6];
                int targetPort = Integer.parseInt(args[7]);
                broker.joinCluster(targetHost, targetPort);
                System.out.println("[Main] " + brokerId + " joined cluster at " + targetHost + ":" + targetPort);
            }
        }

        // Prevent the main thread from exiting, keeping the Docker container alive
        new CountDownLatch(1).await();
    }
}