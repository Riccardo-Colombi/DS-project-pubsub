package it.polimi.pubsub.demos;

import it.polimi.pubsub.client.Client;

public class AutomatedClient {

    public static void main(String[] args) {
        if (args.length < 5) {
            System.out.println("Usage: java ... AutomatedClient <pub|sub> <id> <host> <port> <topic> [num_messages|listen_ms] [delay_ms]");
            System.exit(1);
        }

        String mode = args[0];
        String id = args[1];
        String host = args[2];
        int port = Integer.parseInt(args[3]);
        String topic = args[4];

        try {
            Client client = new Client(id, host, port);
            client.connect();

            if ("pub".equalsIgnoreCase(mode)) {
                int numMessages = args.length > 5 ? Integer.parseInt(args[5]) : 10;
                int delayMs = args.length > 6 ? Integer.parseInt(args[6]) : 200;

                for (int i = 1; i <= numMessages; i++) {
                    String payload = "AutoMessage-" + i;
                    client.publish(topic, payload.getBytes());
                    Thread.sleep(delayMs);
                }

                Thread.sleep(2000);
                client.disconnect();
                System.out.println("Publisher " + id + " finished.");

            } else if ("sub".equalsIgnoreCase(mode)) {
                int listenTimeMs = args.length > 5 ? Integer.parseInt(args[5]) : 10000;
                client.subscribe(topic);

                System.out.println("Subscriber " + id + " listening for " + listenTimeMs + "ms...");
                Thread.sleep(listenTimeMs);

                client.disconnect();
                System.out.println("Subscriber " + id + " finished.");
            }
        } catch (Exception e) {
            System.err.println("AutomatedClient error: " + e.getMessage());
        }

        System.exit(0);
    }
}