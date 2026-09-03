package it.polimi.pubsub.demos;

import it.polimi.pubsub.broker.Broker;
import it.polimi.pubsub.client.Client;

import java.util.Scanner;

public class InteractiveLauncher {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Select node type:\n1 - Broker\n2 - Publisher\n3 - Subscriber");
        int choice = scanner.nextInt();
        scanner.nextLine();

        try {
            if (choice == 1) {
                startBroker(scanner);
            } else if (choice == 2) {
                startPublisher(scanner);
            } else if (choice == 3) {
                startSubscriber(scanner);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void startBroker(Scanner scanner) throws Exception {
        System.out.print("Broker ID: ");
        String id = scanner.nextLine();
        System.out.print("Broker Port: ");
        int brokerPort = scanner.nextInt();
        System.out.print("Client Port: ");
        int clientPort = scanner.nextInt();
        scanner.nextLine();

        Broker broker = new Broker(id, brokerPort, clientPort, "data/" + id + "_wal.log", "data/" + id + "_offline");
        broker.run();

        System.out.println("Broker started. Type 'create' to init cluster or 'join <IP> <PORT>'.");

        while (true) {
            String cmd = scanner.nextLine();
            if ("create".equals(cmd)) {
                broker.createCluster();
            } else if (cmd.startsWith("join")) {
                String[] parts = cmd.split(" ");
                broker.joinCluster(parts[1], Integer.parseInt(parts[2]));
            }
        }
    }

    private static void startPublisher(Scanner scanner) throws Exception {
        Client client = setupClient(scanner, "Publisher");

        while (true) {
            System.out.print("Topic (or 'exit'): ");
            String topic = scanner.nextLine();
            if ("exit".equals(topic)) break;

            System.out.print("Payload: ");
            String payload = scanner.nextLine();
            client.publish(topic, payload.getBytes());
        }
        client.disconnect();
    }

    private static void startSubscriber(Scanner scanner) throws Exception {
        Client client = setupClient(scanner, "Subscriber");

        while (true) {
            System.out.print("Topic (or 'exit'): ");
            String topic = scanner.nextLine();
            if ("exit".equals(topic)) break;

            client.subscribe(topic);
        }
    }

    private static Client setupClient(Scanner scanner, String type) throws Exception {
        System.out.print(type + " ID: ");
        String id = scanner.nextLine();
        System.out.print("Broker IP: ");
        String ip = scanner.nextLine();
        System.out.print("Broker Client Port: ");
        int port = scanner.nextInt();
        scanner.nextLine();

        Client client = new Client(id, ip, port);
        client.connect();
        return client;
    }
}