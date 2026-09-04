package it.polimi.pubsub.demos;

import it.polimi.pubsub.broker.Broker;
import it.polimi.pubsub.client.Client;
import it.polimi.pubsub.utils.ConsoleAppender;

import java.io.File;
import java.util.Scanner;

public class InteractiveLauncher {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.println("[Launcher] Select node type:\n1 - Broker\n2 - Publisher\n3 - Subscriber");
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
            System.err.println("[Launcher] Fatal error: " +  e.getMessage());
        }
    }

    private static void startBroker(Scanner scanner) throws Exception {
        System.out.print("[Launcher] Broker ID: ");
        String id = scanner.nextLine();
        System.out.print("[Launcher] Broker Port: ");
        int brokerPort = scanner.nextInt();
        System.out.print("[Launcher] Client Port: ");
        int clientPort = scanner.nextInt();
        scanner.nextLine();

        String walPath = "data/" + id + "_wal.log";
        String offlinePath = "data/" + id + "_offline";

        System.out.print("[Launcher] Do you want to clean old data and start fresh? (y/n): ");
        String clean = scanner.nextLine();

        if ("y".equalsIgnoreCase(clean)) {
            File walFile = new File(walPath);
            if (walFile.exists()) {
                walFile.delete();
            }

            File offlineDir = new File(offlinePath);
            if (offlineDir.exists()) {
                deleteDirectory(offlineDir);
            }
            System.out.println("[Launcher] Old data cleared successfully.");
        }

        Broker broker = new Broker(id, brokerPort, clientPort, walPath, offlinePath);
        broker.run();

        ConsoleAppender.asyncMessage("Broker started. Commands: 'create', 'join <IP> <PORT>', or 'bridge <IP> <PORT>'");

        while (true) {
            ConsoleAppender.printPrompt("Broker > ");
            String cmd = scanner.nextLine();
            ConsoleAppender.clearPrompt();

            if ("create".equals(cmd)) {
                broker.createCluster();
            } else if (cmd.startsWith("join")) {
                String[] parts = cmd.split(" ");
                broker.joinCluster(parts[1], Integer.parseInt(parts[2]));
            } else if (cmd.startsWith("bridge")) {
                String[] parts = cmd.split(" ");
                broker.connectToPeer(parts[1], Integer.parseInt(parts[2]));
                ConsoleAppender.asyncMessage(String.format("[Launcher] Bridge established with %s:%s", parts[1], parts[2]));
            }
        }
    }

    private static void deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        directoryToBeDeleted.delete();
    }

    private static void startPublisher(Scanner scanner) throws Exception {
        Client client = setupClient(scanner, "Publisher");

        while (true) {
            ConsoleAppender.printPrompt("[Launcher - Publisher] Topic (or 'exit') > ");
            String topic = scanner.nextLine();
            ConsoleAppender.clearPrompt();

            if ("exit".equals(topic)) break;

            ConsoleAppender.printPrompt("Payload > ");
            String payload = scanner.nextLine();
            ConsoleAppender.clearPrompt();

            client.publish(topic, payload.getBytes());
            ConsoleAppender.asyncMessage("[Launcher - Publisher] Message sent to topic: " + topic);
        }

        client.disconnect();
    }

    private static void startSubscriber(Scanner scanner) throws Exception {
        Client client = setupClient(scanner, "Subscriber");

        while (true) {
            ConsoleAppender.printPrompt("[Launcher - Subscriber] Topic (or 'exit') > ");
            String topic = scanner.nextLine();
            ConsoleAppender.clearPrompt();

            if ("exit".equals(topic)) break;

            client.subscribe(topic);
            ConsoleAppender.asyncMessage("[Launcher - Subscriber] Subscribed to topic: " + topic);
        }

        client.disconnect();
    }

    private static Client setupClient(Scanner scanner, String type) throws Exception {
        System.out.print("[Launcher] " + type + " ID: ");
        String id = scanner.nextLine();
        System.out.print("[Launcher] Broker IP: ");
        String ip = scanner.nextLine();
        System.out.print("[Launcher] Broker Client Port: ");
        int port = scanner.nextInt();
        scanner.nextLine();

        Client client = new Client(id, ip, port);
        client.connect();
        return client;
    }
}