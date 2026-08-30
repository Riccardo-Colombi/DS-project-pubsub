package it.polimi.pubsub.demos;

import it.polimi.pubsub.broker.Broker;
import it.polimi.pubsub.client.Client;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

public class HeartBeatCheck {

    public static void cleanDataDirectory(String dirPath) throws IOException {
        Path root = Paths.get(dirPath);
        if (!Files.exists(root)) {
            Files.createDirectories(root);
            System.out.println("Data directory created (was missing): " + root.toAbsolutePath());
            return;
        }

        Files.walkFileTree(root, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                if (exc != null) throw exc;
                // Keep the root itself, delete every child directory
                if (!dir.equals(root)) {
                    Files.delete(dir);
                }
                return FileVisitResult.CONTINUE;
            }
        });

        System.out.println("Data directory cleaned: " + root.toAbsolutePath());
    }

    public static void main(String[] args) throws Exception {

        cleanDataDirectory("data");

        System.out.println("\n--------------------\nCleaned directories\n--------------------\n");

        Thread.sleep(500);


        Broker broker1 = new Broker("broker1", 8090 , 8080, "data/wal_1.log", "data/offlineBroker1");
        Broker broker2 = new Broker("broker2", 8091, 8081, "data/wal_2.log", "data/offlineBroker2");
        Broker broker3 = new Broker("broker3", 8092, 8082, "data/wal_3.log", "data/offlineBroker3");
        Broker broker4 = new Broker("broker4", 8093, 8083, "data/wal_4.log", "data/offlineBroker4");
        Broker broker5 = new Broker("broker5", 8094, 8084, "data/wal_5.log", "data/offlineBroker5");

        Client publisher = new Client("publisher", "localhost", 8080);
        Client subscriber = new Client("subscriber", "localhost", 8080);

        broker1.run();
        broker2.run();
        broker3.run();
        broker4.run();
        broker5.run();

        Thread.sleep(2000);

        publisher.connect();
        subscriber.connect();

        broker1.createCluster();

        Thread.sleep(2000);

        broker2.joinCluster("localhost", 8090);
        broker3.joinCluster("localhost", 8090);
        broker4.joinCluster("localhost", 8090);
        broker5.joinCluster("localhost", 8090);

        Thread.sleep(2000);
        subscriber.subscribe("Cars");
        Thread.sleep(2000);

        broker1.crash();
        Thread.sleep(5000);

        broker1.resurrect("localhost", 8091);
        Thread.sleep(5000);

        publisher.publish("Cars", "IT WORKS".getBytes());

    }
}
