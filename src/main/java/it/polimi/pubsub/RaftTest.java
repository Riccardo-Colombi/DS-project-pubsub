package it.polimi.pubsub;

import it.polimi.pubsub.broker.Broker;
import it.polimi.pubsub.client.Client;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

public class RaftTest {

    private static void cleanDataDirectory(String dirPath) throws IOException {
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

        Thread.sleep(500);

        Broker broker1 = new Broker("broker1", 8090 , 8080, "data/wal_1.log", "data/offlineBroker1");
        Broker broker2 = new Broker("broker2", 8091, 8081, "data/wal_2.log", "data/offlineBroker2");
        Broker broker3 = new Broker("broker3", 8092, 8082, "data/wal_3.log", "data/offlineBroker3");
        Broker broker4 = new Broker("broker-4", 8093, 8083, "data/wal_4.log", "data/offlineBroker4");

        broker1.run();
        broker2.run();
        broker3.run();
        broker4.run();

        Broker broker5 = new Broker("broker5", 8094 , 8084, "data/wal_5.log", "data/offlineBroker5");
        Broker broker6 = new Broker("broker6", 8095, 8085, "data/wal_6.log", "data/offlineBroker6");
        Broker broker7 = new Broker("broker7", 8096, 8086, "data/wal_7.log", "data/offlineBroker7");
        Broker broker8 = new Broker("broker-8", 8097, 8087, "data/wal_8.log", "data/offlineBroker8");

        broker5.run();
        broker6.run();
        broker7.run();
        broker8.run();

        Thread.sleep(500);

        broker1.createCluster();
        broker5.createCluster();

        Thread.sleep(500);

        broker2.joinCluster("localhost", 8090);
        broker3.joinCluster("localhost", 8090);
        broker4.joinCluster("localhost", 8090);

        broker6.joinCluster("localhost", 8094);
        broker7.joinCluster("localhost", 8094);
        broker8.joinCluster("localhost", 8094);

        Thread.sleep(1000);

        broker1.connectToPeer("localhost", 8094);
        broker5.connectToPeer("localhost", 8090);

        Thread.sleep(1000);

        Client subscriber = new Client("client-1", "localhost", 8080);
        Client publisher = new Client("client-2", "localhost", 8084);
        subscriber.connect();
        publisher.connect();

        Thread.sleep(500);

        subscriber.subscribe("Topic1");

        Thread.sleep(500);

        publisher.publish("Topic1", "AAAAAAAAAAAAAA".getBytes());

    }
}