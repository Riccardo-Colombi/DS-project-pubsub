package it.polimi.pubsub.client;

import it.polimi.pubsub.messages.brokerMessages.RedirectToLeaderMessage;
import it.polimi.pubsub.network.MessageSerializer;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientRedirectTest {

    @Test
    public void testClientRedirectsToNewLeader() throws Exception {
        // Latch used to assert that the client successfully reached the new leader
        CountDownLatch leaderReached = new CountDownLatch(1);
        MessageSerializer serializer = new MessageSerializer();

        // 1. Start a Dummy Leader Server on port 8089
        Thread leaderServer = new Thread(() -> {
            try (ServerSocket server = new ServerSocket(8089)) {
                Socket client = server.accept();
                leaderReached.countDown(); // Client successfully connected here!
                client.close();
            } catch (Exception e) {
                // Ignore expected socket closures during test shutdown
            }
        });
        leaderServer.start();

        // 2. Start a Dummy Follower Server on port 8088
        Thread followerServer = new Thread(() -> {
            try (ServerSocket server = new ServerSocket(8088)) {
                Socket client = server.accept();
                PrintWriter out = new PrintWriter(client.getOutputStream(), true);

                // Create and serialize the redirect message pointing to port 8089
                RedirectToLeaderMessage redirectMsg = new RedirectToLeaderMessage(
                        "broker-follower",
                        1L,
                        System.currentTimeMillis(),
                        "BROKER_LEADER_ID", //TODO change this id
                        "localhost",
                        8089,
                        8089
                );

                // Send the message to the client, forcing it to trigger handleRedirectToLeader()
                out.println(serializer.serialize(redirectMsg));
                client.close();
            } catch (Exception e) {
                // Ignore expected socket closures
            }
        });
        followerServer.start();

        // Allow servers a moment to bind to their ports
        Thread.sleep(500);

        // 3. Start the Client pointing to the Follower (port 8088)
        Client client = new Client("test-client", "localhost", 8088);
        client.connect();

        // 4. Assert: Wait up to 5 seconds for the client to redirect and reach the leader
        boolean success = leaderReached.await(5, TimeUnit.SECONDS);

        assertTrue(success, "The client should have parsed the RedirectToLeaderMessage and connected to port 8089.");

        // Clean up resources
        client.disconnect();
        leaderServer.interrupt();
        followerServer.interrupt();
    }
}