package it.polimi.pubsub.client;

import it.polimi.pubsub.messages.UnsubscribeMessage;
import it.polimi.pubsub.network.MessageSerializer;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class ClientUnsubscribeTest {

    @Test
    public void testClientSendsUnsubscribeMessage() throws Exception {
        CountDownLatch messageReceived = new CountDownLatch(1);
        MessageSerializer serializer = new MessageSerializer();
        final String[] receivedJson = new String[1];

        Thread serverThread = new Thread(() -> {
            try (ServerSocket server = new ServerSocket(8087)) {
                Socket clientSocket = server.accept();
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

                String line;
                while ((line = in.readLine()) != null) {
                    if (line.contains("UNSUBSCRIBE")) {
                        receivedJson[0] = line;
                        messageReceived.countDown();
                        break;
                    }
                }
            } catch (Exception ignored) {
            }
        });
        serverThread.start();
        Thread.sleep(500);

        Client client = new Client("test-client", "localhost", 8087);
        client.connect();
        client.unsubscribe("TestTopic");

        boolean success = messageReceived.await(5, TimeUnit.SECONDS);
        assertTrue(success, "Server should have received the unsubscribe message");

        UnsubscribeMessage msg = (UnsubscribeMessage) serializer.deserialize(receivedJson[0]);
        assertEquals("TestTopic", msg.getTopic());

        client.disconnect();
        serverThread.interrupt();
    }
}