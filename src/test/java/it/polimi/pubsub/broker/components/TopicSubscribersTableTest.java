package it.polimi.pubsub.broker.components;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;

public class TopicSubscribersTableTest {

    private TopicSubscribersTable table;

    @BeforeEach
    public void setUp() {
        table = new TopicSubscribersTable();
    }

    @Test
    public void testAddAndRemoveClientSubscription() {
        boolean isNewTopic = table.addClientSubscription("Tech", "client1", true);
        assertTrue(isNewTopic);

        Set<String> clients = table.getInterestedClients("Tech");
        assertTrue(clients.contains("client1"));
        assertTrue(table.isDurableSubscriber("client1", "Tech"));

        boolean removedTopic = table.removeClientSubscription("Tech", "client1");
        assertTrue(removedTopic);

        Set<String> clientsAfter = table.getInterestedClients("Tech");
        assertTrue(clientsAfter.isEmpty());
    }

    @Test
    public void testMultipleSubscribersSameTopic() {
        table.addClientSubscription("News", "client1", false);
        table.addClientSubscription("News", "client2", true);

        Set<String> clients = table.getInterestedClients("News");
        assertEquals(2, clients.size());

        assertFalse(table.isDurableSubscriber("client1", "News"));
        assertTrue(table.isDurableSubscriber("client2", "News"));
    }
}