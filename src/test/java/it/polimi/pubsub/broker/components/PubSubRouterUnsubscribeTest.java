package it.polimi.pubsub.broker.components;

import it.polimi.pubsub.broker.Broker;
import it.polimi.pubsub.messages.UnsubscribeMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class PubSubRouterUnsubscribeTest {

    @Mock private Broker broker;
    @Mock private TopicSubscribersTable subscribersTable;
    @Mock private OfflineMessageStore offlineStore;
    @Mock private ConnectionRegistry registry;
    @Mock private AckTracker ackTracker;

    private PubSubRouter router;

    @BeforeEach
    public void setUp() {
        router = new PubSubRouter("broker1", broker, subscribersTable, offlineStore, registry, ackTracker);
    }

    @Test
    public void testHandleUnsubscribeRemovesClientFromTable() {
        UnsubscribeMessage msg = new UnsubscribeMessage("client1", 1, System.currentTimeMillis(), "TopicA");

        router.handleUnsubscribe(msg);

        verify(subscribersTable, times(1)).removeClientSubscription("TopicA", "client1");
    }
}