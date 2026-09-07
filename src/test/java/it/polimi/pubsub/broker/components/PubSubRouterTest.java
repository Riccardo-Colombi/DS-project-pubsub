package it.polimi.pubsub.broker.components;

import it.polimi.pubsub.broker.Broker;
import it.polimi.pubsub.messages.PublishMessage;
import it.polimi.pubsub.messages.UnsubscribeMessage;
import it.polimi.pubsub.messages.brokerMessages.BrokerSubscribeMessage;
import it.polimi.pubsub.messages.brokerMessages.BrokerUnsubscribeMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Set;

public class PubSubRouterTest {

    private Broker mockBroker;
    private TopicSubscribersTable mockTable;
    private OfflineMessageStore mockStore;
    private ConnectionRegistry mockRegistry;
    private AckTracker mockTracker;
    private PubSubRouter router;

    @BeforeEach
    public void setUp() {
        // Initialize fresh mocks before each test to ensure isolation
        mockBroker = Mockito.mock(Broker.class);
        mockTable = Mockito.mock(TopicSubscribersTable.class);
        mockStore = Mockito.mock(OfflineMessageStore.class);
        mockRegistry = Mockito.mock(ConnectionRegistry.class);
        mockTracker = Mockito.mock(AckTracker.class);

        router = new PubSubRouter("broker1", mockBroker, mockTable, mockStore, mockRegistry, mockTracker);
    }

    @Test
    public void testPublishRouting() {
        Mockito.when(mockTable.getInterestedClients("Sports")).thenReturn(Set.of("clientA"));
        ClientHandler mockHandler = Mockito.mock(ClientHandler.class);
        Mockito.when(mockRegistry.getClientHandler("clientA")).thenReturn(mockHandler);

        PublishMessage pubMsg = new PublishMessage("pub1", 1, System.currentTimeMillis(), "Sports", "Goal!".getBytes());
        router.handlePublish(pubMsg);

        Mockito.verify(mockHandler, Mockito.times(1)).send(pubMsg);
        Mockito.verify(mockBroker, Mockito.times(1)).sendAck("pub1", pubMsg.getMessageId());
    }

    @Test
    public void testHandleUnsubscribeRemovesClientFromTable() {
        // Uniamo qui la logica dell'altro file per non avere classi frammentate
        Broker mockBroker = Mockito.mock(Broker.class);
        TopicSubscribersTable mockTable = Mockito.mock(TopicSubscribersTable.class);
        OfflineMessageStore mockStore = Mockito.mock(OfflineMessageStore.class);
        ConnectionRegistry mockRegistry = Mockito.mock(ConnectionRegistry.class);
        AckTracker mockTracker = Mockito.mock(AckTracker.class);

        PubSubRouter router = new PubSubRouter("broker1", mockBroker, mockTable, mockStore, mockRegistry, mockTracker);
        UnsubscribeMessage msg = new UnsubscribeMessage("client1", 1, System.currentTimeMillis(), "TopicA");

        router.handleUnsubscribe(msg);

        Mockito.verify(mockTable, Mockito.times(1)).removeClientSubscription("TopicA", "client1");
    }

    @Test
    public void testDeliverOfflineMessages() throws Exception {
        PublishMessage msg = new PublishMessage("pub1", 1, System.currentTimeMillis(), "TopicA", "Payload".getBytes());
        Mockito.when(mockStore.drain("client1")).thenReturn(java.util.List.of(msg));

        ClientHandler mockHandler = Mockito.mock(ClientHandler.class);
        Mockito.when(mockRegistry.getClientHandler("client1")).thenReturn(mockHandler);

        router.deliverOfflineMessages("client1");

        Mockito.verify(mockTracker, Mockito.times(1)).trackDelivery(Mockito.eq(msg), Mockito.anySet());
        Mockito.verify(mockHandler, Mockito.times(1)).send(msg);
    }

    @Test
    public void testForwardPublishMessageToOfflineDurableClient() throws Exception {
        PublishMessage msg = new PublishMessage("pub1", 1, System.currentTimeMillis(), "TopicA", "Payload".getBytes());

        Mockito.when(mockTable.getInterestedClients("TopicA")).thenReturn(Set.of("client1"));
        Mockito.when(mockRegistry.getClientHandler("client1")).thenReturn(null);
        Mockito.when(mockTable.isDurableSubscriber("client1", "TopicA")).thenReturn(true);

        router.handlePublish(msg);

        Mockito.verify(mockStore, Mockito.times(1)).enqueue("client1", msg);
    }

    @Test
    public void testHandleBrokerPublishAndForwarding() {
        PublishMessage origMsg = new PublishMessage("pub1", 1, System.currentTimeMillis(), "TopicA", "Payload".getBytes());
        it.polimi.pubsub.messages.brokerMessages.BrokerPublishMessage brokerMsg =
                new it.polimi.pubsub.messages.brokerMessages.BrokerPublishMessage("broker2", 1, System.currentTimeMillis(), origMsg);

        Mockito.when(mockTable.getNextHopOfInterestedBrokers("TopicA")).thenReturn(Set.of("broker3"));
        BrokerHandler mockBrokerHandler = Mockito.mock(BrokerHandler.class);
        Mockito.when(mockRegistry.getBrokerHandler("broker3")).thenReturn(mockBrokerHandler);

        router.handleBrokerPublish(brokerMsg);

        Mockito.verify(mockBroker, Mockito.times(1)).updateHeartbeat("broker2");
        Mockito.verify(mockBroker, Mockito.times(1)).sendAck("broker2", brokerMsg.getMessageId());
        Mockito.verify(mockBrokerHandler, Mockito.times(1)).send(Mockito.any(it.polimi.pubsub.messages.brokerMessages.BrokerPublishMessage.class));
    }

    @Test
    public void testHandleBrokerUnsubscribe() {
        it.polimi.pubsub.messages.brokerMessages.BrokerUnsubscribeMessage msg =
                new it.polimi.pubsub.messages.brokerMessages.BrokerUnsubscribeMessage("broker2", 1, System.currentTimeMillis(), "broker3", "TopicA");

        Mockito.when(mockTable.removeBrokerSubscription("TopicA", "broker3")).thenReturn(true);

        BrokerHandler mockHandler = Mockito.mock(BrokerHandler.class);
        Mockito.when(mockHandler.getNodeId()).thenReturn("broker4");
        Mockito.when(mockRegistry.getConnectedBrokerHandlers()).thenReturn(java.util.List.of(mockHandler));

        router.handleBrokerUnsubscribe(msg);

        Mockito.verify(mockBroker, Mockito.times(1)).sendAck("broker2", msg.getMessageId());
        Mockito.verify(mockHandler, Mockito.times(1)).send(Mockito.any(it.polimi.pubsub.messages.brokerMessages.BrokerUnsubscribeMessage.class));
    }

    @Test
    public void testHandleLeaderModification() {
        Mockito.when(mockTable.getTopics()).thenReturn(Set.of("TopicA"));
        Mockito.when(mockTable.getNextHopOfInterestedBrokers("TopicA")).thenReturn(Set.of("oldLeader"));
        Mockito.when(mockTable.getInterestedBrokers("TopicA")).thenReturn(Set.of("oldLeader"));

        Mockito.when(mockTable.addBrokerSubscription("TopicA", "newLeader", "newLeader")).thenReturn(true);
        Mockito.when(mockTable.removeBrokerSubscription("TopicA", "oldLeader")).thenReturn(true);

        BrokerHandler mockHandler = Mockito.mock(BrokerHandler.class);
        Mockito.when(mockHandler.getNodeId()).thenReturn("broker3");
        Mockito.when(mockRegistry.getConnectedBrokerHandlers()).thenReturn(List.of(mockHandler));

        router.handleLeaderModification("oldLeader", "newLeader");

        Mockito.verify(mockTable, Mockito.times(1)).modifyNextHopForBroker("TopicA", "oldLeader", "newLeader");
        Mockito.verify(mockTable, Mockito.times(1)).addBrokerSubscription("TopicA", "newLeader", "newLeader");
        Mockito.verify(mockTable, Mockito.times(1)).removeBrokerSubscription("TopicA", "oldLeader");

        Mockito.verify(mockHandler, Mockito.times(1)).send(Mockito.any(BrokerSubscribeMessage.class));
        Mockito.verify(mockHandler, Mockito.times(1)).send(Mockito.any(BrokerUnsubscribeMessage.class));
    }
}