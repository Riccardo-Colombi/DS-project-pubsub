package it.polimi.pubsub.broker.raft;

import it.polimi.pubsub.broker.Broker;
import it.polimi.pubsub.broker.components.BrokerHandler;
import it.polimi.pubsub.broker.components.BrokerRole;
import it.polimi.pubsub.broker.components.ConnectionRegistry;
import it.polimi.pubsub.messages.brokerMessages.RequestVoteAckMessage;
import it.polimi.pubsub.messages.brokerMessages.RequestVoteMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class RaftElectionTest {

    @Mock
    private Broker broker;

    @Mock
    private ConnectionRegistry registry;

    private Raft raft;

    @BeforeEach
    public void setUp() {
        raft = new Raft("broker1", 8090, 8080, broker, registry);
    }

    @Test
    public void testStartElectionAndWinMajority() {
        BrokerHandler brokerHandler1 = mock(BrokerHandler.class);
        BrokerHandler brokerHandler2 = mock(BrokerHandler.class);

        when(brokerHandler1.getNodeId()).thenReturn("broker2");
        when(brokerHandler2.getNodeId()).thenReturn("broker3");

        when(registry.getClusterMembers()).thenReturn(Set.of(brokerHandler1, brokerHandler2));

        raft.becomeFollower(1, null);
        raft.startElection();

        assertEquals(BrokerRole.CANDIDATE, raft.getRole());
        int electionTerm = raft.getCurrentTerm();

        RequestVoteAckMessage positiveAck = new RequestVoteAckMessage(
                "broker2",
                1,
                System.currentTimeMillis(),
                electionTerm,
                true
        );

        raft.handleRequestVoteAck(positiveAck);

        assertEquals(BrokerRole.LEADER, raft.getRole());
        verify(broker, atLeastOnce()).sendHeartbeat();
    }

    @Test
    public void testHandleRequestVoteGrantsVoteForHigherTerm() {
        // Force to join a cluster
        raft.becomeFollower(1, null);
        raft.startElection();
        int currentTerm = raft.getCurrentTerm();

        RequestVoteMessage incomingRequest = new RequestVoteMessage(
                "broker3",
                1,
                System.currentTimeMillis(),
                currentTerm + 1,
                0,
                0
        );

        raft.handleRequestVote(incomingRequest);

        assertEquals(currentTerm + 1, raft.getCurrentTerm());
        assertEquals(BrokerRole.FOLLOWER, raft.getRole());
    }
}