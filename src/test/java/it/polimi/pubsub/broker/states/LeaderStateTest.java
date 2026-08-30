package it.polimi.pubsub.broker.states;

import it.polimi.pubsub.broker.Broker;
import it.polimi.pubsub.messages.HeartbeatMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class LeaderStateTest {

    @Mock
    private Broker broker;

    private LeaderState leaderState;

    @BeforeEach
    public void setUp() {
        // Initialize the LeaderState with the mocked Broker
        leaderState = new LeaderState(broker);
    }

    @Test
    public void testHeartbeatFromHigherTermCausesStepDown() {
        // Arrange: Create a mock HeartbeatMessage from a competing leader with a higher term
        HeartbeatMessage mockHeartbeat = mock(HeartbeatMessage.class);
        when(mockHeartbeat.getType()).thenReturn(it.polimi.pubsub.messages.MessageType.HEARTBEAT);
        when(mockHeartbeat.getSenderId()).thenReturn("broker2");
        when(mockHeartbeat.isLeaderHeartbeat()).thenReturn(true);
        when(mockHeartbeat.getTerm()).thenReturn(5);
        when(mockHeartbeat.getLeaderClientPort()).thenReturn(8081);

        // Assume our current broker is only at term 3
        when(broker.getCurrentTerm()).thenReturn(3);

        // Act: The current leader receives the heartbeat
        leaderState.onMessageReceived(mockHeartbeat);

        // Assert: Verify that the liveness timer is updated
        verify(broker, times(1)).updateHeartbeat("broker2");

        // Assert: Verify that the current leader abdicates and becomes a follower
        verify(broker, times(1)).becomeFollower(5, "broker2");

        // Assert: Verify that the new leader's client port is updated
        verify(broker, times(1)).updateLeaderClientPort(8081);
    }

    @Test
    public void testHeartbeatFromLowerTermIsIgnoredForLeadership() {
        // Arrange: Create a mock HeartbeatMessage from an old, isolated leader
        HeartbeatMessage mockHeartbeat = mock(HeartbeatMessage.class);
        when(mockHeartbeat.getType()).thenReturn(it.polimi.pubsub.messages.MessageType.HEARTBEAT);
        when(mockHeartbeat.getSenderId()).thenReturn("broker3");
        when(mockHeartbeat.isLeaderHeartbeat()).thenReturn(true);
        when(mockHeartbeat.getTerm()).thenReturn(2); // Lower term

        // Assume our current broker is at term 4
        when(broker.getCurrentTerm()).thenReturn(4);

        // Act: The current leader receives the stale heartbeat
        leaderState.onMessageReceived(mockHeartbeat);

        // Assert: Verify that the liveness timer is STILL updated (connection is alive)
        verify(broker, times(1)).updateHeartbeat("broker3");

        // Assert: Verify that the current leader DOES NOT step down
        verify(broker, never()).becomeFollower(anyInt(), anyString());
        verify(broker, never()).updateLeaderClientPort(anyInt());
    }
}