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
public class FollowerStateTest {

    @Mock
    private Broker broker;

    private FollowerState followerState;

    @BeforeEach
    public void setUp() {
        followerState = new FollowerState(broker);
    }

    @Test
    public void testHeartbeatFromValidLeader() throws Exception {
        HeartbeatMessage mockHeartbeat = mock(HeartbeatMessage.class);
        when(mockHeartbeat.getType()).thenReturn(it.polimi.pubsub.messages.MessageType.HEARTBEAT);
        when(mockHeartbeat.getSenderId()).thenReturn("broker1");
        when(mockHeartbeat.isLeaderHeartbeat()).thenReturn(true);
        when(mockHeartbeat.getTerm()).thenReturn(3);
        when(mockHeartbeat.getLeaderClientPort()).thenReturn(8080);

        when(broker.getCurrentTerm()).thenReturn(3);
        when(broker.getCurrentLeaderId()).thenReturn("broker1");

        followerState.onMessageReceived(mockHeartbeat);

        verify(broker, times(1)).updateHeartbeat("broker1");
        verify(broker, times(1)).updateLeaderHeartbeat();
        verify(broker, times(1)).updateLeaderClientPort(8080);
    }
}