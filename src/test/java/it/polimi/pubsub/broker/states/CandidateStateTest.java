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
public class CandidateStateTest {

    @Mock
    private Broker broker;

    private CandidateState candidateState;

    @BeforeEach
    public void setUp() {
        candidateState = new CandidateState(broker);
    }

    @Test
    public void testHeartbeatFromNewLeaderCausesStepDown() throws Exception {
        HeartbeatMessage mockHeartbeat = mock(HeartbeatMessage.class);
        when(mockHeartbeat.getType()).thenReturn(it.polimi.pubsub.messages.MessageType.HEARTBEAT);
        when(mockHeartbeat.getSenderId()).thenReturn("broker2");
        when(mockHeartbeat.isLeaderHeartbeat()).thenReturn(true);
        when(mockHeartbeat.getTerm()).thenReturn(4);
        when(mockHeartbeat.getLeaderClientPort()).thenReturn(8081);

        when(broker.getCurrentTerm()).thenReturn(3);

        candidateState.onMessageReceived(mockHeartbeat);

        verify(broker, times(1)).updateHeartbeat("broker2");
        verify(broker, times(1)).becomeFollower(4, "broker2");
        verify(broker, times(1)).updateLeaderClientPort(8081);
    }
}