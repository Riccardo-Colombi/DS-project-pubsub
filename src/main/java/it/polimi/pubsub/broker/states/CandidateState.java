package it.polimi.pubsub.broker.states;

import it.polimi.pubsub.broker.Broker;
import it.polimi.pubsub.broker.BrokerState;
import it.polimi.pubsub.broker.components.BrokerRole;
import it.polimi.pubsub.messages.HeartbeatMessage;
import it.polimi.pubsub.messages.Message;
import it.polimi.pubsub.messages.brokerMessages.AppendEntryMessage;
import it.polimi.pubsub.messages.brokerMessages.RequestVoteAckMessage;
import it.polimi.pubsub.messages.brokerMessages.RequestVoteMessage;

public class CandidateState implements BrokerState {

    private final Broker broker;

    public CandidateState(Broker broker) {
        this.broker = broker;
    }

    @Override
    public BrokerRole getRole() {
        return BrokerRole.CANDIDATE;
    }

    @Override
    public void onMessageReceived(Message message) throws Exception {
        switch (message.getType()) {

            case REQUEST_VOTE_ACK:
                broker.handleRequestVoteAck((RequestVoteAckMessage) message);
                break;

            case HEARTBEAT:
                broker.updateHeartbeat(message.getSenderId());
                HeartbeatMessage hb = (HeartbeatMessage) message;
                if (hb.isLeaderHeartbeat() && hb.getTerm() >= broker.getCurrentTerm()) {
                    broker.becomeFollower(hb.getTerm(), hb.getSenderId());
                    broker.updateLeaderClientPort(hb.getLeaderClientPort());
                }
                break;

            case APPEND_ENTRY:
                AppendEntryMessage ae = (AppendEntryMessage) message;
                if (ae.getLeaderTerm() >= broker.getCurrentTerm()) {
                    broker.becomeFollower(ae.getLeaderTerm(), ae.getSenderId());
                    broker.handleAppendEntry(ae);
                }
                break;

            case REQUEST_VOTE:
                broker.handleRequestVote((RequestVoteMessage) message);
                break;

            default:
                // TODO: check if this is all right
                // broker.dispatchMessage(message);
                break;
        }
    }

    @Override
    public void onElectionTimeout() {
        broker.startElection();
    }

    @Override
    public void onHeartbeatTick() {

    }
}