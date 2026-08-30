package it.polimi.pubsub.broker.states;

import it.polimi.pubsub.broker.Broker;
import it.polimi.pubsub.broker.components.BrokerRole;
import it.polimi.pubsub.broker.BrokerState;
import it.polimi.pubsub.messages.HeartbeatMessage;
import it.polimi.pubsub.messages.Message;
import it.polimi.pubsub.messages.brokerMessages.AppendEntryMessage;
import it.polimi.pubsub.messages.brokerMessages.JoinClusterMessage;
import it.polimi.pubsub.messages.brokerMessages.RequestVoteMessage;

public class LeaderState implements BrokerState {

    private final Broker broker;

    public LeaderState(Broker broker) {
        this.broker = broker;
    }

    @Override
    public BrokerRole getRole() {
        return BrokerRole.LEADER;
    }

    @Override
    public void onMessageReceived(Message message) throws Exception {
        switch (message.getType()) {
            case JOIN_CLUSTER:
                broker.handleJoinClusterRequest((JoinClusterMessage) message);
                break;

            case SUBSCRIBE, UNSUBSCRIBE, PUBLISH:
                broker.replicateToFollowers(message);
                break;

            case REQUEST_VOTE:
                RequestVoteMessage rv = (RequestVoteMessage) message;
                if (rv.getTerm() > broker.getCurrentTerm()) {
                    broker.becomeFollower(rv.getTerm(), null);
                    broker.handleRequestVote(rv);
                }
                break;

            case APPEND_ENTRY:
                broker.handleAppendEntry((AppendEntryMessage) message);
                break;

            case HEARTBEAT:
                HeartbeatMessage hb = (HeartbeatMessage) message;
                broker.updateHeartbeat(hb.getSenderId());
                if (hb.isLeaderHeartbeat() && hb.getTerm() > broker.getCurrentTerm()) {
                    broker.becomeFollower(hb.getTerm(), hb.getSenderId());
                    broker.updateLeaderClientPort(hb.getLeaderClientPort());
                }
                break;

            default:
                broker.dispatchMessage(message);
        }
    }

    @Override
    public void onElectionTimeout() {

    }

    @Override
    public void onHeartbeatTick() {
        broker.sendHeartbeat();
    }
}