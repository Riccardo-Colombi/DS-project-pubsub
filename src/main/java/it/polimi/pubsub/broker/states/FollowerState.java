package it.polimi.pubsub.broker.states;

import it.polimi.pubsub.broker.Broker;
import it.polimi.pubsub.broker.components.BrokerRole;
import it.polimi.pubsub.broker.BrokerState;
import it.polimi.pubsub.messages.AckMessage;
import it.polimi.pubsub.messages.HeartbeatMessage;
import it.polimi.pubsub.messages.Message;
import it.polimi.pubsub.messages.brokerMessages.AppendEntryMessage;
import it.polimi.pubsub.messages.brokerMessages.CommitEntryMessage;
import it.polimi.pubsub.messages.brokerMessages.JoinClusterMessage;
import it.polimi.pubsub.messages.brokerMessages.RequestVoteMessage;

import java.util.concurrent.ConcurrentHashMap;

public class FollowerState implements BrokerState {

    private final Broker broker;
    private final ConcurrentHashMap<String, String> pendingProxies = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> pendingProxyOriginalIds = new ConcurrentHashMap<>();


    public FollowerState(Broker broker) {
        this.broker = broker;
    }

    @Override
    public BrokerRole getRole() {
        return BrokerRole.FOLLOWER;
    }

    @Override
    public void onMessageReceived(Message message) throws Exception {
        switch (message.getType()) {
            case HEARTBEAT:
                HeartbeatMessage hb = (HeartbeatMessage) message;
                broker.updateHeartbeat(hb.getSenderId());

                if (hb.isLeaderHeartbeat() && hb.getTerm() >= broker.getCurrentTerm()) {
                    if (hb.getTerm() == broker.getCurrentTerm()
                            && hb.getSenderId().equals(broker.getCurrentLeaderId()))
                        broker.updateLeaderHeartbeat();
                    else {
                        broker.becomeFollower(hb.getTerm(), hb.getSenderId());
                    }
                    broker.updateLeaderClientPort(hb.getLeaderClientPort());
                }
                break;

            case APPEND_ENTRY:
                broker.handleAppendEntry((AppendEntryMessage) message);
                break;

            case COMMIT_ENTRY:
                CommitEntryMessage commitEntry = (CommitEntryMessage) message;
                broker.commitEntry(commitEntry.getLogIndex());
                break;

            case PUBLISH:
            case SUBSCRIBE:
            case UNSUBSCRIBE:
                broker.redirectClientToLeader(message.getSenderId());
                break;

            case ACK:
                AckMessage ack = (AckMessage) message;
                String originClientId = pendingProxies.remove(ack.getAcknowledgedMessageId());
                if (originClientId != null) {
                    String originalMessageId = pendingProxyOriginalIds.remove(ack.getAcknowledgedMessageId());
                    broker.sendAck(originClientId, originalMessageId);
                } else {
                    broker.dispatchMessage(message);
                }
                break;

            case BROKER_PUBLISH:
                broker.sendAck(message.getSenderId(), message.getMessageId());
                break;

            case REQUEST_VOTE:
                broker.handleRequestVote((RequestVoteMessage) message);
                break;

            case JOIN_CLUSTER:
                broker.handleJoinClusterRequest((JoinClusterMessage) message);
                break;

            default:
                // TODO: Check install snapshot problem
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