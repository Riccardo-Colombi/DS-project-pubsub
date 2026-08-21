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

import java.sql.SQLOutput;
import java.util.Objects;
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

                //System.out.println(broker.getBrokerId() + " is " + broker.getRole() + " for " + broker.getCurrentLeaderId());

                if (hb.getTerm() >= broker.getCurrentTerm()) {
                    if (broker.getCurrentLeaderId().equals(hb.getSenderId()))
                        broker.updateLeaderHeartbeat();
                    else if (broker.getCurrentTerm() != hb.getTerm() /* it will be an higher term */){
                        // TODO UPDATE LEADER THAT HAS THIS HIGHER TERM
                        broker.becomeFollower(hb.getTerm(), hb.getLeaderId());
                    }
                }
                break;

            case APPEND_ENTRY:
                broker.handleAppendEntry((AppendEntryMessage) message);
                break;

            case COMMIT_ENTRY:
                CommitEntryMessage commitEntry = (CommitEntryMessage) message;
                broker.commitEntry(commitEntry.getLogIndex());
                break;

            // todo : PUBLISH SUBSCRIBE and UNSUBSCRIBE are for edge case coverage, need to understand if they are fully covered

            case PUBLISH:
                broker.redirectClientToLeader(message.getSenderId());
                break;

            case SUBSCRIBE:
                broker.redirectClientToLeader(message.getSenderId());
                break;

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
                // this default behavior is to ensure that clients that sends requests
                // to a follower will be redirected to the leader

                // todo ensure this is the default behavior and that every other possible case is covered

                broker.dispatchMessage(message);
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