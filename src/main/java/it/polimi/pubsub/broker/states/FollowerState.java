package it.polimi.pubsub.broker.states;

import it.polimi.pubsub.broker.Broker;
import it.polimi.pubsub.broker.components.BrokerRole;
import it.polimi.pubsub.broker.BrokerState;
import it.polimi.pubsub.messages.AckMessage;
import it.polimi.pubsub.messages.Message;
import it.polimi.pubsub.messages.brokerMessages.AppendEntryMessage;
import it.polimi.pubsub.messages.brokerMessages.CommitEntryMessage;
import it.polimi.pubsub.messages.brokerMessages.JoinClusterMessage;

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
                broker.updateHeartbeat(message.getSenderId());
                break;

            case JOIN_CLUSTER:
                broker.handleJoinClusterRequest((JoinClusterMessage) message);
                break;

            case APPEND_ENTRY:
                AppendEntryMessage appendEntry = (AppendEntryMessage) message;
                broker.appendToWal(appendEntry.getEntry());
                broker.sendAppendEntryAck(appendEntry);
                break;

            case COMMIT_ENTRY:
                CommitEntryMessage commitEntry = (CommitEntryMessage) message;
                broker.applyToLocalState(commitEntry.getEntry());
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

            default:
                // this default behavior is to ensure that clients that sends requests
                // to a follower will be redirected to the leader

                // todo ensure this is the default behavior and that every other possible case is covered

                broker.dispatchMessage(message);
        }
    }

    @Override
    public void onElectionTimeout() {

    }

    @Override
    public void onHeartbeatTick() {

    }
}