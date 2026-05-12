package it.polimi.pubsub.broker.states;

import it.polimi.pubsub.broker.Broker;
import it.polimi.pubsub.broker.components.BrokerRole;
import it.polimi.pubsub.broker.BrokerState;
import it.polimi.pubsub.messages.Message;
import it.polimi.pubsub.messages.brokerMessages.*;

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

//            case PROXY:
//                ProxyMessage proxy = (ProxyMessage) message;
//                broker.replicateToFollowers(
//                        proxy.getInnerMessage(),
//                        proxy.getOriginClientId(),
//                        proxy.getInnerMessage().getMessageId(),
//                        proxy.getSenderId(),
//                        proxy.getMessageId());
//                break;
//
//            case PUBLISH:
//            case SUBSCRIBE:
//            case UNSUBSCRIBE:
//                broker.replicateToFollowers(
//                        message,
//                        message.getSenderId(),
//                        message.getMessageId(),
//                        null,
//                        null);
//                break;
//
//            case APPEND_ENTRY_ACK:
//                broker.handleAppendEntryAck((AppendEntryAckMessage) message);
//                break;

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