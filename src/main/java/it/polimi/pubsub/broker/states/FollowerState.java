package it.polimi.pubsub.broker.states;

import it.polimi.pubsub.broker.Broker;
import it.polimi.pubsub.broker.components.BrokerRole;
import it.polimi.pubsub.broker.BrokerState;
import it.polimi.pubsub.messages.AckMessage;
import it.polimi.pubsub.messages.Message;
import it.polimi.pubsub.messages.MessageType;
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

//            case APPEND_ENTRY:
//                AppendEntryMessage appendEntry = (AppendEntryMessage) message;
//                broker.appendToWal(appendEntry.getEntry());
//                broker.sendAppendEntryAck(appendEntry);
//                break;
//
//            case PUBLISH:
//            case SUBSCRIBE:
//            case UNSUBSCRIBE:
//                String proxyMessageId = broker.proxyToLeader(message);
//                if (proxyMessageId != null) {
//                    pendingProxies.put(proxyMessageId, message.getSenderId());
//                    pendingProxyOriginalIds.put(proxyMessageId, message.getMessageId());
//                }
//                break;

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