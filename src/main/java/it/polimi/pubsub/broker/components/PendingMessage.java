package it.polimi.pubsub.broker.components;

import it.polimi.pubsub.messages.Message;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class PendingMessage {

    private final Message message;
    private volatile long trackingTimestamp;
    private final Set<String> pendingSubscribers;
    //private final int majority;
//    private final Set<String> acksReceived = ConcurrentHashMap.newKeySet();
//    private String originClientId;
//    private String originMessageId;
//    private String proxyBrokerId;
//    private String proxyMessageId;

    public PendingMessage(Message message, Set<String> targetSubscribers) {
        this.message = message;
        this.trackingTimestamp = System.currentTimeMillis();
        this.pendingSubscribers = ConcurrentHashMap.newKeySet();
        this.pendingSubscribers.addAll(targetSubscribers);
        //this.majority = 0;
    }

//    public PendingMessage(Message message, String originClientId,
//                          String originMessageId, String proxyBrokerId,
//                          String proxyMessageId, int majority) {
//        this.message         = message;
//        this.trackingTimestamp = System.currentTimeMillis();
//        this.pendingSubscribers = ConcurrentHashMap.newKeySet();
//        this.originClientId  = originClientId;
//        this.originMessageId = originMessageId;
//        this.proxyBrokerId   = proxyBrokerId;
//        this.majority        = majority;
//        this.proxyMessageId = proxyMessageId;
//    }
//
//    // Aggiunge l'ACK e controlla se si è raggiunta la majority
//    public boolean addAckAndCheckMajority(String brokerId) {
//        acksReceived.add(brokerId);
//        return majority > 0 && acksReceived.size() >= majority;
//    }
//
//    public boolean isRaftEntry() {
//        return majority > 0;
//    }

    public void acknowledge(String subscriberId) {
        pendingSubscribers.remove(subscriberId);
    }

    public boolean isFullyAcknowledged() {
        return pendingSubscribers.isEmpty();
    }

    public void resetTimestamp() {
        trackingTimestamp = System.currentTimeMillis();
    }

    public Set<String> getPendingSubscribers() { return pendingSubscribers; }
    public Message getMessage()                { return message; }
    public long getTrackingTimestamp()         { return trackingTimestamp; }
    //public String getOriginClientId()          { return originClientId; }
    //public String getOriginMessageId()         { return originMessageId; }

    //public String getProxyBrokerId() { return proxyBrokerId; }
    //public String getProxyMessageId() { return proxyMessageId; }
}
