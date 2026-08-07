package it.polimi.pubsub.messages.brokerMessages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.polimi.pubsub.broker.components.BrokerAddress;
import it.polimi.pubsub.messages.Message;
import it.polimi.pubsub.messages.MessageType;
import java.util.List;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class JoinClusterAckMessage extends Message {

    private final String clusterId;
    private final int currentTerm;
    private final List<BrokerAddress> peerAddresses;
    private final String leaderHost;
    private final int leaderPort;
    private final int leaderClientPort;

    @JsonCreator
    public JoinClusterAckMessage(
            @JsonProperty("senderId") String senderId,
            @JsonProperty("sequenceNumber") long sequenceNumber,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("clusterId") String clusterId,
            @JsonProperty("currentTerm") int currentTerm,
            @JsonProperty("peerAddresses") List<BrokerAddress> peerAddresses,
            @JsonProperty("leaderHost") String leaderHost,
            @JsonProperty("leaderPort") int leaderPort,
            @JsonProperty("leaderClientPort") int leaderClientPort) {

        super(senderId, sequenceNumber, timestamp);
        this.clusterId       = clusterId;
        this.currentTerm     = currentTerm;
        this.peerAddresses   = peerAddresses;
        this.leaderHost      = leaderHost;
        this.leaderPort      = leaderPort;
        this.leaderClientPort = leaderClientPort;
    }

    @JsonProperty("clusterId")
    public String getClusterId() {
        return clusterId;
    }

    @JsonProperty("currentTerm")
    public int getCurrentTerm() {
        return currentTerm;
    }

    @JsonProperty("peerAddresses")
    public List<BrokerAddress> getPeerAddresses() {
        return peerAddresses;
    }

    @JsonProperty("leaderHost")
    public String getLeaderHost() {
        return leaderHost;
    }

    @JsonProperty("leaderPort")
    public int getLeaderPort() {
        return leaderPort;
    }

    @JsonProperty("leaderClientPort")
    public int getLeaderClientPort() {
        return leaderClientPort;
    }

    @Override
    public MessageType getType() {
        return MessageType.JOIN_CLUSTER_ACK;
    }

}