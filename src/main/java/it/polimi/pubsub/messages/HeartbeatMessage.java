package it.polimi.pubsub.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class HeartbeatMessage extends Message {

    private final int term;
    private final String leaderId;

    // leaderHeartbeat = true only if sent by real leader.
    private final boolean leaderHeartbeat;
    // valid only if leaderHeartbeat = true
    private final int leaderClientPort;
    private final String leaderHost;
    private final int leaderPort;

    @JsonCreator
    public HeartbeatMessage(
            @JsonProperty("senderId") String senderId,
            @JsonProperty("sequenceNumber") long sequenceNumber,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("term") int term,
            @JsonProperty("leaderId") String leaderId,
            @JsonProperty("leaderHeartbeat") boolean leaderHeartbeat,
            @JsonProperty("leaderClientPort") int leaderClientPort,
            @JsonProperty("leaderHost") String leaderHost,
            @JsonProperty("leaderPort") int leaderPort) {

        super(senderId, sequenceNumber, timestamp);
        this.term = term;
        this.leaderId = leaderId;
        this.leaderHeartbeat = leaderHeartbeat;
        this.leaderClientPort = leaderClientPort;
        this.leaderHost = leaderHost;
        this.leaderPort = leaderPort;
    }

    public int getTerm() { return term; }

    public String getLeaderId() { return leaderId; }

    public boolean isLeaderHeartbeat() { return leaderHeartbeat; }

    public int getLeaderClientPort() { return leaderClientPort; }

    @JsonProperty("leaderHost")
    public String getLeaderHost() { return leaderHost; }

    @JsonProperty("leaderPort")
    public int getLeaderPort() { return leaderPort; }

    @Override
    @JsonIgnore
    public MessageType getType() {
        return MessageType.HEARTBEAT;
    }
}