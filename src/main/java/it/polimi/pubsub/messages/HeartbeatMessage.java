package it.polimi.pubsub.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class HeartbeatMessage extends Message {

    private final int term;
    private final String leaderId;

    // leaderHeartbeat = true only if sent by real leader.
    private final boolean leaderHeartbeat;
    // valid only if leaderHeartbeat = true
    private final int leaderClientPort;

    @JsonCreator
    public HeartbeatMessage(
            @JsonProperty("senderId") String senderId,
            @JsonProperty("sequenceNumber") long sequenceNumber,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("term") int term,
            @JsonProperty("leaderId") String leaderId,
            @JsonProperty("leaderHeartbeat") boolean leaderHeartbeat,
            @JsonProperty("leaderClientPort") int leaderClientPort) {

        super(senderId, sequenceNumber, timestamp);
        this.term = term;
        this.leaderId = leaderId;
        this.leaderHeartbeat = leaderHeartbeat;
        this.leaderClientPort = leaderClientPort;
    }

    public int getTerm() {
        return term;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public boolean isLeaderHeartbeat() { return leaderHeartbeat; }

    public int getLeaderClientPort() { return leaderClientPort; }

    @Override
    @JsonIgnore
    public MessageType getType() {
        return MessageType.HEARTBEAT;
    }
}
