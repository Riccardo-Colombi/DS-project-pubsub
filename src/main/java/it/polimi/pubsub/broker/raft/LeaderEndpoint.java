package it.polimi.pubsub.broker.raft;

/**
 * Immutable description of the current cluster leader.
 * Either all fields describe the same node, or the reference is null (leader unknown).
 * Never mutate — always replace.
 */
public record LeaderEndpoint(String leaderId, String host, int brokerPort, int clientPort) {

    public boolean isComplete() {
        return leaderId != null && host != null && !host.isBlank()
                && brokerPort > 0 && clientPort > 0;
    }

    public boolean isSameLeader(String otherId) {
        return leaderId != null && leaderId.equals(otherId);
    }

    @Override
    public String toString() {
        return String.format("%s@%s:%d(client:%d)", leaderId, host, brokerPort, clientPort);
    }
}