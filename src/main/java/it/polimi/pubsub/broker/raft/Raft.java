package it.polimi.pubsub.broker.raft;

import it.polimi.pubsub.broker.Broker;
import it.polimi.pubsub.broker.BrokerState;
import it.polimi.pubsub.broker.components.BrokerAddress;
import it.polimi.pubsub.broker.components.BrokerHandler;
import it.polimi.pubsub.broker.components.BrokerRole;
import it.polimi.pubsub.broker.components.ConnectionHandler;
import it.polimi.pubsub.broker.components.ConnectionRegistry;
import it.polimi.pubsub.broker.states.CandidateState;
import it.polimi.pubsub.broker.states.FollowerState;
import it.polimi.pubsub.broker.states.LeaderState;
import it.polimi.pubsub.broker.states.SoloState;
import it.polimi.pubsub.broker.storage.LogEntry;
import it.polimi.pubsub.broker.storage.RaftMetadata;
import it.polimi.pubsub.messages.Message;
import it.polimi.pubsub.messages.brokerMessages.*;
import it.polimi.pubsub.utils.ConsoleAppender;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class Raft {

    private final String BROKER_ID;
    private final int BROKER_PORT;
    private final int CLIENT_PORT;
    private final Broker broker;
    private final ConnectionRegistry connectionRegistry;

    private volatile BrokerState state;
    private String clusterId;
    private volatile int currentTerm;
    private String currentLeaderId;
    private String leaderHost;
    private int leaderPort;
    private int leaderClientPort;

    private volatile String votedFor = null;
    private volatile int electionVotes = 0;
    private volatile long lastLeaderHeartbeatMs = System.currentTimeMillis();
    private volatile long electionTimeoutMs;

    private final List<LogEntry> raftLog = new ArrayList<>();
    private volatile int commitIndex = 0;
    private volatile int lastApplied = 0;
    private final ConcurrentHashMap<String, Integer> followerNextIndex  = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> followerMatchIndex = new ConcurrentHashMap<>();

    public Raft(String brokerId, int brokerPort, int clientPort,
                Broker broker, ConnectionRegistry connectionRegistry) {
        this.BROKER_ID = brokerId;
        this.BROKER_PORT = brokerPort;
        this.CLIENT_PORT = clientPort;
        this.broker = broker;
        this.connectionRegistry = connectionRegistry;
        this.electionTimeoutMs = ThreadLocalRandom.current().nextLong(6000, 12001);
        this.state = new SoloState(broker);
    }

    public BrokerState getState() {
        return state;
    }

    public void setState(BrokerState state) {
        this.state = state;
    }

    public BrokerRole getRole() {
        return state.getRole();
    }

    public int getCurrentTerm() {
        return currentTerm;
    }

    public int getCommitIndex() {
        return commitIndex;
    }

    public int getLastApplied() {
        return lastApplied;
    }

    public String getLeaderHost() {
        return leaderHost;
    }

    public int getLeaderPort() {
        return leaderPort;
    }

    public int getLeaderClientPort() {
        return leaderClientPort;
    }

    public long getElectionTimeoutMs() {
        return electionTimeoutMs;
    }

    public long getLastLeaderHeartbeatMs() {
        return lastLeaderHeartbeatMs;
    }

    public synchronized int getLastLogIndex() {
        return raftLog.size();
    }

    public synchronized int getLastLogTerm() {
        return raftLog.isEmpty() ? 0 : raftLog.get(raftLog.size() - 1).getTerm();
    }

    public void updateLeaderHeartbeat() {
        lastLeaderHeartbeatMs = System.currentTimeMillis();
    }

    public void checkElectionTimeout() {
        if (System.currentTimeMillis() - lastLeaderHeartbeatMs > electionTimeoutMs) {
            state.onElectionTimeout();
        }
    }

    public synchronized void startElection() {
        if (state.getRole() == BrokerRole.LEADER || state.getRole() == BrokerRole.SOLO) return;

        currentTerm++;
        votedFor = BROKER_ID;
        electionVotes = 1;
        electionTimeoutMs = ThreadLocalRandom.current().nextLong(6000, 12001);
        lastLeaderHeartbeatMs = System.currentTimeMillis();
        state = new CandidateState(broker);

        ConsoleAppender.asyncMessage(String.format("[Raft %s] Starting election for term %d",
                BROKER_ID, currentTerm));

        long seq = broker.getMessageCounter();
        RequestVoteMessage rv = new RequestVoteMessage(BROKER_ID, seq, System.currentTimeMillis(), currentTerm, getLastLogIndex(), getLastLogTerm());

        for (BrokerHandler handler : connectionRegistry.getClusterMembers()) {
            handler.send(rv);
        }
    }

    public synchronized void handleRequestVote(RequestVoteMessage msg) {
        ensureClusterMember(msg.getSenderId());

        int candidateTerm = msg.getTerm();

        if (candidateTerm > currentTerm) {
            currentTerm = candidateTerm;
            votedFor = null;
            becomeFollower(currentTerm, null);
        }

        boolean voteGranted = false;

        if (candidateTerm >= currentTerm) {
            boolean canVote = (votedFor == null || votedFor.equals(msg.getSenderId()));
            boolean logOk = isCandidateLogUpToDate(msg.getLastLogTerm(), msg.getLastLogIndex());

            if (canVote && logOk) {
                votedFor = msg.getSenderId();
                lastLeaderHeartbeatMs = System.currentTimeMillis();
                voteGranted = true;
            }
        }

        sendRequestVoteAck(msg.getSenderId(), voteGranted);
        ConsoleAppender.asyncMessage(String.format("[Raft %s] Vote for %s (term %d): %b",
                BROKER_ID, msg.getSenderId(), candidateTerm, voteGranted));
    }

    private void ensureClusterMember(String brokerId) {
        BrokerHandler handler = connectionRegistry.getBrokerHandler(brokerId);
        if (handler != null) {
            handler.markAsClusterMember();
        }
    }

    private boolean isCandidateLogUpToDate(int candidateLastTerm, int candidateLastIndex) {
        int myLastTerm = getLastLogTerm();
        int myLastIndex = getLastLogIndex();
        return candidateLastTerm > myLastTerm || (candidateLastTerm == myLastTerm && candidateLastIndex >= myLastIndex);
    }

    private void sendRequestVoteAck(String candidateId, boolean granted) {
        long seq = broker.getMessageCounter();
        RequestVoteAckMessage ack = new RequestVoteAckMessage(BROKER_ID, seq, System.currentTimeMillis(), currentTerm, granted);
        ConnectionHandler handler = connectionRegistry.getConnectionHandler(candidateId);
        if (handler != null) {
            handler.send(ack);
        }
    }

    public synchronized void handleRequestVoteAck(RequestVoteAckMessage msg) {
        ensureClusterMember(msg.getSenderId());

        if (msg.getTerm() > currentTerm) {
            currentTerm = msg.getTerm();
            votedFor = null;
            state = new FollowerState(broker);
            return;
        }

        if (state.getRole() != BrokerRole.CANDIDATE)
            return;

        if (msg.isVoteGranted()) {
            electionVotes++;
            int clusterSize = connectionRegistry.getClusterMembers().size() + 1;
            if (electionVotes > clusterSize / 2) {
                becomeLeader();
            }
        }
    }

    public synchronized void becomeLeader() {
        state = new LeaderState(broker);
        currentLeaderId = BROKER_ID;

        // Automatically resolves the actual LAN IP of this machine
        this.leaderHost = resolveAdvertisedHost();

        this.leaderPort = BROKER_PORT;
        this.leaderClientPort = CLIENT_PORT;

        int leaderLastIndex = getLastLogIndex();
        for (BrokerHandler handler : connectionRegistry.getClusterMembers()) {
            followerNextIndex.put(handler.getNodeId(), leaderLastIndex + 1);
            followerMatchIndex.put(handler.getNodeId(), 0);
        }

        ConsoleAppender.asyncMessage(String.format("[Raft %s] Became LEADER for term %d",
                BROKER_ID, currentTerm));
        broker.sendHeartbeat();
    }

    public synchronized void becomeFollower(int term, String leaderId) {
        if (term > currentTerm) {
            currentTerm = term;
            votedFor = null;
        }
        if (leaderId != null) {
            currentLeaderId = leaderId;
            BrokerHandler leaderHandler = connectionRegistry.getBrokerHandler(leaderId);
            if (leaderHandler != null) {
                this.leaderHost = leaderHandler.getRemoteHost();
                this.leaderPort = leaderHandler.getRemoteBrokerPort();
            }
        }
        electionVotes = 0;
        state = new FollowerState(broker);
        lastLeaderHeartbeatMs = System.currentTimeMillis();

        ConsoleAppender.asyncMessage(String.format("[Raft %s] Became FOLLOWER for term %d (Leader: %s)",
                BROKER_ID, currentTerm, leaderId));
    }

    public synchronized void replicateToFollowers(Message msg) throws Exception {
        Collection<BrokerHandler> clusterMembers = connectionRegistry.getClusterMembers();
        int newLogIndex = raftLog.size() + 1;
        raftLog.add(new LogEntry(currentTerm, msg));

        if (clusterMembers.isEmpty()) {
            commitIndex = newLogIndex;
            applyCommittedEntries();
            return;
        }

        for (BrokerHandler handler : clusterMembers) {
            sendAppendEntriesToFollower(handler.getNodeId());
        }
    }

    public synchronized void sendAppendEntriesToFollower(String followerId) {
        BrokerHandler handler = connectionRegistry.getBrokerHandler(followerId);
        if (handler == null) {
            return;
        }

        int nextInd = followerNextIndex.getOrDefault(followerId, raftLog.size() + 1);

        // If the requested index is <= 0, or if the requested entry was compacted (null command),
        // we have a log hole and must send the snapshot.
        if (nextInd <= 0 || (nextInd <= raftLog.size() && raftLog.get(nextInd - 1).getCommand() == null)) {
            try {
                sendInstallSnapshotToFollower(followerId);
            } catch (Exception e) {
                ConsoleAppender.asyncMessage(String.format("[Raft %s] [ERROR] Failed to send InstallSnapshot to %s: %s", BROKER_ID, followerId, e.getMessage()));
            }
            return;
        }

        if (nextInd > raftLog.size()) {
            return;
        }

        int prevLogIndex = nextInd - 1;
        int prevLogTerm = (prevLogIndex > 0) ? raftLog.get(prevLogIndex - 1).getTerm() : 0;
        LogEntry entry = raftLog.get(nextInd - 1);
        long seq = broker.getMessageCounter();
        AppendEntryMessage appendEntry = new AppendEntryMessage(
                BROKER_ID, seq, System.currentTimeMillis(),
                entry.getCommand(),
                prevLogIndex, prevLogTerm,
                currentTerm, commitIndex);
        handler.send(appendEntry);
    }

    public synchronized void handleAppendEntry(AppendEntryMessage msg) throws Exception {
        ensureClusterMember(msg.getSenderId());

        if (msg.getLeaderTerm() < currentTerm) {
            sendAppendEntryNack(msg, false);
            return;
        }
        updateLeaderHeartbeat();

        // If you are not a FOLLOWER (when entering in the "handleAppendEntry"), then you are a CANDIDATE
        if (msg.getLeaderTerm() > currentTerm || state.getRole() != BrokerRole.FOLLOWER) {
            becomeFollower(msg.getLeaderTerm(), msg.getSenderId());
        }

        ConsoleAppender.asyncMessage(String.format("[Raft %s] Received APPEND_ENTRY from %s", BROKER_ID, msg.getSenderId()));

        int prevLogIndex = msg.getPrevLogIndex();
        int prevLogTerm = msg.getPrevLogTerm();

        if (prevLogIndex > 0) {
            if (raftLog.size() < prevLogIndex) {
                sendAppendEntryNack(msg, false);
                return;
            }
            if (raftLog.get(prevLogIndex - 1).getTerm() != prevLogTerm) {
                raftLog.subList(prevLogIndex - 1, raftLog.size()).clear();
                sendAppendEntryNack(msg, false);
                return;
            }
        }

        int newEntryIndex = prevLogIndex + 1;
        if (raftLog.size() >= newEntryIndex) {
            raftLog.subList(newEntryIndex - 1, raftLog.size()).clear();
        }
        raftLog.add(new LogEntry(msg.getLeaderTerm(), msg.getMessage()));

        if (msg.getLeaderCommit() > commitIndex) {
            commitIndex = Math.min(msg.getLeaderCommit(), raftLog.size());
            applyFollowerCommittedEntries();
        }

        broker.appendToWal(msg.getMessage());
        sendAppendEntryNack(msg, true);
    }

    public synchronized void handleInstallSnapshot(InstallSnapshotMessage msg) throws Exception {
        ensureClusterMember(msg.getSenderId());

        // Reject obsolete terms
        if (msg.getLeaderTerm() < currentTerm) {
            sendInstallSnapshotAck(msg.getSenderId());
            return;
        }

        updateLeaderHeartbeat();

        // Step down if we were a candidate or have an older term
        if (msg.getLeaderTerm() > currentTerm || state.getRole() != BrokerRole.FOLLOWER) {
            becomeFollower(msg.getLeaderTerm(), msg.getSenderId());
        }

        // Ignora se lo snapshot è effettivamente più vecchio del nostro commit,
        // TRANNE se siamo appena nati (commitIndex == 0), nel qual caso
        // accettiamo qualsiasi cosa ci mandi il Leader legittimo.
        if (commitIndex > 0 && msg.getLastIncludedIndex() <= commitIndex) {
            sendInstallSnapshotAck(msg.getSenderId());
            return;
        }

        // Clear our obsolete in-memory log
        raftLog.clear();
        for (int i = 0; i < msg.getLastIncludedIndex(); i++) {
            raftLog.add(new LogEntry(msg.getLastIncludedTerm(), null));
        }

        commitIndex = msg.getLastIncludedIndex();
        lastApplied = msg.getLastIncludedIndex();

        broker.installStateFromSnapshot(msg.getStateMachineState());

        sendInstallSnapshotAck(msg.getSenderId());
    }

    private void sendInstallSnapshotAck(String leaderId) {
        long seq = broker.getMessageCounter();
        InstallSnapshotAckMessage ack = new InstallSnapshotAckMessage(
                BROKER_ID, seq, System.currentTimeMillis(),
                currentTerm, commitIndex
        );

        ConnectionHandler handler = connectionRegistry.getConnectionHandler(leaderId);
        if (handler != null) {
            handler.send(ack);
        }
    }

    public synchronized void handleAppendEntryAck(AppendEntryAckMessage msg) throws Exception {
        broker.updateHeartbeat(msg.getSenderId());
        String followerId = msg.getSenderId();

        if (!msg.isSuccess()) {
            int newNext = Math.max(1, msg.getAcknowledgedLogIndex() + 1);
            followerNextIndex.put(followerId, newNext);
            sendAppendEntriesToFollower(followerId);
            return;
        }

        int ackedIndex = msg.getAcknowledgedLogIndex();
        followerMatchIndex.merge(followerId, ackedIndex, Math::max);
        followerNextIndex.put(followerId, followerMatchIndex.get(followerId) + 1);

        tryCommit();
        sendAppendEntriesToFollower(followerId);
    }

    private void sendAppendEntryNack(AppendEntryMessage appendEntry, boolean success) {
        long seq = broker.getMessageCounter();
        AppendEntryAckMessage ack = new AppendEntryAckMessage(
                BROKER_ID, seq, System.currentTimeMillis(),
                success, raftLog.size());
        ConnectionHandler handler = connectionRegistry.getConnectionHandler(appendEntry.getSenderId());
        if (handler != null) handler.send(ack);
    }

    public synchronized void sendInstallSnapshotToFollower(String followerId) throws Exception {
        BrokerHandler handler = connectionRegistry.getBrokerHandler(followerId);
        if (handler == null) {
            return;
        }

        try {
            List<Message> currentState = broker.gatherStateForSnapshot();

            int lastIncIndex = commitIndex;
            int lastIncTerm = currentTerm;

            long seq = broker.getMessageCounter();
            InstallSnapshotMessage snapshotMsg = new InstallSnapshotMessage(
                    BROKER_ID, seq, System.currentTimeMillis(),
                    currentTerm, lastIncIndex, lastIncTerm, currentState
            );

            handler.send(snapshotMsg);

            // The follower sent the snapshot and the size was huge. The problem is that
            // it took the FOLLOWER millisecond to send the ACK back. This fix the bug
            followerNextIndex.put(followerId, commitIndex + 1);

            ConsoleAppender.asyncMessage(String.format("[Raft %s] Sending InstallSnapshot to lagging follower %s",
                    BROKER_ID, followerId));
        } catch (Exception e) {
            ConsoleAppender.asyncMessage(String.format("[Raft %s] [ERROR] Failed to send InstallSnapshot to %s: %s",
                    BROKER_ID, followerId, e.getMessage()));
        }

    }

    // Called on the LEADER when the follower successfully installed the snapshot
    public synchronized void handleInstallSnapshotAck(InstallSnapshotAckMessage msg) {
        broker.updateHeartbeat(msg.getSenderId());
        String followerId = msg.getSenderId();

        if (msg.getTerm() > currentTerm) {
            currentTerm = msg.getTerm();
            becomeFollower(currentTerm, null);
            return;
        }

        if (state.getRole() != BrokerRole.LEADER) return;

        // The follower has successfully jumped to the snapshot index.
        // Update its matchIndex and nextIndex accordingly.
        int ackedIndex = msg.getLastIncludedIndex();
        followerMatchIndex.merge(followerId, ackedIndex, Math::max);
        followerNextIndex.put(followerId, followerMatchIndex.get(followerId) + 1);

        // Immediately try to send any new AppendEntries that arrived while
        // the snapshot was transferring.
        sendAppendEntriesToFollower(followerId);
    }

    private synchronized void tryCommit() throws Exception {
        int clusterSize = connectionRegistry.getClusterMembers().size() + 1;
        int majority    = clusterSize / 2 + 1;

        for (int n = raftLog.size(); n > commitIndex; n--) {
            if (raftLog.get(n - 1).getTerm() != currentTerm) continue;

            int replicatedCount = 1;
            for (int mi : followerMatchIndex.values()) {
                if (mi >= n) replicatedCount++;
            }

            if (replicatedCount >= majority) {
                commitIndex = n;
                break;
            }
        }

        applyCommittedEntries();
    }

    private synchronized void applyCommittedEntries() throws Exception {
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry entry = raftLog.get(lastApplied - 1);

            broker.dispatchMessage(entry.getCommand());

            long seq = broker.getMessageCounter();
            CommitEntryMessage commit = new CommitEntryMessage(
                    BROKER_ID, seq, System.currentTimeMillis(),
                    entry.getCommand(), lastApplied);
            for (BrokerHandler handler : connectionRegistry.getClusterMembers()) {
                handler.send(commit);
            }
        }
    }

    public synchronized void commitEntry(int logIndex) {
        commitIndex = Math.max(commitIndex, logIndex);
        applyFollowerCommittedEntries();
    }

    private synchronized void applyFollowerCommittedEntries() {
        while (lastApplied < commitIndex && lastApplied < raftLog.size()) {
            lastApplied++;
            LogEntry entry = raftLog.get(lastApplied - 1);
            broker.applyToLocalState(entry.getCommand());
        }
    }

    public void createCluster() {
        if (state.getRole() != BrokerRole.SOLO) {
            ConsoleAppender.asyncMessage(String.format("[Raft %s] [ERROR] createCluster() called on a node that is not SOLO (current role: %s)",
                    BROKER_ID, state.getRole()));
            return;
        }

        this.clusterId = java.util.UUID.randomUUID().toString();
        this.currentTerm = 1;
        this.currentLeaderId = BROKER_ID;

        // Automatically resolves the actual LAN IP of this machine
        this.leaderHost = resolveAdvertisedHost();

        this.leaderPort = BROKER_PORT;
        this.leaderClientPort = CLIENT_PORT;

        this.state = new LeaderState(broker);
        ConsoleAppender.asyncMessage(String.format("[Raft %s] Cluster created (term=%d, role=LEADER)",
                BROKER_ID, currentTerm));
    }

    public void handleJoinClusterRequest(JoinClusterMessage msg) {
        // System.out.println("Handling Join request from:" + msg.getSenderId() + "recived by" + this.BROKER_ID);

        String requesterId = msg.getSenderId();

        if (!BROKER_ID.equals(currentLeaderId)) {
            long seq = broker.getMessageCounter();
            RedirectToLeaderMessage redirect = new RedirectToLeaderMessage(BROKER_ID, seq, System.currentTimeMillis(), currentLeaderId, leaderHost, leaderPort, leaderClientPort);
            broker.sendAck(requesterId, msg.getMessageId());
            connectionRegistry.getConnectionHandler(requesterId).send(redirect);
            return;
        }

        BrokerHandler requesterHandler = connectionRegistry.getBrokerHandler(requesterId);
        if (requesterHandler != null) {
            requesterHandler.markAsClusterMember();

            followerNextIndex.put(requesterId, commitIndex > 0 ? 1 : getLastLogIndex() + 1);
            followerMatchIndex.put(requesterId, 0);

            if (commitIndex > 0) {
                try {
                    sendInstallSnapshotToFollower(requesterId);
                } catch (Exception e) {
                    ConsoleAppender.asyncMessage("[Raft " + BROKER_ID + "] Error sending snapshot to new follower: " + e.getMessage());
                }
            }
        }

        followerNextIndex.put(requesterId, commitIndex > 0 ? 1 : getLastLogIndex() + 1);
        followerMatchIndex.put(requesterId, 0);

        List<BrokerAddress> peers = connectionRegistry.getConnectedBrokerHandlers()
                .stream()
                .filter(h -> !h.getNodeId().equals(requesterId))
                .map(h -> new BrokerAddress(h.getNodeId(), h.getRemoteHost(), h.getRemoteBrokerPort()))
                .collect(java.util.stream.Collectors.toList());

        long seq = broker.getMessageCounter();
        JoinClusterAckMessage joinAck = new JoinClusterAckMessage(
                BROKER_ID, seq, System.currentTimeMillis(),
                clusterId, currentTerm, peers,
                this.leaderHost,
                BROKER_PORT,
                CLIENT_PORT
        );
        connectionRegistry.getConnectionHandler(requesterId).send(joinAck);

        ConsoleAppender.asyncMessage(String.format("[Raft %s] Accepted %s into the cluster", BROKER_ID, requesterId));
    }

    public void handleJoinClusterAck(JoinClusterAckMessage msg) {
        this.clusterId = msg.getClusterId();
        this.currentTerm = msg.getCurrentTerm();
        this.currentLeaderId = msg.getSenderId();
        this.state = new FollowerState(broker);

        BrokerHandler leaderHandler = connectionRegistry.getBrokerHandler(msg.getSenderId());
        if (leaderHandler != null) leaderHandler.markAsClusterMember();

        // Prefer the address of the socket we are actually connected on;
        // fall back to the self-reported one only if that is unusable.
        String observed = (leaderHandler != null) ? leaderHandler.getRemoteHost() : null;
        this.leaderHost = isUsableHost(observed) ? observed : msg.getLeaderHost();

        this.leaderPort = msg.getLeaderPort();
        this.leaderClientPort = msg.getLeaderClientPort();

        this.votedFor = null;
        this.electionVotes = 0;
        this.lastLeaderHeartbeatMs = System.currentTimeMillis();

        broker.getExecutor().submit(() -> {
            for (BrokerAddress peer : msg.getPeerAddresses()) {
                if (!connectionRegistry.isBrokerConnected(peer.getBrokerId())) {
                    try {
                        connectionRegistry.getBrokerHandler(peer.getBrokerId());
                        BrokerHandler h = broker.connectToPeer(peer.getHost(), peer.getBrokerPort());
                        h.markAsClusterMember();
                    } catch (Exception e) {
                        ConsoleAppender.asyncMessage(String.format("[Raft %s] [ERROR] Failed to connect to peer %s: %s",
                                BROKER_ID, peer.getBrokerId(), e.getMessage()));
                    }
                }
            }
        });

        ConsoleAppender.asyncMessage(String.format("[Raft %s] Joined cluster (term=%d, leader=%s)",
                BROKER_ID, currentTerm, currentLeaderId));
    }

    private static boolean isUsableHost(String h) {
        if (h == null || h.isBlank()) return false;

        try {
            java.net.InetAddress a = java.net.InetAddress.getByName(h);
            return !a.isLoopbackAddress() && !a.isAnyLocalAddress();
        } catch (Exception e) {
            return false;
        }
    }

    public synchronized void restoreState(RaftMetadata metadata) {
        if (metadata == null) return;

        this.currentTerm  = metadata.getCurrentTerm();
        this.votedFor     = metadata.getVotedFor();
        this.commitIndex  = metadata.getCommitIndex();
        this.lastApplied  = metadata.getLastApplied();
        this.clusterId    = metadata.getClusterId();

        ConsoleAppender.asyncMessage(String.format("[Raft %s] State restored from snapshot (term=%d, commitIndex=%d)",
                BROKER_ID, currentTerm, commitIndex));
    }

    public void reset() {
        electionVotes = 0;
        votedFor = null;
        commitIndex = 0;
        lastApplied = 0;
        raftLog.clear();
        followerNextIndex.clear();
        followerMatchIndex.clear();
        lastLeaderHeartbeatMs = System.currentTimeMillis();
        state = new SoloState(broker);
    }

    // Getters
    public String getCurrentLeaderId() {
        return currentLeaderId;
    }

    public String getVotedFor() {
        return votedFor;
    }

    public String getClusterId() {
        return clusterId;
    }

    // determine the IP address that a server or message broker should "advertise" to external clients so they know how to connect to it
    private String resolveAdvertisedHost() {
        String configured = System.getProperty("broker.advertisedHost");
        if (configured != null && !configured.isBlank()) return configured;
        try {
            for (java.net.NetworkInterface ni : java.util.Collections.list(java.net.NetworkInterface.getNetworkInterfaces())) {
                if (!ni.isUp() || ni.isLoopback() || ni.isVirtual()) continue;
                for (java.net.InetAddress addr : java.util.Collections.list(ni.getInetAddresses())) {
                    if (addr instanceof java.net.Inet4Address && addr.isSiteLocalAddress())
                        return addr.getHostAddress();
                }
            }
        } catch (Exception ignored) { }
        return "127.0.0.1";
    }

    // Setters
    public synchronized void setLeaderClientPort(int port) {
        this.leaderClientPort = port;
    }

    // Clear memory
    public synchronized void clearLogInMemory() {
        for (int i = 0; i < commitIndex && i < raftLog.size(); i++) {
            LogEntry oldEntry = raftLog.get(i);
            if (oldEntry != null && oldEntry.getCommand() != null) {
                raftLog.set(i, new LogEntry(oldEntry.getTerm(), null));
            }
        }

        ConsoleAppender.asyncMessage(String.format("[Raft %s] RAM log compacted up to index %d",
                BROKER_ID, commitIndex));
    }
}