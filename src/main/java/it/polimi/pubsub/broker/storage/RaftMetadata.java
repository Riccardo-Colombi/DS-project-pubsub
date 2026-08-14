package it.polimi.pubsub.broker.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Persistent Raft state snapshot, saved next to the WAL each time
 * an application state snapshot is taken.
 *
 * currentTerm and votedFor are the fields whose loss is dangerous: without
 * them, a restarted node could vote twice in the same term,
 * violating Raft's "at most one leader per term" guarantee.
 *
 * commitIndex and lastApplied are included to speed up recovery (avoid
 * having to redo the application replay from scratch), but their loss is not
 * dangerous for correctness.
 */
public class RaftMetadata {

    private final int currentTerm;
    private final String votedFor;
    private final int commitIndex;
    private final int lastApplied;

    @JsonCreator
    public RaftMetadata(
            @JsonProperty("currentTerm") int currentTerm,
            @JsonProperty("votedFor") String votedFor,
            @JsonProperty("commitIndex") int commitIndex,
            @JsonProperty("lastApplied") int lastApplied) {

        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
        this.commitIndex = commitIndex;
        this.lastApplied = lastApplied;
    }

    public int getCurrentTerm()  { return currentTerm; }
    public String getVotedFor()  { return votedFor; }
    public int getCommitIndex()  { return commitIndex; }
    public int getLastApplied()  { return lastApplied; }
}