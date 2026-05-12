package it.polimi.pubsub.messages.brokerMessages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.polimi.pubsub.messages.Message;
import it.polimi.pubsub.messages.MessageType;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RedirectToLeaderMessage extends Message {

    private final String leaderHost;
    private final int leaderPort;

    @JsonCreator
    public RedirectToLeaderMessage(
            @JsonProperty("senderId") String senderId,
            @JsonProperty("sequenceNumber") long sequenceNumber,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("leaderHost") String leaderHost,
            @JsonProperty("leaderPort") int leaderPort) {
        super(senderId, sequenceNumber, timestamp);
        this.leaderHost = leaderHost;
        this.leaderPort = leaderPort;
    }

    @JsonProperty("leaderHost")
    public String getLeaderHost() { return leaderHost; }

    @JsonProperty("leaderPort")
    public int getLeaderPort() { return leaderPort; }


    @Override
    public MessageType getType() {
        return MessageType.REDIRECT_TO_LEADER;
    }
}