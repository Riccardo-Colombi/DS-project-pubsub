package it.polimi.pubsub.broker.components;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class BrokerAddress {

    private final String brokerId;
    private final String host;
    private final int brokerPort;

    @JsonCreator
    public BrokerAddress(
            @JsonProperty("brokerId") String brokerId,
            @JsonProperty("host") String host,
            @JsonProperty("brokerPort") int brokerPort) {

        this.brokerId   = brokerId;
        this.host       = host;
        this.brokerPort = brokerPort;

    }

    public String getBrokerId() {
        return brokerId;
    }

    public String getHost() {
        return host;
    }

    public int getBrokerPort() {
        return brokerPort;
    }

}