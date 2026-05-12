package it.polimi.pubsub.broker.states;

import it.polimi.pubsub.broker.Broker;
import it.polimi.pubsub.broker.components.BrokerRole;
import it.polimi.pubsub.broker.BrokerState;
import it.polimi.pubsub.messages.Message;
import it.polimi.pubsub.messages.MessageType;

public class SoloState implements BrokerState {

    private final Broker broker;

    public SoloState(Broker broker) {
        this.broker = broker;
    }

    @Override
    public BrokerRole getRole() {
        return BrokerRole.SOLO;
    }

    @Override
    public void onMessageReceived(Message message) throws Exception {
        if (message.getType() == MessageType.JOIN_CLUSTER) {
            System.out.println("[SoloState] JoinClusterMessage ricevuto ma questo broker non è in un cluster. Messaggio scartato.");
            return;
        }

        broker.dispatchMessage(message);
    }


    @Override
    public void onElectionTimeout() {

    }

    @Override
    public void onHeartbeatTick() {

    }
}