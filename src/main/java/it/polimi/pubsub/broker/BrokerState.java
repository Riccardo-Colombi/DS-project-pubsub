package it.polimi.pubsub.broker;

import it.polimi.pubsub.broker.components.BrokerRole;
import it.polimi.pubsub.messages.Message;

public interface BrokerState {

    BrokerRole getRole();

    /**
     * Gestisce un messaggio in arrivo.
     * Ogni stato decide se processarlo, rifiutarlo o redirigere.
     */
    void onMessageReceived(Message message) throws Exception;

    /**
     * Chiamato dallo scheduler quando scade l'election timer.
     * FOLLOWER → diventa CANDIDATE (fase 2)
     * LEADER   → no-op
     * SOLO     → no-op
     */
    void onElectionTimeout();

    /**
     * Chiamato dallo scheduler quando è ora di inviare heartbeat.
     * Solo il LEADER lo usa attivamente.
     */
    void onHeartbeatTick();
}