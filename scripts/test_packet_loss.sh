#!/bin/bash

source "$(dirname "$0")/utils.sh"

setup_cluster

echo "[Test 1] Injecting 30% packet loss on broker2..."
# docker exec --privileged broker2: esegue un comando nel container 'broker2' con permessi di amministratore root.
# tc qdisc add dev eth0 root netem loss 30%:
#   - tc (Traffic Control): utility di Linux per gestire il traffico di rete.
#   - qdisc (Queueing Discipline): l'algoritmo che decide come accodare i pacchetti.
#   - dev eth0: l'interfaccia di rete bersaglio all'interno del container.
#   - root: applica la regola alla radice dell'albero di uscita, colpendo tutto il traffico.
#   - netem (Network Emulator): il modulo kernel per simulare reti WAN (ritardi, perdite, duplicazioni).
#   - loss 30%: istruisce il kernel a scartare casualmente il 30% dei pacchetti in uscita.
docker exec --privileged broker2 tc qdisc add dev eth0 root netem loss 30%

echo "[Action] Starting automated Subscriber on broker3 (in background)..."
# L'opzione '-d' (detach) fa sì che il processo venga eseguito in background. In questo modo
# lo script bash non si blocca ad aspettare il subscriber, ma passa subito alla riga successiva.
docker exec -d broker3 java -cp /app/app.jar it.polimi.pubsub.demos.AutomatedClient sub sub1 localhost 8082 ciao 10000

echo "[Action] Starting automated Publisher on broker1..."
# Omettendo '-d', lo script aspetterà che questo comando finisca.
# Il publisher invierà 20 messaggi con una pausa di 200 millisecondi tra l'uno e l'altro.
docker exec broker1 java -cp /app/app.jar it.polimi.pubsub.demos.AutomatedClient pub pub1 localhost 8080 ciao 20 200

echo "[Action] Publisher finished. Waiting 3 seconds for cluster synchronization..."
sleep 3

SENT=$(docker logs broker1 2>&1 | grep "Received PUBLISH" | wc -l)
REP_B2=$(docker logs broker2 2>&1 | grep "APPEND_ENTRY" | wc -l)
REP_B3=$(docker logs broker3 2>&1 | grep "APPEND_ENTRY" | wc -l)

echo "--- METRICS: PACKET LOSS ---"
echo "Leader processed: $SENT"
echo "Broker2 (30% Loss) replication attempts: $REP_B2"
echo "Broker3 (0% Loss) replication attempts: $REP_B3"

if [ "$SENT" -gt 0 ]; then
    SUCCESS_RATE=$(( (REP_B2 * 100) / SENT ))
    echo "Broker2 Replication Rate: $SUCCESS_RATE% (Target: >100% due to Raft retries)"
fi

teardown_cluster