#!/bin/bash

source "$(dirname "$0")/utils.sh"

setup_cluster

echo "[Test 5] Injecting 15% packet corruption on broker2..."
# Il parametro "corrupt 15%" inverte casualmente un singolo bit in un offset random del pacchetto
# per il 15% del traffico in uscita.
docker exec --privileged broker2 tc qdisc add dev eth0 root netem corrupt 15%

echo "[Action] Starting automated Publisher on broker1..."
docker exec broker1 java -cp /app/app.jar it.polimi.pubsub.demos.AutomatedClient pub pub1 localhost 8080 ciao 20 200

echo "[Action] Publisher finished. Waiting 5 seconds for retries..."
sleep 5

echo "--- METRICS: PACKET CORRUPTION ---"
B2_DESERIALIZATION_ERRORS=$(docker logs broker2 2>&1 | grep "Deserialization failed\|Corrupted" | wc -l)
B2_APPENDS=$(docker logs broker2 2>&1 | grep "Received APPEND_ENTRY" | wc -l)

echo "Broker2 handled $B2_DESERIALIZATION_ERRORS corrupted payloads gracefully."
echo "Broker2 eventually processed $B2_APPENDS correct entries via retries."

teardown_cluster