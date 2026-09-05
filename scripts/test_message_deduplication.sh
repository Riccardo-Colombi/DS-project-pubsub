#!/bin/bash

source "$(dirname "$0")/utils.sh"

setup_cluster

echo "[Test 4] Injecting 30% packet duplication and 200ms delay on broker2..."
# Il parametro "duplicate 30%" ordina al kernel di clonare il 30% dei pacchetti in uscita.
# Il delay aggiunto serve a sfasare l'arrivo dei cloni, rendendo più difficile il lavoro al ricevitore.
docker exec --privileged broker2 tc qdisc add dev eth0 root netem duplicate 30% delay 200ms

echo "[Action] Starting automated Subscriber on broker3 (in background)..."
docker exec -d broker3 java -cp /app/app.jar it.polimi.pubsub.demos.AutomatedClient sub sub1 localhost 8082 ciao 10000

echo "[Action] Starting automated Publisher on broker1..."
docker exec broker1 java -cp /app/app.jar it.polimi.pubsub.demos.AutomatedClient pub pub1 localhost 8080 ciao 20 200

echo "[Action] Publisher finished. Waiting 3 seconds..."
sleep 3

echo "--- METRICS: DUPLICATION MINIMIZATION ---"
B2_PROCESSED=$(docker logs broker2 2>&1 | grep "Received APPEND_ENTRY" | wc -l)

echo "Leader published: 20 messages"
echo "Broker2 processed: $B2_PROCESSED APPEND_ENTRY"

if [ "$B2_PROCESSED" -le 25 ]; then
    echo "Result: PASS - System successfully minimized duplicated packets at application level."
else
    echo "Result: WARNING - High number of duplicated messages processed ($B2_PROCESSED)."
fi

teardown_cluster