#!/bin/bash
source "$(dirname "$0")/utils.sh"

setup_cluster

echo "[Test 6] Starting automated Subscriber on broker3 (Durable)..."
docker exec -d broker3 java -cp /app/app.jar it.polimi.pubsub.demos.AutomatedClient sub sub1 localhost 8082 SystemStatus 10000

echo "[Test 6] Injecting 100% packet loss on broker1 to simulate partition..."
docker exec --privileged broker1 tc qdisc replace dev eth0 root netem loss 100%
sleep 8

echo "[Action] Starting automated Publisher on broker1 (Isolated)..."
docker exec broker1 java -cp /app/app.jar it.polimi.pubsub.demos.AutomatedClient pub pub1 localhost 8080 SystemStatus 5 200

echo "[Action] Removing partition (healing the network)..."
docker exec --privileged broker1 tc qdisc del dev eth0 root
sleep 8

echo "--- METRICS: PARTITION SURVIVAL ---"
# Check if the durable subscriber eventually received the messages after healing
RECEIVED=$(docker logs broker3 2>&1 | grep "SystemStatus" | wc -l)

if [ "$RECEIVED" -gt 0 ]; then
    echo "Result: PASS - Subscriber received offline messages after network healed."
else
    echo "Result: FAIL - Partition caused data loss."
fi

teardown_cluster