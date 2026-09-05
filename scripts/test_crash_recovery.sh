#!/bin/bash

source "$(dirname "$0")/utils.sh"

setup_cluster

echo "[Test 3] Stopping broker3 brutally (SIGKILL)..."
docker kill broker3

echo "[Action] Starting automated Publisher on broker1..."
docker exec broker1 java -cp /app/app.jar it.polimi.pubsub.demos.AutomatedClient pub pub1 localhost 8080 ciao 20 200

echo "[Test 3] Restarting broker3..."
docker start broker3
echo "[Test 3] Waiting 10s for WAL recovery and Raft sync..."
sleep 10

echo "--- METRICS: CRASH RECOVERY ---"
RECOVERED=$(docker logs broker3 2>&1 | grep "State recovered from WAL" | wc -l)
SYNCED_SNAPSHOT=$(docker logs broker3 2>&1 | grep "Successfully installed distributed snapshot" | wc -l)
SYNCED_ENTRIES=$(docker logs broker3 2>&1 | grep "Received APPEND_ENTRY" | wc -l)

if [ "$RECOVERED" -gt 0 ]; then
    echo "Result: broker3 successfully read local WAL upon restart."
elif [ "$RECOVERED" -gt 0 ]; then
    echo "Result: broker3 successfully read local WAL upon restart."
else
    echo "Result: FAIL - broker3 did not read local WAL."
fi

if [ "$SYNCED_SNAPSHOT" -gt 0 ]; then
    echo "Result: broker3 caught up via Heavy Distributed Snapshot."
elif [ "$SYNCED_ENTRIES" -gt 0 ]; then
    echo "Result: broker3 caught up via Light AppendEntry synchronization ($SYNCED_ENTRIES packets)."
else
    echo "Result: FAIL - broker3 did not synchronize with the Leader."
fi

teardown_cluster