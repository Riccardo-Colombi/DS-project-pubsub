#!/bin/bash

source "$(dirname "$0")/utils.sh"

setup_cluster

echo "[Test 2] Injecting 100% packet loss on broker1 to simulate partition..."
docker exec --privileged broker1 tc qdisc add dev eth0 root netem loss 100%

echo "[Test 2] Waiting 25s for election timeout and split-vote resolution..."
sleep 25

echo "--- METRICS: LEADER ELECTION ---"
B2_ELECTED=$(docker logs broker2 2>&1 | grep "Became LEADER" | wc -l)
B3_ELECTED=$(docker logs broker3 2>&1 | grep "Became LEADER" | wc -l)

if [ "$B2_ELECTED" -gt 0 ]; then
    echo "Result: broker2 successfully detected the partition and became the new LEADER."
elif [ "$B3_ELECTED" -gt 0 ]; then
    echo "Result: broker3 successfully detected the partition and became the new LEADER."
else
    echo "Result: FAIL - No new leader was elected."
fi

teardown_cluster