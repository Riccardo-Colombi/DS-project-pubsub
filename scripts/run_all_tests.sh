#!/bin/bash

echo "==================================================="
echo "      DISTRIBUTED SYSTEM TEST SUITE RUNNER         "
echo "==================================================="
echo ""

# Define the list of test scripts to execute
TESTS=(
    "test_packet_loss.sh"
    "test_leader_election.sh"
    "test_crash_recovery.sh"
)

# Move to the scripts directory to ensure correct relative paths
cd "$(dirname "$0")"

# Iterate over each test script
for TEST_FILE in "${TESTS[@]}"; do
    if [ -f "$TEST_FILE" ]; then
        echo -e "\n\n>>> STARTING SUITE: $TEST_FILE <<<\n"

        # Execute the test script
        bash "$TEST_FILE"

        echo -e "\n>>> COMPLETED SUITE: $TEST_FILE <<<\n"
        echo "---------------------------------------------------"
    else
        echo "[ERROR] Test file $TEST_FILE not found! Skipping..."
    fi
done

echo "==================================================="
echo "              ALL TESTS COMPLETED                  "
echo "==================================================="