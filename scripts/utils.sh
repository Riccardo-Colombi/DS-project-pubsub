#!/bin/bash

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$PROJECT_ROOT/docker-compose.yml"

setup_cluster() {
    echo "[Setup] Starting cluster..."
    docker-compose -f $COMPOSE_FILE up -d
    echo "[Setup] Waiting 15s for Raft leader election..."
    sleep 15
}

teardown_cluster() {
    echo "[Teardown] Cleaning up cluster..."
    docker-compose -f $COMPOSE_FILE down -v
}