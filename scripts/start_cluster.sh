#!/bin/bash

NUM_WORKERS=${1:-5}

echo "[Cluster] Building binaries..."
go build -o orch-master cmd/master/main.go
go build -o orch-worker cmd/worker/main.go

echo "[Cluster] Starting master..."
./orch-master &
sleep 5

echo "[Cluster] Starting $NUM_WORKERS workers..."
for i in $(seq 1 $NUM_WORKERS); do
    api_port=$((8082 + i))
    gossip_port=$((9082 + i))
    ./orch-worker -api ":$api_port" -gossip ":$gossip_port" &
    sleep 0.3
done

echo "[Cluster] Ready. Master + $NUM_WORKERS workers."
echo "[Cluster] Logs in logs/ directory."