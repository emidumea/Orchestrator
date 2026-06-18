#!/bin/bash

NUM_TASKS=50
IMAGE="alpine"

echo "[Load test] Submitting $NUM_TASKS tasks..."
for i in $(seq 1 $NUM_TASKS); do
    ./orch-cli submit --image $IMAGE > /dev/null 2>&1 &
done

wait

echo "[Load test] Waiting for tasks to be processed..."
sleep 15

echo "[Load test] Exporting results..."
./orch-cli export

echo "[Load test] Done! Check tasks_export.csv"