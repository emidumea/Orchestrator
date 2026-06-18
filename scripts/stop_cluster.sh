#!/bin/bash
pkill -f orch-master
pkill -f orch-worker
docker stop $(docker ps -q) 2>/dev/null
echo "[Cluster] Stopped."