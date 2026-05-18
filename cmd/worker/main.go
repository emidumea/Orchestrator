package main

import (
	"flag"

	"log"

	"orchestrator/internal/docker"
	"orchestrator/internal/worker"
)

func main() {
	apiPort := flag.String("api", ":8080", "HTTP port for worker agent")
	gossipPort := flag.String("gossip", ":8082", "UDP port for gossip communication")
	masterAddr := flag.String("master", "localhost:8081", "Gossip address of the seed (master) node")

	flag.Parse()

	manager, err := docker.CreateDockerManager()
	if err != nil {
		log.Fatalf("Failed to create docker manager: %v", err)
	}

	workerAgent := worker.CreateWorkerAgent(*apiPort, manager, *gossipPort, *masterAddr)

	if err := workerAgent.StartWorker(); err != nil {
		log.Fatalf("Error starting worker agent: %v", err)
	}

}