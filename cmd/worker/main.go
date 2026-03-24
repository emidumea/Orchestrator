package main

import (
	"log"

	"orchestrator/internal/docker"
	"orchestrator/internal/worker"
)

func main() {
	manager, err := docker.CreateDockerManager()
	if err != nil {
		log.Fatalf("Failed to create docker manager: %v", err)
	}

	workerAgent := worker.CreateWorkerAgent(":8080", manager)
	err = workerAgent.RegisterToMaster("http://localhost:3000")
	if err != nil {
		log.Fatalf("Failed to register to master: %v", err)
	}
	
	if err := workerAgent.StartWorker(); err != nil {
		log.Fatalf("Error starting worker agent: %v", err)
	}

}
