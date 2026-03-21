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
	if err := workerAgent.StartWorker(); err != nil {
		log.Fatalf("Error starting worker agent: %v", err)
	}
	//taskConfig := docker.ContainerInfo{ContainerName: "test-nginx", ImageName: "nginx:latest"}

	//fmt.Printf("Trying to download and start image %s...\n", taskConfig.ImageName)
	//containerID, err := manager.StartContainer(context.Background(), taskConfig)
	//if err != nil {
	//	log.Fatalf("An error occured while starting container: %v", err)

	//}

	//fmt.Printf("Container started successfully. ID: %s\n", containerID)
}
