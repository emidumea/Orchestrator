package main

import (
	"context"
	"fmt"
	"log"

	"orchestrator/internal/docker"
)

func main() {
	manager, err := docker.CreateDockerManager()
	if err != nil {
		log.Fatalf("Failed to create docker manager: %v", err)
	}

	taskConfig := docker.ContainerInfo{ContainerName: "test-nginx", ImageName: "nginx:latest"}

	fmt.Printf("Trying to download and start image %s...\n", taskConfig.ImageName)
	containerID, err := manager.StartContainer(context.Background(), taskConfig)
	if err != nil {
		log.Fatalf("An error occured while starting container: %v", err)

	}

	fmt.Printf("Container started successfully. ID: %s\n", containerID)
}
