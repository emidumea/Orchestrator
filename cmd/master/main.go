package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"orchestrator/internal/models"
)

func main() {
	fmt.Println("Starting master node (testing)...")

	task := models.Task{
		ID:            "task-test-1",
		Image:         "nginx:latest",
		ContainerName: "nginx-from-master",
		State:         models.Scheduled,
	}

	jsonTask, err := json.Marshal(task)
	if err != nil {
		log.Fatalf("An error occured while creating the JSON: %v", err)
	}

	startURL := "http://localhost:8080/task/start"
	respStart, err := http.Post(startURL, "application/json", bytes.NewBuffer(jsonTask))
	if err != nil {
		log.Fatalf("Failed to create resource at: %s. Error: %v)", startURL, err)
	}

	if respStart.StatusCode == http.StatusCreated {
		fmt.Println("Task created successfully.")
	}

	respStart.Body.Close()

	fmt.Println("Sendind stop request in 5 seconds...")
	time.Sleep(5 * time.Second)

	stopURL := "http://localhost:8080/task/stop"
	respStop, err := http.Post(stopURL, "application/json", bytes.NewBuffer(jsonTask))
	if err != nil {
		log.Fatalf("Failed to create resource at: %s. Error: %v)", stopURL, err)
	}
	defer respStop.Body.Close()

	if respStop.StatusCode == http.StatusOK {
		fmt.Println("Task stopped successfully.")
	}
}
