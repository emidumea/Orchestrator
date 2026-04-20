package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"orchestrator/internal/master"
	"orchestrator/internal/models"
	"orchestrator/internal/store"
)

func main() {
	fmt.Println("Starting master node (testing)...")

	dbStore, err := store.CreateStore("master.db")
	if err != nil {
		log.Fatalf("Failed to create the store: %v", err)
	}

	m := master.CreateMaster(":3000", dbStore)
	go func() {
		if err := m.StartMaster(); err != nil {
			log.Fatalf("Failed to start master: %v", err)
		}
	}()

	fmt.Println("Master node is running on port 3000... Waiting for worker nodes.")
	time.Sleep(10 * time.Second)

	task := models.Task{
		Image:         "nginx:latest",
		//ContainerName: "nginx-node",
	}

	jsonTask, err := json.Marshal(task)
	if err != nil {
		log.Fatalf("An error occured while creating the JSON: %v", err)
	}

	submitURL := "http://localhost:3000/task/submit"
	respSubmit, err := http.Post(submitURL, "application/json", bytes.NewBuffer(jsonTask))
	if err != nil {
		log.Fatalf("Failed to submit task to master. Error: %v", err)
	}
	defer respSubmit.Body.Close()

	if respSubmit.StatusCode == http.StatusCreated {
		fmt.Println("Task submitted to master successfully. The task is now pending and waiting to be scheduled.")
	}

	select {}
}
