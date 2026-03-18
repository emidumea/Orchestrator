package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"orchestrator/internal/docker"
	"orchestrator/internal/models"
)

type WorkerAgent struct {
	Port string
	dm *docker.DockerManager 
}

func (wa *WorkerAgent) StartWorker() error {
	http.HandleFunc("/task/start", wa.HandleStartTask)

	fmt.Printf("Worker agent is listetning on port %s...\n", wa.Port)
	return http.ListenAndServe(wa.Port, nil)
}


func (wa *WorkerAgent) HandleStartTask(w http.ResponseWriter, r * http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var task models.Task
	err := json.NewDecoder(r.Body).Decode(&task)
	if err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	fmt.Printf("Worker agent received task: %s (Image %s)\n", task.ID, task.Image)

	containerConfig := docker.ContainerInfo {
		ContainerName: task.ContainerName,
		ImageName: task.Image,
	}

	containerID, err := wa.dm.StartContainer(context.Background(), containerConfig)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to start container: %v", err), http.StatusInternalServerError)
		return
	}

	// task.ContainerID = containerID
	// task.State = models.Running
	
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(fmt.Sprintf("Task started successfully. Container ID: %s", containerID)))

}