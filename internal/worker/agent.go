package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/google/uuid"

	"orchestrator/internal/docker"
	"orchestrator/internal/models"
)

type WorkerAgent struct {
	Port string
	NodeID string
	dm *docker.DockerManager 
}

func CreateWorkerAgent(port string, manager *docker.DockerManager) *WorkerAgent {
	return &WorkerAgent {
		NodeID: uuid.NewString(),
		Port: port,
		dm: manager,
	}
}

func (wa *WorkerAgent) RegisterToMaster(masterAddress string) error {

	node := models.Node {
		ID: wa.NodeID,
		Address: "http://localhost" + wa.Port,
	}

	jsonNode, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("An error occured while creating the JSON with the worker node data: %v", err)
	}

	resp, err := http.Post(masterAddress+"/worker/register", "application/json", bytes.NewBuffer(jsonNode))
	if err != nil {
		return fmt.Errorf("Failed to register worker node to master: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusCreated {
		fmt.Printf("Registered to master (ID: %s)\n", node.ID)
	}
	return nil
}


func (wa *WorkerAgent) StartWorker() error {
	http.HandleFunc("/task/start", wa.HandleStartTask)
	http.HandleFunc("/task/stop", wa.HandleStopTask)

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

	task.ContainerID = containerID
	task.State = models.Running
	
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(fmt.Sprintf("Task started successfully. Container ID: %s", containerID)))

}

func (wa* WorkerAgent) HandleStopTask(w http.ResponseWriter, r *http.Request) {
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

	fmt.Printf("Worker agent received a request to stop the task %s (Image %s)", task.ID, task.Image)

	err = wa.dm.StopContainer(context.Background(), task.ContainerName)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to stop container: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("Task stopped successfully. Container Name: %s", task.ContainerName)))

}