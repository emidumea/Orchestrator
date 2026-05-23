package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"log"

	"github.com/google/uuid"

	"orchestrator/internal/docker"
	"orchestrator/internal/models"
	"orchestrator/internal/gossip"
)

type WorkerAgent struct {
	Port string
	NodeID string
	dm *docker.DockerManager 
	Gossip *gossip.GossipManager
}

func CreateWorkerAgent(port string, manager *docker.DockerManager, gossipPort string, masterGossipAddr string) *WorkerAgent {
	agentID := uuid.NewString()

	gossipManager := gossip.CreateGossipManager(agentID, gossipPort, port)

	gossipManager.MemList.UpdateMember("master-node", masterGossipAddr, "", 0, 0)

	gossipManager.GetMetrics = getSystemMetrics
	
	return &WorkerAgent {
		NodeID: agentID,
		Port: port,
		dm: manager,
		Gossip: gossipManager,
	}
}

func (wa *WorkerAgent) StartWorker() error {
	wa.Gossip.Start()
	
	http.HandleFunc("/task/start", wa.HandleStartTask)
	http.HandleFunc("/task/stop", wa.HandleStopTask)

	log.Printf("[Worker %s] agent is listetning on port %s...\n", wa.NodeID[:8], wa.Port)
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
	log.Printf("[Worker %s] agent received task: %s (Image %s)\n", wa.NodeID[:8],task.ID, task.Image)

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
	
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(task)
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

	log.Printf("[Worker %s] agent received a request to stop the task %s (Image %s)", wa.NodeID[:8], task.ID, task.Image)

	err = wa.dm.StopContainer(context.Background(), task.ContainerName)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to stop container: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("Task stopped successfully. Container Name: %s", task.ContainerName)))

}