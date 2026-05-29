package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"net"
	"github.com/google/uuid"

	"orchestrator/internal/docker"
	"orchestrator/internal/gossip"
	"orchestrator/internal/middleware"
	"orchestrator/internal/models"
)

type WorkerAgent struct {
	Port string
	NodeID string
	dm *docker.DockerManager 
	Gossip *gossip.GossipManager
	Token string

	runningContainers map[string]string
	mu sync.Mutex
}

func CreateWorkerAgent(port string, manager *docker.DockerManager, gossipPort string, masterGossipAddr string, token string) *WorkerAgent {
	agentID := uuid.NewString()

	gossipManager := gossip.CreateGossipManager(agentID, gossipPort, port)

	masterIP, masterPortRaw, err := net.SplitHostPort(masterGossipAddr)
	if err != nil {
		log.Fatalf("[Worker] Master address is invalid: %v", err)
	}
	masterPort := ":" + masterPortRaw
	gossipManager.MemList.UpdateMember("master-node", masterIP, masterPort, "", 0, 0)

	gossipManager.GetMetrics = getSystemMetrics
	
	return &WorkerAgent {
		NodeID: agentID,
		Port: port,
		dm: manager,
		Gossip: gossipManager,
		Token: token,
		runningContainers: make(map[string]string),
	}
}

func (wa *WorkerAgent) StartWorker() error {
	mux := http.NewServeMux()

	wa.Gossip.Start()
	
	mux.HandleFunc("/task/start", middleware.Auth(wa.Token, wa.HandleStartTask))
	mux.HandleFunc("/task/stop", middleware.Auth(wa.Token, wa.HandleStopTask))

	log.Printf("[Worker %s] agent is listetning on port %s...\n", wa.NodeID[:8], wa.Port)
	return http.ListenAndServe(wa.Port, mux)
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
	wa.mu.Lock()
	wa.runningContainers[task.ID] = containerID
	wa.mu.Unlock()
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

func (wa *WorkerAgent) Shutdown() {
	wa.mu.Lock()
	defer wa.mu.Unlock()

	log.Println("[Worker] Stopping all active containers...")

	for taskID, containerID := range wa.runningContainers {
		log.Printf("[Worker] Stopping container %s (task %s)\n", containerID, taskID)
		err := wa.dm.StopContainer(context.Background(), containerID)
		if err != nil {
			log.Printf("[Worker] Failed to stop container %s: %v\n", containerID, err)
		}
	}
	log.Println("[Worker] Shutdown complete")
}