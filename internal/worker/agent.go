package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"orchestrator/internal/docker"
	"orchestrator/internal/gossip"
	"orchestrator/internal/middleware"
	"orchestrator/internal/models"
)

type RunningTask struct {
	ContainerID string
	ExecutionToken string
}

type WorkerAgent struct {
	Port string
	NodeID string
	MasterURL string
	dm *docker.DockerManager 
	Gossip *gossip.GossipManager
	Token string

	runningContainers map[string]RunningTask
	mu sync.Mutex
}

func CreateWorkerAgent(port string, manager *docker.DockerManager, gossipPort string, seedAddrs string, token string, masterURL string) *WorkerAgent {
	agentID := uuid.NewString()

	seeds := make([]string, 0)
	for _, seed := range strings.Split(seedAddrs, ",") {
		seed = strings.TrimSpace(seed)
		if seed == "" {
			continue
		}
		if _, _, err := net.SplitHostPort(seed); err != nil {
			log.Printf("[Worker] Invalid seed address '%s', skipping: %v", seed, err)
			continue
		}
		seeds = append(seeds, seed)
	}

	gossipManager := gossip.CreateGossipManager(agentID, gossipPort, port, seeds)

	gossipManager.GetMetrics = getSystemMetrics
	
	return &WorkerAgent {
		NodeID: agentID,
		Port: port,
		dm: manager,
		Gossip: gossipManager,
		Token: token,
		MasterURL: masterURL,
		runningContainers: make(map[string]RunningTask),
	}
}

func (wa *WorkerAgent) StartWorker() error {
	mux := http.NewServeMux()

	wa.Gossip.Start()
	go wa.reconcileLoop()
	
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
		Command: task.Command,
	}

	containerID, err := wa.dm.StartContainer(context.Background(), containerConfig)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to start container: %v", err), http.StatusInternalServerError)
		return
	}

	task.ContainerID = containerID
	wa.mu.Lock()
	wa.runningContainers[task.ID] = RunningTask{
		ContainerID: containerID,
		ExecutionToken: task.ExecutionToken,
	}
	wa.mu.Unlock()
	task.State = models.Running
	
	go wa.waitAndReport(task.ID, containerID)

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

	for taskID, rt := range wa.runningContainers {
		log.Printf("[Worker] Stopping container %s (task %s)\n", rt.ContainerID, taskID)
		err := wa.dm.StopContainer(context.Background(), rt.ContainerID)
		if err != nil {
			log.Printf("[Worker] Failed to stop container %s: %v\n", rt.ContainerID, err)
		}
	}
	log.Println("[Worker] Shutdown complete")
}

func (wa *WorkerAgent) waitAndReport(taskID, containerID string) {
	log.Printf("[Worker] waitAndReport started for task %s\n", taskID[:8])

	exitCode, err := wa.dm.WaitForContainer(context.Background(), containerID)
	
	if err != nil {
		log.Printf("[Worker] Error waiting for contaner %s: %v\n", containerID[:12], err)
		return
	}

	log.Printf("[Worker] Task %s finished with exit code %d\n", taskID[:8], exitCode)
	wa.mu.Lock()
	rt := wa.runningContainers[taskID]
	delete(wa.runningContainers, taskID)
	wa.mu.Unlock()

	wa.reportCompletion(taskID, exitCode, rt.ExecutionToken)
}

func (wa *WorkerAgent) reportCompletion(taskID string, exitCode int64, execToken string) {
	if wa.MasterURL == "" {
		return
	}

	payload := map[string]interface{}{
		"task_id": taskID,
		"exit_code": exitCode,
		"worker_id":wa.NodeID,
		"execution_token": execToken,
	}

	jsonData, _ := json.Marshal(payload)

	url := wa.MasterURL + "/task/complete"
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("[Worker] Failed to create completion request: %v\n", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+wa.Token)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[Worker] Failed to report completion for task %s: %v\n", taskID[:8], err)
		return
	}

	defer resp.Body.Close()

	log.Printf("[Worker] Reported completion for task %s (status %d)\n", taskID[:8], resp.StatusCode)
}


func (wa *WorkerAgent) reconcileLoop() {
	for {
		time.Sleep(10 * time.Second)

		wa.mu.Lock()
		type taskToken struct {
			TaskID string `json:"task_id"`
			ExecutionToken string `json:"execution_token"`
		}
		tasks := make([]taskToken, 0, len(wa.runningContainers))
		for taskID, rt := range wa.runningContainers {
			tasks = append(tasks, taskToken {
				TaskID: taskID,
				ExecutionToken: rt.ExecutionToken,
			})
		}

		wa.mu.Unlock()

		if len(tasks) == 0 {
			continue
		}

		// batching - one request including all the tasks
		payload := map[string]interface{}{
			"worker_id": wa.NodeID,
			"tasks": tasks,
		}

		jsonData, _ := json.Marshal(payload)

		url := wa.MasterURL + "/task/verify"
		req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
		if err != nil {
			continue
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+wa.Token)

		client := &http.Client{Timeout: 5 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("[Reconcile] Could not reach master: %v\n", err)
			continue
		}

		var result struct {
			InvalidTasks []string `json:"invalid_tasks"`
		}

		err = json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		if err != nil {
			continue
		}

		for _, taskID := range result.InvalidTasks {
			wa.mu.Lock()
			rt, exists := wa.runningContainers[taskID]
			wa.mu.Unlock()

			if !exists {
				continue
			}

			log.Printf("[Reconcile] Task %s no longer belongs to this worker. Stopping orphan container...\n", taskID[:8])

			if err := wa.dm.StopContainer(context.Background(), rt.ContainerID); err != nil {
				log.Printf("[Reconcile] Failed to stop orphan container: %v\n", err)
				continue
			}

			wa.mu.Lock()
			delete(wa.runningContainers, taskID)
			wa.mu.Unlock()
		}
	}
}