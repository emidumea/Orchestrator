package master

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
	"log"
	"github.com/google/uuid"

	"orchestrator/internal/gossip"
	"orchestrator/internal/models"
	"orchestrator/internal/store"
	"orchestrator/internal/middleware"

)
type Master struct {
	Port string
	Store *store.Store
	mu sync.Mutex
	Gossip *gossip.GossipManager
	Token string
}

func CreateMaster(port string, store *store.Store, token string) *Master {
	gossipManager := gossip.CreateGossipManager("master-node", ":8081", port, nil)

	m := &Master {
		Port: port,
		Store: store,
		Gossip: gossipManager,
		Token: token,
	}

	m.Gossip.OnNodeDown = m.handleNodeFailure

	return m
}

func (m *Master) StartMaster() error {
	mux := http.NewServeMux()

	m.ResetOrphanTasksOnBoot()
	m.Gossip.Start()
	go m.StartScheduler()

	mux.HandleFunc("/task/submit", middleware.Auth(m.Token, m.HandleSubmitTask))
	mux.HandleFunc("/tasks", middleware.Auth(m.Token, m.HandleGetAllTasks))
	mux.HandleFunc("/nodes", middleware.Auth(m.Token, m.HandleGetNodes))
	mux.HandleFunc("/task/complete", middleware.Auth(m.Token, m.handleCompleteTask))

	fs := http.FileServer(http.Dir("./web"))
	mux.Handle("/", fs)

	return http.ListenAndServe(m.Port, mux)
}

func (m *Master) HandleSubmitTask(w http.ResponseWriter, r * http.Request) {
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
	task.SubmittedAt = time.Now().UnixMilli()
	task.ID = uuid.NewString()

	task.State = models.Pending

	if task.ContainerName == "" {
		task.ContainerName = fmt.Sprintf("task-%s", task.ID[:8])
	}

	err = m.Store.SaveTask(task)
	if err != nil {
		http.Error(w, "Failed to save task", http.StatusInternalServerError)
		return
	}

	log.Printf("[Master] Received task %s and saved it successfully.\n", task.ID)

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(task)
}

func (m *Master) HandleGetAllTasks(w http.ResponseWriter, r * http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	tasks, err := m.Store.ListTasks()
	if err != nil {
		http.Error(w, "Failed to fetch all tasks", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tasks)
}

func (m *Master) ResetOrphanTasksOnBoot() {
	log.Println("[Master] Checking for orphan tasks on boot...")

	tasks, err := m.Store.ListTasks()
	if err != nil {
		log.Printf("[Master] Failed to fetch tasks on boot: %v\n", err)
		return
	}

	for _, task := range tasks {
		if task.State == models.Running {
			log.Printf("[Master] Found orphan task %s. Resetting to PENDING.\n", task.ID)
			task.State = models.Pending
			task.WorkerID = ""
			task.ContainerID = ""
			m.Store.SaveTask(task)
		}
	}
}

func (m *Master) HandleGetNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	nodes := m.Gossip.MemList.GetActiveMembers(m.Gossip.NodeID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(nodes)
}


func (m *Master) handleCompleteTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var report struct {
		TaskID string `json:"task_id"`
		ExitCode int64 `json:"exit_code"`
		WorkerID string `json:"worker_id"`
	}

	if err := json.NewDecoder(r.Body).Decode(&report); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	task, err := m.Store.GetTask(report.TaskID)
	if err != nil {
		http.Error(w, "Task not found", http.StatusNotFound)
		return
	}

	if report.ExitCode == 0 {
		task.State = models.Completed
	} else {
		task.State = models.Failed
	}

	if err := m.Store.SaveTask(*task); err != nil {
		http.Error(w, "Failed to update task", http.StatusInternalServerError)
		return
	}

	log.Printf("[Master] Task %s reported as %s (exit code %d)\n", report.TaskID[:8], task.State, report.ExitCode)

	w.WriteHeader(http.StatusOK)
}