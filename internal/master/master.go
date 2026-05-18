package master

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"

	"orchestrator/internal/gossip"
	"orchestrator/internal/models"
	"orchestrator/internal/store"
)
type Master struct {
	Port string
	Store *store.Store
	WorkerNodes map[string]*models.Node
	mu sync.Mutex
	Gossip *gossip.GossipManager
}

func CreateMaster(port string, store *store.Store) *Master {
	gossipManager := gossip.CreateGossipManager("master-node", ":8081", port)

	m := &Master {
		Port: port,
		Store: store,
		WorkerNodes: make(map[string]*models.Node),
		Gossip: gossipManager,
	}

	m.Gossip.OnNodeDown = m.handleNodeFailure

	return m
}

func (m *Master) StartMaster() error {
	m.ResetOrphanTasksOnBoot()
	m.Gossip.Start()
	go m.StartScheduler()

	http.HandleFunc("/worker/register", m.HandleRegisterWorkerNode)

	http.HandleFunc("/task/submit", m.HandleSubmitTask)
	http.HandleFunc("/tasks", m.HandleGetAllTasks)
	http.HandleFunc("/nodes", m.HandleGetNodes)
	return http.ListenAndServe(m.Port, nil)
}

func (m *Master) HandleRegisterWorkerNode(w http.ResponseWriter, r * http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var node models.Node

	err := json.NewDecoder(r.Body).Decode(&node)
	if err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	node.State = models.NodeActive
	node.LastSeen = time.Now()

	m.mu.Lock()
	m.WorkerNodes[node.ID] = & node
	m.mu.Unlock()


	fmt.Printf("[Master] Registered worker node: %s (Address: %s)\n", node.ID, node.Address)


	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(node)

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

	fmt.Printf("[Master] Received task %s and saved it successfully.\n", task.ID)

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
	fmt.Println("[Master] Checking for orphan tasks on boot...")

	tasks, err := m.Store.ListTasks()
	if err != nil {
		fmt.Printf("[Master] Failed to fetch tasks on boot: %v\n", err)
		return
	}

	for _, task := range tasks {
		if task.State == models.Running {
			fmt.Printf("[Master] Found orphan task %s. Resetting to PENDING.\n", task.ID)
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
