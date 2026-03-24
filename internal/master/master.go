package master

import (
	"fmt"
	"net/http"
	"encoding/json"
	"sync"
	"time"

	"orchestrator/internal/models"
	"orchestrator/internal/store"

)
type Master struct {
	Port string
	Store *store.Store
	WorkerNodes map[string]*models.Node
	mu sync.Mutex

}

func CreateMaster(port string, store *store.Store) *Master {
	return &Master {
		Port: port,
		Store: store,
		WorkerNodes: make(map[string]*models.Node),
	}
}

func (m *Master) StartMaster() error {
	http.HandleFunc("/worker/register", m.HandleRegisterWorkerNode)

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

	//node.ID = uuid.NewString()
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