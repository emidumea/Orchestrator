package master

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
	"log"

	"orchestrator/internal/gossip"
	"orchestrator/internal/models"
)

func (m *Master) StartScheduler() {
	log.Println("[Scheduler] Starting task scheduler...")

	time.Sleep(10 * time.Second)
	for {
		time.Sleep(3 * time.Second)

		tasks, err := m.Store.ListTasks()
		if err != nil {
			log.Printf("[Scheduler] An error occured while fetching tasks: %v", err)
			continue
		}

		activeWorkers := m.Gossip.MemList.GetActiveMembers(m.Gossip.NodeID)
		workerCount := len(activeWorkers)

		if workerCount == 0 {
			log.Println("[Scheduler] No worker nodes available at the moment.")
			continue
		}

		const estimatedMemCost uint64 = 50 // suppose each tasks needs at least 50MB of free memory
		const estimatedCPUCost float64 = 5.0 // suppose it needs at least 5% of free CPU

		for _, task := range tasks {
			if task.State == models.Pending {

				var bestWorker *gossip.Member
				var bestScore float64 = -1


				for i := range activeWorkers {
					worker := &activeWorkers[i]

					if worker.MemoryFree < estimatedMemCost {
						continue
					}

					score := float64(worker.MemoryFree) * 0.4 + worker.CPUFree * 0.6
					if score > bestScore {
						bestScore = score
						bestWorker = worker
	
					}
				}

				if bestWorker == nil {
					log.Printf("[Scheduler] Insufficient resources to schedule task %s. Will retry later.\n", task.ID)
					continue
				}


				log.Printf("[Scheduler] Assigning task %s to worker %s\n", task.ID, bestWorker.ID)

				err := m.dispatchTask(task, *bestWorker)
				if err != nil {
					log.Printf("[Scheduler] Failed to dispatch task %s: %v\n", task.ID, err)
					task.State = models.Failed
					m.Store.SaveTask(task)
					continue
				}
				
				for i := range activeWorkers {
					if activeWorkers[i].ID == bestWorker.ID {
						activeWorkers[i].MemoryFree -= estimatedMemCost
						activeWorkers[i].CPUFree -= estimatedCPUCost
						break
					}
				}
				

				for i := range tasks {
					if tasks[i].ID == task.ID {
						tasks[i].State = models.Running
						tasks[i].WorkerID = bestWorker.ID
						break
					}
				}


			}
		}

	}
}

func (m *Master) dispatchTask(task models.Task, worker gossip.Member) error {

	task.ContainerName = fmt.Sprintf("task-%s-%d", task.ID[:8], time.Now().Unix())
	
	jsonTask, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("[Scheduler] An error occured while creating the JSON with the task data: %v", err)
	}

	startURL := fmt.Sprintf("http://%s%s/task/start", worker.IP, worker.APIPort)
	

	req, err := http.NewRequest("POST", startURL, bytes.NewBuffer(jsonTask))
	if err != nil {
		return fmt.Errorf("[Scheduler] Failed to create HTTP request for task %s: %v\n", task.ID, err)
	}
	
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+m.Token)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("[Scheduler] Failed to schedule task %s to worker %s: %v\n", task.ID, worker.ID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusCreated {
		log.Printf("[Scheduler] Task %s scheduled to worker %s successfully:\n", task.ID, worker.ID)

		var updatedTask models.Task
		err := json.NewDecoder(resp.Body).Decode(&updatedTask)
		if err != nil {
			return fmt.Errorf("[Scheduler] An error occured while decoding the response from worker: %v", err)
		}
		
		updatedTask.WorkerID = worker.ID

		return m.Store.SaveTask(updatedTask)
	}

	bodyMsg, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("Worker returned status code: %d. Docker error: %s", resp.StatusCode, string(bodyMsg))

}



func (m * Master) handleNodeFailure(nodeID string) {
	log.Printf("[Fault-Tolerance] Node %s is dead. Recovering tasks...\n", nodeID)

	tasks, err := m.Store.ListTasks()
	if err != nil {
		log.Printf("[Fault-Tolerance] Failed to fetch tasks for recovery: %v\n", err)
		return
	}

	for _, task := range tasks {
		if task.WorkerID == nodeID && task.State == models.Running {
			log.Printf("[Fault-Tolerance] Recovering task %s\n", task.ID)

			task.State = models.Pending
			task.WorkerID = ""
			task.ContainerID = ""
			m.Store.SaveTask(task)
		}
	}
}