package master

import (
	"fmt"
	"time"
	"encoding/json"
	"net/http"
	"bytes"

	"orchestrator/internal/models"
	"orchestrator/internal/gossip"
)

func (m *Master) StartScheduler() {
	fmt.Println("[Scheduler] Starting task scheduler...")

	for {
		time.Sleep(3 * time.Second)

		tasks, err := m.Store.ListTasks()
		if err != nil {
			fmt.Printf("[Scheduler] An error occured while fetching tasks: %v", err)
			continue
		}

		activeWorkers := m.Gossip.MemList.GetActiveMembers(m.Gossip.NodeID)
		workerCount := len(activeWorkers)

		if workerCount == 0 {
			fmt.Println("[Scheduler] No worker nodes available at the moment.")
			continue
		}

		for _, task := range tasks {
			if task.State == models.Pending {

				assignedWorker := activeWorkers[0]


				fmt.Printf("[Scheduler] Assigning task %s to worker %s\n", task.ID, assignedWorker.ID)

				err := m.dispatchTask(task, assignedWorker)
				if err != nil {
					fmt.Printf("[Scheduler] Failed to dispatch task %s: %v\n", task.ID, err)
					continue
				}
			}
		}

	}
}

func (m *Master) dispatchTask(task models.Task, worker gossip.Member) error {

	jsonTask, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("[Scheduler] An error occured while creating the JSON with the task data: %v", err)
	}

	startURL := "http://" + worker.APIPort + "/task/start"
	resp, err := http.Post(startURL, "application/json", bytes.NewBuffer(jsonTask))
	if err != nil {
		return fmt.Errorf("[Scheduler] Failed to schedule task %s to worker %s: %v\n", task.ID, worker.ID, err)
	}
	
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusCreated {
		fmt.Printf("[Scheduler] Task %s scheduled to worker %s successfully:\n", task.ID, worker.ID)

		var updatedTask models.Task
		err := json.NewDecoder(resp.Body).Decode(&updatedTask)
		if err != nil {
			return fmt.Errorf("[Scheduler] An error occured while decoding the response from worker: %v", err)
		}
		
		updatedTask.WorkerID = worker.ID

		return m.Store.SaveTask(updatedTask)
	}

	return fmt.Errorf("Worker returned status code: %d", resp.StatusCode)

}
