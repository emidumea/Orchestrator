package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"orchestrator/internal/models"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/joho/godotenv"
)

var masterURL string

type TaskRequest struct {
	Image   string   `json:"image"`
	Command []string `json:"command,omitempty"`
}

type TaskResponse struct {
	ID            string `json:"ID"`
	Image         string `json:"image"`
	State         string `json:"State"`
	WorkerID      string `json:"WorkerID"`
	ContainerName string `json:"ContainerName"`
}

type NodeResponse struct {
	ID      string `json:"id"`
	Address string `json:"ip"`
	APIPort string `json:"api_port"`
	State   string `json:"state"`
}

func printUsage() {
	fmt.Println("Orchestrator CLI")
	fmt.Println("\nUsage:")
	fmt.Println("    orch-cli <command> [args]")
	fmt.Println("\nAvailable commands:")
	fmt.Println("  submit  - Submit a new task to the master node")
	fmt.Println("  tasks   - List all the tasks from the system")
}

func getToken() string {
	err := godotenv.Load()
	if err != nil {
		log.Println("[Warning] No .env file found. Using default environment variables.")
	}

	token := os.Getenv("ORCHESTRATOR_TOKEN")
	if token == "" {
		log.Fatal("[Error] ORCHESTRATOR_TOKEN is not set in .env file.")
	}
	masterURL = os.Getenv("MASTER_URL")
	if masterURL == "" {
		masterURL = "http://localhost:3000"
	}

	fmt.Printf("DEBUG Token: '%s'\n", token)
	return token
}
func submitTask(image string, command []string) {
	token := getToken()

	taskReq := TaskRequest{
		Image:   image,
		Command: command,
	}

	jsonTask, err := json.Marshal(taskReq)
	if err != nil {
		fmt.Printf("An error occured while creating the JSON task: %v\n", err)
		return
	}

	fmt.Printf("Submitting task to master (Image: %s, Command: %v)\n", image, command)
	req, err := http.NewRequest("POST", masterURL+"/task/submit", bytes.NewBuffer(jsonTask))
	if err != nil {
		fmt.Printf("Failed to create HTTP request to master: %v\n", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Failed to submit task to master: %v\n", err)
		return
	}

	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusOK {
		fmt.Println("Task submitted successfully.")
		fmt.Printf("Response from master: %s\n", string(respBody))
	} else {
		fmt.Printf("Failed to submit task. (Status: %d). Response: %s\n", resp.StatusCode, string(respBody))
	}
}

func listTasks() {
	token := getToken()

	req, err := http.NewRequest("GET", masterURL+"/tasks", nil)
	if err != nil {
		fmt.Printf("Failed to create HTTP request to master: %v\n", err)
		return
	}

	req.Header.Set("Authorization", "Bearer "+token)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Failed to submit task to master: %v\n", err)
		return
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Printf("Failed to fetch tasks. Master returned status code: %d\n", resp.StatusCode)
		return
	}

	var tasks []TaskResponse

	if err := json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
		fmt.Printf("An error occured while reading the response from master: %v\n", err)
		return
	}

	if len(tasks) == 0 {
		fmt.Println("No tasks found.")
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "TASK ID\tIMAGE\tSTATE\tWORKER ID\tCONTAINER NAME")
	fmt.Fprintln(w, "-------\t-----\t-----\t---------\t--------------")

	for _, task := range tasks {
		shorterID := task.ID
		if len(task.ID) > 8 {
			shorterID = task.ID[:8]
		}

		workerID := task.WorkerID
		if workerID == "" {
			workerID = "N/A"
		}

		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", shorterID, task.Image, task.State, workerID, task.ContainerName)
	}

	w.Flush()
}

func listNodes() {
	token := getToken()

	req, err := http.NewRequest("GET", masterURL+"/nodes", nil)
	if err != nil {
		fmt.Printf("Failed to create HTTP request to master: %v\n", err)
		return
	}

	req.Header.Set("Authorization", "Bearer "+token)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Failed to submit task to master: %v\n", err)
		return
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Printf("Failed to fetch nodes. Master returned status code: %d\n", resp.StatusCode)
		return
	}

	var nodes []NodeResponse
	if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
		fmt.Printf("An error occured while reading the response from master: %v\n", err)
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "NODE ID\tADDRESS\tAPI PORT\tSTATE")
	fmt.Fprintln(w, "-------\t-------\t--------\t-----")

	for _, node := range nodes {
		shorterID := node.ID
		if len(node.ID) > 8 {
			shorterID = node.ID[:8]
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", shorterID, node.Address, node.APIPort, node.State)
	}
	w.Flush()
}


func handleExport() {
	token := getToken()

	req, _ := http.NewRequest("GET", masterURL+"/tasks", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("Failed to fetch tasks: %v", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Fatalf("Failed to get tasks. Status: %d", resp.StatusCode)
	}

	var tasks []models.Task
	json.NewDecoder(resp.Body).Decode(&tasks)

	file, err := os.Create("tasks_data.csv")
	if err != nil {
		log.Fatalf("Failed to create CSV file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	header := []string{
		"Task_ID", 
		"Image",
		"State",
		"Worker_ID",
		"SubmittedAt_ms",
		"ScheduledAt_ms",
		"startedAt_ms",
		"Wait_Latency_ms",
		"Boot_Latency_ms",
		"Execution_Time_ms",
	}

	writer.Write(header)
	count := 0
	for _, task := range tasks {
		if task.State == models.Running || task.State == models.Failed {
			waitLatency := task.ScheduledAt - task.SubmittedAt
			bootLatency := task.StartedAt - task.ScheduledAt
			totalLatency := task.StartedAt - task.SubmittedAt

			if waitLatency < 0 {
				waitLatency = 0
			}
			if bootLatency < 0 {
				bootLatency = 0
			}
			if totalLatency < 0 {
				totalLatency = 0
			}

			workerShortID := task.WorkerID
			if len(workerShortID) > 8 {
				workerShortID = workerShortID[:8]
			}

			row := []string {
				task.ID[:8],
				task.Image,
				string(task.State),
				workerShortID,
				fmt.Sprintf("%d", task.SubmittedAt),
				fmt.Sprintf("%d", task.ScheduledAt),
				fmt.Sprintf("%d", task.StartedAt),
				fmt.Sprintf("%d", waitLatency),
				fmt.Sprintf("%d", bootLatency),
				fmt.Sprintf("%d", totalLatency),
			}
			writer.Write(row)
			count++
		}
	}
	fmt.Printf("Exported %d tasks to 'tasks_data.csv'\n", count)
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "submit":
		submitCommand := flag.NewFlagSet("submit", flag.ExitOnError)
		img := submitCommand.String("image", "", "The Docker container image (ex: nginx, alpine)")
		cmd := submitCommand.String("cmd", "", "The command to run inside the container (ex: 'sleep 10')")

		submitCommand.Parse(os.Args[2:])

		var cmdList []string
		if *cmd != "" {
			cmdList = strings.Split(*cmd, " ")
		}

		submitTask(*img, cmdList)

	case "export":
		handleExport()

	case "tasks":
		listTasks()

	case "nodes":
		listNodes()

	default:
		fmt.Printf("Unknown command: '%s'\n\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}

}
