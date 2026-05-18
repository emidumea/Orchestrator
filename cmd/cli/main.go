package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"text/tabwriter"
)

const masterURL = "http://localhost:3000"

type TaskRequest struct {
	Image string `json:"image"`
	Command []string `json:"command,omitempty"`
}

type TaskResponse struct {
	ID string `json:"ID"`
	Image string `json:"Image"`
	State string `json:"State"`
	WorkerID string `json:"WorkerID"`
	ContainerName string `json:"ContainerName"`
}

type NodeResponse struct {
	ID string `json:"id"`
	Address string `json:"address"`
	APIPort string `json:"api_port"`
	State string `json:"state"`
}

func printUsage() {
	fmt.Println("Orchestrator CLI")
	fmt.Println("\nUsage:")
	fmt.Println("    orch-cli <command> [args]")
	fmt.Println("\nAvailable commands:")
	fmt.Println("  submit  - Submit a new task to the master node")
	fmt.Println("  tasks   - List all the tasks from the system")
}

func submitTask(image string, command []string) {
	taskReq := TaskRequest {
		Image: image,
		Command: command,
	}

	jsonTask, err := json.Marshal(taskReq)
	if err != nil {
		fmt.Printf("An error occured while creating the JSON task: %v\n", err)
		return
	}

	fmt.Printf("Submitting task to master (Image: %s, Command: %v)\n", image, command)
	resp, err := http.Post(masterURL+"/task/submit", "application/json", bytes.NewBuffer(jsonTask))
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
	resp, err := http.Get(masterURL + "/tasks")
	if err != nil {
		fmt.Printf("Failed to fetch tasks from master: %v\n", err)
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

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3,' ', 0)
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
	resp, err := http.Get(masterURL + "/nodes")
	if err != nil {
		fmt.Printf("Failed to fetch nodes from master: %v\n", err)
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

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3,' ', 0)
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
