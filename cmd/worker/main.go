package main

import (
	"flag"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"

	"orchestrator/internal/docker"
	"orchestrator/internal/utils"
	"orchestrator/internal/worker"
)

func setupLogger(fileName string) *os.File {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}

	multiWriter := io.MultiWriter(os.Stdout, file)

	log.SetFlags(log.Ldate | log.Ltime)
	log.SetOutput(multiWriter)

	return file
}

func main() {
	logFile := setupLogger("worker.log")
	defer logFile.Close()

	err := godotenv.Load()
	if err != nil {
		log.Println("[Warning] No .env file found. Using default environment variables.")
	}

	token := os.Getenv("ORCHESTRATOR_TOKEN")
	if token == "" {
		log.Fatal("ORCHESTRATOR_TOKEN is not set")
	}

	masterURL := os.Getenv("MASTER_URL")
	if masterURL == "" {
		log.Println("[Warning] MASTER_URL not set, task completion reporting disabled.")
	}


	apiPort := flag.String("api", ":8080", "HTTP port for worker agent")
	gossipPort := flag.String("gossip", ":8082", "UDP port for gossip communication")
	masterAddr := flag.String("master", "localhost:8081", "Comma-separated gossip addresses of seed nodes")

	flag.Parse()

	myIP := utils.GetLocalIP()
	log.Printf("[System] Worker node starting on IP: %s", myIP)

	manager, err := docker.CreateDockerManager()
	if err != nil {
		log.Fatalf("Failed to create docker manager: %v", err)
	}

	workerAgent := worker.CreateWorkerAgent(*apiPort, manager, *gossipPort, *masterAddr, token, masterURL)

	go func() {
		if err := workerAgent.StartWorker(); err != nil {
			log.Fatalf("Error starting worker agent: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	<-stop
	log.Println("Shuttind down worker agent...")
	workerAgent.Shutdown()
	os.Exit(0)
}