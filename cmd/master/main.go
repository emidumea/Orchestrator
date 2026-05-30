package main

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/joho/godotenv"

	"orchestrator/internal/master"
	"orchestrator/internal/store"
	"orchestrator/internal/utils"
)

func setupLogger(fileName string) *os.File {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}

	multiWriter := io.MultiWriter(os.Stdout, file)

	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.SetOutput(multiWriter)

	return file
}

func main() {
	logFile := setupLogger("orchestrator.log")
	defer logFile.Close()

	err := godotenv.Load()
	if err != nil {
		log.Println("[Warning] No .env file found. Using default environment variables.")
	}

	token := os.Getenv("ORCHESTRATOR_TOKEN")
	if token == "" {
		log.Fatal("ORCHESTRATOR_TOKEN is not set")
	}

	port := os.Getenv("MASTER_PORT")
	if port == "" {
		port = "3000"
	}

	log.Println("Starting master node ...")

	dbStore, err := store.CreateStore("master.db")
	if err != nil {
		log.Fatalf("Failed to create the store: %v", err)
	}

	m := master.CreateMaster(":"+port, dbStore, token)

	myIP := utils.GetLocalIP()
	log.Printf("--------------------------------")
	log.Printf("Master node is live")
	log.Printf("Master address for cli: http://%s:%s", myIP, port)
	log.Printf("Master address for workers: %s:8081", myIP)
	log.Printf("--------------------------------")

	envMap, err := godotenv.Read(".env")
	if err == nil {
		envMap["MASTER_URL"] = fmt.Sprintf("http://%s:%s", myIP, port)

		err = godotenv.Write(envMap, ".env")
		if err != nil {
			log.Printf("[Warning] Couldn't update .env automatically: %v", err)
		} else {
			log.Printf("[System] '.env' file successfully updated with the new IP")
		}
	}

	if err := m.StartMaster(); err != nil {
		log.Fatalf("Failed to start master: %v", err)
	}


}
