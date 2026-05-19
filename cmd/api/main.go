package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"notification-service/internal/broker"
	"notification-service/internal/models"
)

// In a real app, this would query a database. We are simulating it.
func mockDatabaseLookup(audienceID string) []string {
	// Simulating resolving an audience to 3 email addresses
	return []string{"alice@example.com", "bob@example.com", "charlie@example.com"}
}

func main() {
	// 1. Initialize the Kafka Producer
	// (Connecting to the Kafka container running via docker-compose)
	kafkaProducer := broker.NewProducer([]string{"localhost:9092"}, "email_dispatch")
	defer kafkaProducer.Close()

	// 2. Define the HTTP Handler
	http.HandleFunc("/api/campaign/launch", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Parse the incoming request
		var req models.CampaignRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		// Fan-Out Step 1: Query the database to get the user list
		emails := mockDatabaseLookup(req.AudienceID)

		// Fan-Out Step 2: Create individual jobs
		var jobs []models.EmailJob
		for i, email := range emails {
			jobs = append(jobs, models.EmailJob{
				JobID:        fmt.Sprintf("%s-job-%d", req.CampaignID, i),
				EmailAddress: email,
				TemplateID:   req.TemplateID,
			})
		}

		// Fan-Out Step 3: Push jobs to Kafka
		err := kafkaProducer.PublishJobs(r.Context(), jobs)
		if err != nil {
			http.Error(w, "Failed to queue campaign", http.StatusInternalServerError)
			log.Printf("Kafka error: %v", err)
			return
		}

		// Respond immediately to the client
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{
			"status":  "Campaign Accepted",
			"message": fmt.Sprintf("Queued %d emails for processing", len(emails)),
		})
	})

	// 3. Start the Server
	port := ":8080"
	fmt.Printf("API Gateway starting on port %s...\n", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
