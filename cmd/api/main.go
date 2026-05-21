package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"notification-service/internal/broker"
	"notification-service/internal/logging"
	"notification-service/internal/metrics"
	"notification-service/internal/models"

	"github.com/joho/godotenv"
)

var allowedTemplates = map[string]struct{}{
	"welcome":      {},
	"early_access": {},
	"newsletter":   {},
}

func mockDatabaseLookup(audienceID string) ([]string, error) {
	return []string{
		fmt.Sprintf("alice+%s@example.com", audienceID),
		fmt.Sprintf("bob+%s@example.com", audienceID),
		fmt.Sprintf("charlie+%s@example.com", audienceID),
	}, nil
}

func main() {
	logger := logging.Init("api")

	if err := godotenv.Load(); err != nil {
		logger.Warn("no .env file found, using system environment variables")
	}

	kafkaProducer := broker.NewProducer(
		envCSV("KAFKA_BROKERS", "localhost:9092"),
		envOrDefault("KAFKA_TOPIC", "email_dispatch"),
	)

	defer func() {
		if err := kafkaProducer.Close(); err != nil {
			logger.Error("failed to close kafka producer", "error", err)
		}
	}()

	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())

	mux.HandleFunc("/api/campaign/launch", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		defer r.Body.Close()

		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

		var req models.CampaignRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}

		if req.CampaignID == "" ||
			req.TemplateID == "" ||
			req.AudienceID == "" ||
			req.Subject == "" {

			http.Error(w, "missing required fields", http.StatusBadRequest)
			return
		}

		if _, ok := allowedTemplates[req.TemplateID]; !ok {
			http.Error(w, "invalid template_id", http.StatusBadRequest)
			return
		}

		emails, err := mockDatabaseLookup(req.AudienceID)
		if err != nil {
			logger.Error(
				"database lookup failed",
				"campaign_id", req.CampaignID,
				"audience_id", req.AudienceID,
				"error", err,
			)
			http.Error(w, "failed to resolve audience", http.StatusInternalServerError)
			return
		}

		jobs := make([]models.EmailJob, 0, len(emails))
		for i, email := range emails {
			jobs = append(jobs, models.EmailJob{
				JobID:        fmt.Sprintf("%s-job-%d", req.CampaignID, i),
				EmailAddress: email,
				TemplateID:   req.TemplateID,
				Subject:      req.Subject,
			})
		}

		err = kafkaProducer.PublishJobs(r.Context(), jobs)
		if err != nil {
			logger.Error(
				"kafka publish failed",
				"campaign_id", req.CampaignID,
				"template_id", req.TemplateID,
				"kafka_topic", "email_dispatch",
				"error", err,
			)
			http.Error(w, "failed to queue campaign", http.StatusInternalServerError)
			return
		}

		logger.Info(
			"campaign accepted",
			"campaign_id", req.CampaignID,
			"template_id", req.TemplateID,
			"queued_emails", len(emails),
		)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)

		if err := json.NewEncoder(w).Encode(map[string]string{
			"status": "Campaign Accepted",
			"message": fmt.Sprintf(
				"Queued %d emails for processing",
				len(emails),
			),
		}); err != nil {
			logger.Error("failed to write response", "error", err)
		}
	})

	server := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverErrors := make(chan error, 1)

	go func() {
		logger.Info("api gateway starting", "addr", server.Addr)
		serverErrors <- server.ListenAndServe()
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server failed", "error", err)
			os.Exit(1)
		}

	case sig := <-stop:
		logger.Info("received shutdown signal", "signal", sig.String())
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown error", "error", err)
	}

	logger.Info("api gateway shut down gracefully")
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envCSV(key, fallback string) []string {
	v := os.Getenv(key)
	if v == "" {
		return []string{fallback}
	}
	return strings.Split(v, ",")
}
