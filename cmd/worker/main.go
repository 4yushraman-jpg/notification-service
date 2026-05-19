package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"notification-service/internal/broker"
	"notification-service/internal/models"

	"github.com/segmentio/kafka-go"
)

type WorkItem struct {
	Job models.EmailJob
	Msg *kafka.Message
}

func main() {
	fmt.Println("Starting Notification Workers...")

	consumer := broker.NewConsumer(
		[]string{"localhost:9092"},
		"email_dispatch",
		"email-senders",
	)

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Printf("Failed to close consumer: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	setupGracefulShutdown(cancel)

	// Internal work queue
	jobQueue := make(chan WorkItem, 100)

	var wg sync.WaitGroup

	// ----------------------------------------------------------------
	// SINGLE Kafka consumer goroutine
	// ----------------------------------------------------------------
	wg.Add(1)
	go consumeLoop(ctx, consumer, jobQueue, &wg)

	// ----------------------------------------------------------------
	// Worker pool
	// ----------------------------------------------------------------
	const numWorkers = 5

	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go workerLoop(ctx, i, consumer, jobQueue, &wg)
	}

	wg.Wait()

	fmt.Println("All workers shut down gracefully.")
}

// --------------------------------------------------------------------
// Kafka consumer loop
// ONLY this goroutine fetches from Kafka
// --------------------------------------------------------------------

func consumeLoop(
	ctx context.Context,
	consumer *broker.Consumer,
	jobQueue chan<- WorkItem,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	defer close(jobQueue)

	log.Println("[Consumer] Started")

	for {
		job, msg, err := consumer.FetchJob(ctx)

		if err != nil {

			// Graceful shutdown
			if errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded) {

				log.Println("[Consumer] Shutdown signal received")
				return
			}

			// Poison pill handling
			if errors.Is(err, broker.ErrPoisonPill) {
				log.Printf("[Consumer] Poison pill detected: %v", err)

				if msg != nil {
					commitCtx, cancel := context.WithTimeout(
						context.Background(),
						5*time.Second,
					)

					if err := consumer.CommitJob(commitCtx, msg); err != nil {
						log.Printf(
							"[Consumer] Failed to commit poison pill: %v",
							err,
						)
					}

					cancel()
				}

				continue
			}

			// Transient Kafka/network error
			log.Printf("[Consumer] Fetch error: %v", err)

			select {
			case <-time.After(250 * time.Millisecond):
			case <-ctx.Done():
				return
			}

			continue
		}

		select {

		case jobQueue <- WorkItem{
			Job: *job,
			Msg: msg,
		}:

		case <-ctx.Done():
			return
		}
	}
}

// --------------------------------------------------------------------
// Worker pool
// ONLY processes jobs
// DOES NOT fetch from Kafka
// --------------------------------------------------------------------

func workerLoop(
	ctx context.Context,
	workerID int,
	consumer *broker.Consumer,
	jobQueue <-chan WorkItem,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	log.Printf("[Worker %d] Started", workerID)

	for {
		select {

		case <-ctx.Done():
			log.Printf("[Worker %d] Shutting down", workerID)
			return

		case item, ok := <-jobQueue:

			if !ok {
				log.Printf("[Worker %d] Job queue closed", workerID)
				return
			}

			// --------------------------------------------------------
			// PROCESSING PHASE
			// --------------------------------------------------------

			log.Printf(
				"[Worker %d] Sending '%s' to %s (JobID=%s)",
				workerID,
				item.Job.TemplateID,
				item.Job.EmailAddress,
				item.Job.JobID,
			)

			// Simulated SMTP latency
			select {
			case <-time.After(100 * time.Millisecond):
			case <-ctx.Done():
				return
			}

			// --------------------------------------------------------
			// COMMIT PHASE
			// --------------------------------------------------------

			commitCtx, cancelCommit := context.WithTimeout(
				context.Background(),
				5*time.Second,
			)

			err := consumer.CommitJob(commitCtx, item.Msg)

			cancelCommit()

			if err != nil {
				log.Printf(
					"[Worker %d] Failed to commit JobID=%s: %v",
					workerID,
					item.Job.JobID,
					err,
				)

				continue
			}

			log.Printf(
				"[Worker %d] Successfully processed JobID=%s",
				workerID,
				item.Job.JobID,
			)
		}
	}
}

// --------------------------------------------------------------------
// Graceful shutdown
// --------------------------------------------------------------------

func setupGracefulShutdown(cancel context.CancelFunc) {
	signals := make(chan os.Signal, 1)

	signal.Notify(
		signals,
		os.Interrupt,
		syscall.SIGTERM,
	)

	go func() {
		<-signals

		fmt.Println("\nReceived shutdown signal...")

		cancel()
	}()
}
