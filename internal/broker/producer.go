package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"notification-service/internal/models"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokerURLs []string, topic string) *Producer {
	w := &kafka.Writer{
		Addr:     kafka.TCP(brokerURLs...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	return &Producer{writer: w}
}

func (p *Producer) PublishJobs(ctx context.Context, jobs []models.EmailJob) error {
	var messages []kafka.Message

	for _, job := range jobs {
		jobBytes, err := json.Marshal(job)
		if err != nil {
			log.Printf("Failed to marshal job %s: %v", job.JobID, err)
			continue
		}

		messages = append(messages, kafka.Message{
			Key:   []byte(job.JobID),
			Value: jobBytes,
		})
	}

	if len(messages) == 0 {
		return errors.New("no valid messages to publish")
	}

	var err error
	const retries = 3

	for i := 0; i < retries; i++ {
		timeoutCtx, cancel := context.WithTimeout(ctx, time.Second)

		err = p.writer.WriteMessages(timeoutCtx, messages...)

		cancel()

		if err == nil {
			return nil
		}

		if errors.Is(err, kafka.LeaderNotAvailable) ||
			errors.Is(err, context.DeadlineExceeded) ||
			errors.Is(err, context.Canceled) {

			log.Printf(
				"Kafka transient error (attempt %d/%d): %v",
				i+1,
				retries,
				err,
			)

			select {
			case <-time.After(250 * time.Millisecond):
			case <-ctx.Done():
				return ctx.Err()
			}

			continue
		}

		return fmt.Errorf("unexpected kafka error: %w", err)
	}

	return fmt.Errorf(
		"failed to publish jobs after %d retries: %w",
		retries,
		err,
	)
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
