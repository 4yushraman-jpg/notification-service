package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"notification-service/internal/metrics"
	"notification-service/internal/models"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
	topic  string
}

func NewProducer(brokerURLs []string, topic string) *Producer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokerURLs...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
	}
	return &Producer{writer: w, topic: topic}
}

func (p *Producer) PublishJobs(ctx context.Context, jobs []models.EmailJob) error {
	start := time.Now()
	defer func() {
		metrics.KafkaPublishDuration.Observe(time.Since(start).Seconds())
	}()

	var messages []kafka.Message

	for _, job := range jobs {
		jobBytes, err := json.Marshal(job)
		if err != nil {
			return fmt.Errorf(
				"marshal job %s: %w",
				job.JobID,
				err,
			)
		}

		messages = append(messages, kafka.Message{
			Key:   []byte(job.JobID),
			Value: jobBytes,
		})
	}

	var err error
	const retries = 3

	for i := 0; i < retries; i++ {
		timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)

		err = p.writer.WriteMessages(timeoutCtx, messages...)

		cancel()

		if err == nil {
			slog.Info(
				"kafka publish succeeded",
				"message_count", len(messages),
				"kafka_topic", p.topic,
			)
			return nil
		}

		if errors.Is(err, kafka.LeaderNotAvailable) ||
			errors.Is(err, context.DeadlineExceeded) ||
			errors.Is(err, context.Canceled) {

			slog.Warn(
				"kafka transient publish error",
				"attempt", i+1,
				"max_attempts", retries,
				"kafka_topic", p.topic,
				"error", err,
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

// PublishRaw writes a pre-serialized payload to the producer topic.
func (p *Producer) PublishRaw(ctx context.Context, key string, value []byte) error {
	start := time.Now()
	defer func() {
		metrics.KafkaPublishDuration.Observe(time.Since(start).Seconds())
	}()

	var err error
	const retries = 3

	msg := kafka.Message{
		Key:   []byte(key),
		Value: value,
	}

	for i := 0; i < retries; i++ {
		timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)

		err = p.writer.WriteMessages(timeoutCtx, msg)

		cancel()

		if err == nil {
			return nil
		}

		if errors.Is(err, kafka.LeaderNotAvailable) ||
			errors.Is(err, context.DeadlineExceeded) ||
			errors.Is(err, context.Canceled) {

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
		"failed to publish raw message after %d retries: %w",
		retries,
		err,
	)
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
