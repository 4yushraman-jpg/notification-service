package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"notification-service/internal/models"

	"github.com/segmentio/kafka-go"
)

var ErrPoisonPill = errors.New("poison pill: malformed message")

type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(brokerURLs []string, topic string, groupID string) *Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokerURLs,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  time.Second,
	})

	return &Consumer{reader: r}
}

// FetchJob reads a single message. It returns the message even on a JSON error
// so the caller can commit it and prevent infinite poison-pill loops.
func (c *Consumer) FetchJob(ctx context.Context) (*models.EmailJob, *kafka.Message, error) {
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fetch message: %w", err)
	}

	var job models.EmailJob
	if err := json.Unmarshal(msg.Value, &job); err != nil {
		// POISON PILL FIX: Return the message and a specific error type
		return nil, &msg, fmt.Errorf("%w: %v", ErrPoisonPill, err)
	}

	return &job, &msg, nil
}

// CommitJob advances the consumer group offset for msg.
// With multiple worker goroutines, commits must remain ordered per partition
// (safe when a single worker processes and commits sequentially).
func (c *Consumer) CommitJob(ctx context.Context, msg *kafka.Message) error {
	return c.reader.CommitMessages(ctx, *msg)
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
