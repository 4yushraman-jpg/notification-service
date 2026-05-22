package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"notification-service/internal/models"
)

const DLQTopic = "email_dispatch_dlq"

// DLQProducer publishes permanently failed jobs to the dead letter queue.
type DLQProducer struct {
	producer *Producer
}

func NewDLQProducer(brokerURLs []string) *DLQProducer {
	return &DLQProducer{
		producer: NewProducer(brokerURLs, DLQTopic),
	}
}

func (d *DLQProducer) PublishFailure(
	ctx context.Context,
	job models.EmailJob,
	errMsg string,
	retryCount int,
) error {
	payload := models.DLQMessage{
		OriginalJob:  job,
		ErrorMessage: errMsg,
		Timestamp:    time.Now().UTC(),
		RetryCount:   retryCount,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal dlq payload: %w", err)
	}

	return d.producer.PublishRaw(ctx, job.JobID, body)
}

func (d *DLQProducer) Close() error {
	return d.producer.Close()
}
