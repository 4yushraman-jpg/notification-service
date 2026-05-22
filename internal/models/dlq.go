package models

import "time"

// DLQMessage is published to email_dispatch_dlq for permanently failed jobs.
type DLQMessage struct {
	OriginalJob  EmailJob  `json:"original_job"`
	ErrorMessage string    `json:"error_message"`
	Timestamp    time.Time `json:"timestamp"`
	RetryCount   int       `json:"retry_count"`
}
