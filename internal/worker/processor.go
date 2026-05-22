package worker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"notification-service/internal/broker"
	"notification-service/internal/cache"
	"notification-service/internal/mailer"
	"notification-service/internal/metrics"
	"notification-service/internal/models"

	"github.com/segmentio/kafka-go"
)

const (
	maxSMTPAttempts = 3
	maxCommitTries  = 3
)

var smtpBackoffs = []time.Duration{
	2 * time.Second,
	4 * time.Second,
	8 * time.Second,
}

// Processor handles idempotency, rate limiting, SMTP delivery, DLQ, and commits.
type Processor struct {
	Consumer *broker.Consumer
	Redis    *cache.RedisCache
	Mailer   *mailer.Mailer
	DLQ      *broker.DLQProducer
	Logger   *slog.Logger
}

// ProcessJob runs the full delivery pipeline for one Kafka message.
// Returns false if the worker should shut down (context canceled).
func (p *Processor) ProcessJob(
	ctx context.Context,
	workerID int,
	job models.EmailJob,
	msg *kafka.Message,
) bool {
	log := p.Logger.With(
		"worker_id", workerID,
		"job_id", job.JobID,
		"template_id", job.TemplateID,
		"email", job.EmailAddress,
		"kafka_topic", "email_dispatch",
	)

	var claimed bool
	committed := false

	defer func() {
		if claimed && !committed {
			p.releaseClaim(job.JobID, log)
		}
	}()

	duplicate, ok := p.claimJob(ctx, job, msg, log)
	if !ok {
		return !errorsIsCanceled(ctx)
	}
	if duplicate {
		return true
	}
	claimed = true

	if !p.waitForRateLimit(ctx, log) {
		return !errorsIsCanceled(ctx)
	}

	log.Info("sending email")

	sendErr, smtpAttempts := p.sendWithRetry(ctx, job, log)
	if sendErr == nil {
		if err := p.commitMessageWithRetry(ctx, msg, log); err != nil {
			log.Error(
				"email sent but kafka commit failed; claim retained to prevent duplicate send",
				"error", err,
			)
			committed = true
		} else {
			committed = true
			metrics.EmailsSentTotal.Inc()
			log.Info("successfully processed job")
		}

		if !wait(ctx, 3*time.Second) {
			return false
		}
		return true
	}

	if mailer.IsPermanentSMTPError(sendErr) {
		p.handlePermanentFailure(
			ctx,
			job,
			msg,
			sendErr,
			smtpAttempts,
			log,
			&committed,
		)
		return !errorsIsCanceled(ctx)
	}

	log.Warn(
		"transient smtp failure, message not committed",
		"error", sendErr,
		"smtp_attempts", smtpAttempts,
	)
	return !errorsIsCanceled(ctx)
}

func (p *Processor) releaseClaim(jobID string, log *slog.Logger) {
	releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := p.Redis.ReleaseJobClaim(releaseCtx, jobID); err != nil {
		log.Warn("failed to release idempotency claim for retry", "error", err)
		return
	}
	log.Info("released idempotency claim for kafka redelivery")
}

func (p *Processor) claimJob(
	ctx context.Context,
	job models.EmailJob,
	msg *kafka.Message,
	log *slog.Logger,
) (duplicate bool, ok bool) {
	for {
		claimed, err := p.Redis.TryClaimJob(ctx, job.JobID)
		if err == nil {
			if !claimed {
				log.Info("duplicate job skipped")
				p.commitDuplicate(msg, log)
				return true, true
			}
			return false, true
		}

		metrics.RedisErrorsTotal.Inc()
		log.Error("redis idempotency error", "error", err)

		if !wait(ctx, 250*time.Millisecond) {
			return false, false
		}
	}
}

func (p *Processor) commitDuplicate(
	msg *kafka.Message,
	log *slog.Logger,
) {
	if err := p.commitMessage(msg); err != nil {
		log.Error("failed to commit duplicate job", "error", err)
	}
}

func (p *Processor) waitForRateLimit(
	ctx context.Context,
	log *slog.Logger,
) bool {
	for {
		allowed, err := p.Redis.AllowRequest(ctx, 1)
		if err != nil {
			metrics.RedisErrorsTotal.Inc()
			log.Error("redis rate limiter error", "error", err)
			if !wait(ctx, 1*time.Second) {
				return false
			}
			continue
		}

		if allowed {
			return true
		}

		metrics.RateLimitHitsTotal.Inc()
		log.Warn("rate limit exceeded")

		if !wait(ctx, 100*time.Millisecond) {
			return false
		}
	}
}

func (p *Processor) sendWithRetry(
	ctx context.Context,
	job models.EmailJob,
	log *slog.Logger,
) (error, int) {
	templateData := mailer.TemplateData{
		Email: job.EmailAddress,
		JobID: job.JobID,
	}

	var lastErr error

	for attempt := 1; attempt <= maxSMTPAttempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err(), attempt
		}

		start := time.Now()
		lastErr = p.Mailer.SendEmail(
			job.EmailAddress,
			job.Subject,
			job.TemplateID,
			templateData,
		)
		metrics.SMTPSendDuration.Observe(time.Since(start).Seconds())

		if lastErr == nil {
			return nil, attempt
		}

		log.Error(
			"smtp send failed",
			"error", lastErr,
			"retry_attempt", attempt,
		)

		if mailer.IsPermanentSMTPError(lastErr) {
			return lastErr, attempt
		}

		if !mailer.IsTransientSMTPError(lastErr) {
			log.Warn("unclassified smtp error, treating as transient", "error", lastErr)
		}

		if attempt >= maxSMTPAttempts {
			break
		}

		backoff := smtpBackoffs[attempt-1]
		metrics.SMTPRetryTotal.Inc()
		log.Info(
			"smtp retry scheduled",
			"retry_attempt", attempt,
			"backoff_duration", backoff.String(),
		)

		if !wait(ctx, backoff) {
			return ctx.Err(), attempt
		}
	}

	return lastErr, maxSMTPAttempts
}

func (p *Processor) handlePermanentFailure(
	ctx context.Context,
	job models.EmailJob,
	msg *kafka.Message,
	sendErr error,
	smtpAttempts int,
	log *slog.Logger,
	committed *bool,
) {
	metrics.EmailsFailedTotal.Inc()
	log.Error(
		"permanent smtp failure",
		"error", sendErr,
		"smtp_attempts", smtpAttempts,
	)

	publishCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dlqErr := p.DLQ.PublishFailure(
		publishCtx,
		job,
		sendErr.Error(),
		smtpAttempts,
	)

	if dlqErr != nil {
		metrics.DLQPublishTotal.WithLabelValues("failure").Inc()
		log.Error(
			"dlq publish failed, original message not committed",
			"error", dlqErr,
		)
		return
	}

	metrics.DLQPublishTotal.WithLabelValues("success").Inc()
	log.Info("dlq publish succeeded", "kafka_topic", broker.DLQTopic)

	// Pin claim so redelivery does not attempt another SMTP send.
	*committed = true

	if err := p.commitMessageWithRetry(ctx, msg, log); err != nil {
		log.Error(
			"dlq published but kafka commit failed; claim retained, expect duplicate-skip on redelivery",
			"error", err,
		)
		return
	}

	log.Info("permanent failure committed after dlq publish")
}

func (p *Processor) commitMessage(msg *kafka.Message) error {
	commitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return p.Consumer.CommitJob(commitCtx, msg)
}

func (p *Processor) commitMessageWithRetry(
	ctx context.Context,
	msg *kafka.Message,
	log *slog.Logger,
) error {
	var lastErr error

	for attempt := 1; attempt <= maxCommitTries; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		lastErr = p.commitMessage(msg)
		if lastErr == nil {
			return nil
		}

		log.Warn(
			"kafka commit failed, retrying",
			"error", lastErr,
			"retry_attempt", attempt,
		)

		if attempt >= maxCommitTries {
			break
		}

		if !wait(ctx, 250*time.Millisecond) {
			return ctx.Err()
		}
	}

	return lastErr
}

func wait(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func errorsIsCanceled(ctx context.Context) bool {
	return errors.Is(ctx.Err(), context.Canceled) ||
		errors.Is(ctx.Err(), context.DeadlineExceeded)
}
