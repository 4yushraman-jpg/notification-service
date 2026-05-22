package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	EmailsSentTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "emails_sent_total",
		Help: "Total emails successfully sent via SMTP.",
	})

	EmailsFailedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "emails_failed_total",
		Help: "Total emails that failed with a permanent SMTP error.",
	})

	SMTPRetryTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "smtp_retry_total",
		Help: "Total SMTP send retries after transient errors.",
	})

	KafkaFetchErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kafka_fetch_errors_total",
		Help: "Total transient Kafka consumer fetch errors.",
	})

	RedisErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "redis_errors_total",
		Help: "Total Redis operation errors.",
	})

	RateLimitHitsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "rate_limit_hits_total",
		Help: "Total times the global rate limiter blocked a send.",
	})

	PoisonPillTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "poison_pill_total",
		Help: "Total poison pill messages detected and skipped.",
	})

	DLQPublishTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dlq_publish_total",
		Help: "Total DLQ publish attempts by result.",
	}, []string{"result"})

	SMTPSendDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "smtp_send_duration_seconds",
		Help:    "SMTP send latency in seconds.",
		Buckets: prometheus.DefBuckets,
	})

	KafkaPublishDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "kafka_publish_duration_seconds",
		Help:    "Kafka publish latency in seconds.",
		Buckets: prometheus.DefBuckets,
	})

	ActiveWorkers = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "active_workers",
		Help: "Number of worker goroutines currently running.",
	})

	QueuedJobs = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "queued_jobs",
		Help: "Number of jobs waiting in the internal work queue.",
	})
)

func init() {
	prometheus.MustRegister(
		EmailsSentTotal,
		EmailsFailedTotal,
		SMTPRetryTotal,
		KafkaFetchErrorsTotal,
		RedisErrorsTotal,
		RateLimitHitsTotal,
		PoisonPillTotal,
		DLQPublishTotal,
		SMTPSendDuration,
		KafkaPublishDuration,
		ActiveWorkers,
		QueuedJobs,
	)
}

// Handler returns the Prometheus scrape endpoint handler.
func Handler() http.Handler {
	return promhttp.Handler()
}
