package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"notification-service/internal/broker"
	"notification-service/internal/cache"
	"notification-service/internal/logging"
	"notification-service/internal/mailer"
	"notification-service/internal/metrics"
	"notification-service/internal/models"
	"notification-service/internal/worker"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
)

type WorkItem struct {
	Job models.EmailJob
	Msg *kafka.Message
}

func main() {
	logger := logging.Init("worker")
	logger.Info("starting notification workers")

	if err := godotenv.Load(); err != nil {
		logger.Warn("no .env file found, using system environment variables")
	}

	brokerURLs := envCSV("KAFKA_BROKERS", "localhost:9092")
	dispatchTopic := envOrDefault("KAFKA_TOPIC", "email_dispatch")

	consumer := broker.NewConsumer(
		brokerURLs,
		dispatchTopic,
		envOrDefault("KAFKA_GROUP_ID", "email-senders"),
	)

	defer func() {
		if err := consumer.Close(); err != nil {
			logger.Error("failed to close consumer", "error", err)
		}
	}()

	dlqProducer := broker.NewDLQProducer(brokerURLs)
	defer func() {
		if err := dlqProducer.Close(); err != nil {
			logger.Error("failed to close dlq producer", "error", err)
		}
	}()

	redisCache, err := cache.NewRedisCache(envOrDefault("REDIS_ADDR", "localhost:6379"))
	if err != nil {
		logger.Error("failed to connect to redis", "error", err)
		os.Exit(1)
	}

	defer func() {
		if err := redisCache.Close(); err != nil {
			logger.Error("failed to close redis cache", "error", err)
		}
	}()

	smtpPort, err := strconv.Atoi(os.Getenv("SMTP_PORT"))
	if err != nil {
		logger.Error("invalid smtp port", "error", err)
		os.Exit(1)
	}

	smtpClient, err := mailer.New(
		os.Getenv("SMTP_HOST"),
		smtpPort,
		os.Getenv("SMTP_USERNAME"),
		os.Getenv("SMTP_PASSWORD"),
	)
	if err != nil {
		logger.Error("failed to initialize mailer", "error", err)
		os.Exit(1)
	}

	processor := &worker.Processor{
		Consumer: consumer,
		Redis:    redisCache,
		Mailer:   smtpClient,
		DLQ:      dlqProducer,
		Logger:   logger,
	}

	ctx, cancel := context.WithCancel(context.Background())
	setupGracefulShutdown(logger, cancel)

	metricsAddr := envOrDefault("METRICS_ADDR", ":9091")
	startMetricsServer(ctx, logger, metricsAddr)

	jobQueue := make(chan WorkItem, 100)

	var wg sync.WaitGroup

	wg.Add(1)
	go consumeLoop(ctx, logger, consumer, jobQueue, &wg)

	const numWorkers = 1
	metrics.ActiveWorkers.Set(float64(numWorkers))

	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go workerLoop(ctx, logger, processor, i, jobQueue, &wg)
	}

	wg.Wait()
	logger.Info("all workers shut down gracefully")
}

func consumeLoop(
	ctx context.Context,
	logger *slog.Logger,
	consumer *broker.Consumer,
	jobQueue chan<- WorkItem,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	defer close(jobQueue)

	logger.Info("kafka consumer started", "kafka_topic", "email_dispatch")

	for {
		job, msg, err := consumer.FetchJob(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded) {

				logger.Info("consumer shutdown signal received")
				return
			}

			if errors.Is(err, broker.ErrPoisonPill) {
				metrics.PoisonPillTotal.Inc()
				logger.Warn("poison pill detected", "error", err)

				if msg != nil {
					commitCtx, cancel := context.WithTimeout(
						context.Background(),
						5*time.Second,
					)

					if err := consumer.CommitJob(commitCtx, msg); err != nil {
						logger.Error(
							"failed to commit poison pill",
							"error", err,
						)
					}

					cancel()
				}

				continue
			}

			metrics.KafkaFetchErrorsTotal.Inc()
			logger.Warn("kafka fetch error", "error", err)

			select {
			case <-time.After(250 * time.Millisecond):
			case <-ctx.Done():
				return
			}

			continue
		}

		metrics.QueuedJobs.Set(float64(len(jobQueue) + 1))

		select {
		case jobQueue <- WorkItem{Job: *job, Msg: msg}:
			metrics.QueuedJobs.Set(float64(len(jobQueue)))
		case <-ctx.Done():
			return
		}
	}
}

func workerLoop(
	ctx context.Context,
	logger *slog.Logger,
	processor *worker.Processor,
	workerID int,
	jobQueue <-chan WorkItem,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	workerLogger := logger.With("worker_id", workerID)
	workerLogger.Info("worker started")

	for {
		select {
		case <-ctx.Done():
			workerLogger.Info("worker shutting down")
			return

		case item, ok := <-jobQueue:
			if !ok {
				workerLogger.Info("job queue closed")
				return
			}

			metrics.QueuedJobs.Set(float64(len(jobQueue)))

			if !processor.ProcessJob(ctx, workerID, item.Job, item.Msg) {
				return
			}
		}
	}
}

func startMetricsServer(
	ctx context.Context,
	logger *slog.Logger,
	addr string,
) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())

	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info("metrics server listening", "addr", addr)
		if err := server.ListenAndServe(); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server failed", "error", err)
		}
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("metrics server shutdown error", "error", err)
		}
	}()
}

func setupGracefulShutdown(
	logger *slog.Logger,
	cancel context.CancelFunc,
) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-signals
		logger.Info("received shutdown signal", "signal", sig.String())
		cancel()
	}()
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
