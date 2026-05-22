package logging

import (
	"log/slog"
	"os"
)

// Init configures the process-wide structured logger.
func Init(service string) *slog.Logger {
	level := slog.LevelInfo
	if os.Getenv("LOG_LEVEL") == "debug" {
		level = slog.LevelDebug
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	})

	logger := slog.New(handler).With("service", service)
	slog.SetDefault(logger)
	return logger
}
