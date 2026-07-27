package slog

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/notrodans/nebula-go/config"
)

type LoggerKey struct{}

func SetupLogger(cfg *config.Config) *slog.Logger {
	var log *slog.Logger
	switch cfg.Env {
	case config.Development:
		log = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))
	case config.Production:
		log = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		}))
	case config.Testing:
		log = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))
	case config.Staging:
		log = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))
	default:
		panic(fmt.Errorf("unhandled ENV variable %q", cfg.Env))
	}

	return log
}

func LoggerFrom(ctx context.Context) *slog.Logger {
	logger, ok := ctx.Value(LoggerKey{}).(*slog.Logger)
	if !ok {
		panic("logger middleware not connected")
	}
	return logger
}
