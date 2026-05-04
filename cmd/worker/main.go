package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gophprofile/internal/config"
	"gophprofile/internal/queue"
	"gophprofile/internal/repository"
	"gophprofile/internal/storage"
	avatarworker "gophprofile/internal/worker"
)

func main() {
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := repository.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	defer db.Close()

	s3, err := storage.NewMinIO(ctx, cfg.S3Endpoint, cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3Bucket, cfg.S3UseSSL)
	if err != nil {
		log.Fatalf("connect s3: %v", err)
	}

	broker, err := connectRabbit(ctx, cfg.RabbitURL, cfg.RabbitExchange, cfg.RabbitQueue)
	if err != nil {
		log.Fatalf("connect rabbitmq: %v", err)
	}
	defer broker.Close()

	w := avatarworker.New(repository.NewPostgres(db), s3, broker)
	log.Print("worker started")
	if err := w.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("worker failed: %v", err)
	}
}

func connectRabbit(ctx context.Context, url, exchange, queueName string) (*queue.RabbitMQ, error) {
	var lastErr error
	for attempt := 1; attempt <= 20; attempt++ {
		broker, err := queue.NewRabbitMQ(url, exchange, queueName)
		if err == nil {
			return broker, nil
		}
		lastErr = err
		log.Printf("connect rabbitmq attempt %d failed: %v", attempt, err)
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, lastErr
}
