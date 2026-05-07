package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"gophprofile/internal/app"
	"gophprofile/internal/config"
	"gophprofile/internal/repository"
	"gophprofile/internal/storage"
	avatarworker "gophprofile/internal/worker"
)

func main() {
	cfg, err := config.LoadE()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := repository.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	defer db.Close()

	s3, err := storage.NewMinIO(ctx, cfg.S3Endpoint, cfg.PublicBaseURL, cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3Bucket, cfg.S3UseSSL)
	if err != nil {
		log.Fatalf("connect s3: %v", err)
	}

	broker, err := app.ConnectRabbit(ctx, cfg.RabbitURL, cfg.RabbitExchange, cfg.RabbitQueue)
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
