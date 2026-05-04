package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gophprofile/internal/api"
	"gophprofile/internal/config"
	"gophprofile/internal/handlers"
	"gophprofile/internal/queue"
	"gophprofile/internal/repository"
	"gophprofile/internal/services"
	"gophprofile/internal/storage"
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

	repo := repository.NewPostgres(db)
	service := services.NewAvatarService(repo, s3, broker, cfg.MaxFileSize)
	handler := handlers.NewAvatarHandler(service, api.Health{DB: repo, S3: s3, Broker: broker}, cfg.MaxFileSize)

	server := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: handler.Routes(),
	}

	go func() {
		log.Printf("server listening on %s", cfg.HTTPAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownDelay)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown: %v", err)
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
