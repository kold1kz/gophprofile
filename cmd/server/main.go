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
	"gophprofile/internal/app"
	"gophprofile/internal/config"
	"gophprofile/internal/handlers"
	"gophprofile/internal/repository"
	"gophprofile/internal/services"
	"gophprofile/internal/storage"
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

	repo := repository.NewPostgres(db)
	service := services.NewAvatarService(repo, s3, broker, cfg.MaxFileSize)
	go runOutboxPublisher(ctx, service)
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

func runOutboxPublisher(ctx context.Context, service *services.AvatarService) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := service.FlushOutbox(ctx, 50); err != nil {
				log.Printf("flush outbox: %v", err)
			}
		}
	}
}
