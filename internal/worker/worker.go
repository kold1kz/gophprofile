package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"gophprofile/internal/domain"
	"gophprofile/internal/queue"
	"gophprofile/internal/services"
	"gophprofile/pkg/imaging"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Worker читает события из RabbitMQ и выполняет тяжелую фоновую работу с файлами.
type Worker struct {
	repo    services.AvatarRepository
	storage services.ObjectStorage
	events  *queue.RabbitMQ
}

// New создает worker с репозиторием, объектным хранилищем и очередью событий.
func New(repo services.AvatarRepository, storage services.ObjectStorage, events *queue.RabbitMQ) *Worker {
	return &Worker{repo: repo, storage: storage, events: events}
}

// Run запускает цикл чтения сообщений и останавливается, когда закрыт контекст или очередь.
func (w *Worker) Run(ctx context.Context) error {
	deliveries, err := w.events.Consume()
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case delivery, ok := <-deliveries:
			if !ok {
				return nil
			}
			w.handleDelivery(ctx, delivery)
		}
	}
}

func (w *Worker) handleDelivery(ctx context.Context, delivery amqp.Delivery) {
	err := retry(ctx, 3, func() error {
		switch delivery.RoutingKey {
		case queue.RoutingAvatarUploaded:
			var event domain.AvatarUploadEvent
			if err := json.Unmarshal(delivery.Body, &event); err != nil {
				return err
			}
			return w.HandleUploadEvent(ctx, event)
		case queue.RoutingAvatarDeleted:
			var event domain.AvatarDeleteEvent
			if err := json.Unmarshal(delivery.Body, &event); err != nil {
				return err
			}
			return w.HandleDeleteEvent(ctx, event)
		default:
			return fmt.Errorf("unknown routing key: %s", delivery.RoutingKey)
		}
	})
	if err != nil {
		_ = delivery.Nack(false, false)
		return
	}
	_ = delivery.Ack(false)
}

// HandleUploadEvent обрабатывает загрузку: делает миниатюры, сохраняет их и обновляет статус аватара.
func (w *Worker) HandleUploadEvent(ctx context.Context, event domain.AvatarUploadEvent) error {
	if event.MessageID != "" {
		processed, err := w.repo.ProcessedMessage(ctx, event.MessageID)
		if err != nil {
			return err
		}
		if processed {
			return nil
		}
	}

	locked, err := w.repo.MarkProcessing(ctx, event.AvatarID)
	if err != nil {
		return err
	}
	if !locked {
		if event.MessageID != "" {
			return w.repo.SaveProcessedMessage(ctx, event.MessageID)
		}
		return nil
	}

	body, _, err := w.storage.Download(ctx, event.S3Key)
	if err != nil {
		_ = w.repo.UpdateProcessingFailed(ctx, event.AvatarID)
		return err
	}
	defer body.Close()

	info, err := imaging.Decode(body)
	if err != nil {
		_ = w.repo.UpdateProcessingFailed(ctx, event.AvatarID)
		return err
	}

	sizes := []struct {
		name   string
		width  int
		height int
	}{
		{name: "100x100", width: 100, height: 100},
		{name: "300x300", width: 300, height: 300},
	}
	thumbnails := make([]domain.Thumbnail, 0, len(sizes))
	for _, size := range sizes {
		data, err := imaging.ResizeJPEG(info.Image, size.width, size.height)
		if err != nil {
			_ = w.repo.UpdateProcessingFailed(ctx, event.AvatarID)
			return err
		}
		key := fmt.Sprintf("thumbnails/%s/%s.jpg", event.AvatarID, size.name)
		if err := w.storage.Upload(ctx, key, "image/jpeg", int64(len(data)), bytesReader(data)); err != nil {
			_ = w.repo.UpdateProcessingFailed(ctx, event.AvatarID)
			return err
		}
		url, _ := w.storage.PresignedGetURL(ctx, key)
		thumbnails = append(thumbnails, domain.Thumbnail{Size: size.name, S3Key: key, URL: url})
	}
	if err := w.repo.UpdateProcessed(ctx, event.AvatarID, thumbnails); err != nil {
		return err
	}
	if event.MessageID != "" {
		return w.repo.SaveProcessedMessage(ctx, event.MessageID)
	}
	return nil
}

// HandleDeleteEvent удаляет из S3 все файлы, связанные с аватаром.
func (w *Worker) HandleDeleteEvent(ctx context.Context, event domain.AvatarDeleteEvent) error {
	if event.MessageID != "" {
		processed, err := w.repo.ProcessedMessage(ctx, event.MessageID)
		if err != nil {
			return err
		}
		if processed {
			return nil
		}
	}
	for _, key := range event.S3Keys {
		if key == "" {
			continue
		}
		if err := w.storage.Delete(ctx, key); err != nil {
			return err
		}
	}
	if event.MessageID != "" {
		return w.repo.SaveProcessedMessage(ctx, event.MessageID)
	}
	return nil
}

func retry(ctx context.Context, attempts int, fn func() error) error {
	var err error
	delay := 300 * time.Millisecond
	for i := 0; i < attempts; i++ {
		if err = fn(); err == nil {
			return nil
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		delay *= 2
	}
	return err
}

func bytesReader(data []byte) *bytes.Reader {
	return bytes.NewReader(data)
}
