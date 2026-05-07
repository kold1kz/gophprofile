package services

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"strings"
	"time"

	"gophprofile/internal/domain"
	"gophprofile/pkg/imaging"

	"github.com/google/uuid"
)

var (
	// ErrFileTooLarge означает, что загруженный файл превысил лимит из конфигурации.
	ErrFileTooLarge = errors.New("file too large")
)

// AvatarService содержит основную бизнес-логику аватаров.
// Он связывает в один сценарий репозиторий, объектное хранилище и очередь событий.
type AvatarService struct {
	repo        AvatarRepository
	storage     ObjectStorage
	publisher   EventPublisher
	maxFileSize int64
}

// NewAvatarService создает сервис аватаров с нужными зависимостями.
func NewAvatarService(repo AvatarRepository, storage ObjectStorage, publisher EventPublisher, maxFileSize int64) *AvatarService {
	return &AvatarService{repo: repo, storage: storage, publisher: publisher, maxFileSize: maxFileSize}
}

// UploadInput собирает данные, которые приходят при загрузке файла.
type UploadInput struct {
	UserID   string
	FileName string
	Size     int64
	Reader   io.Reader
}

// Upload проверяет файл, сохраняет оригинал в S3, создает запись в базе и публикует событие для worker.
func (s *AvatarService) Upload(ctx context.Context, in UploadInput) (domain.Avatar, error) {
	if strings.TrimSpace(in.UserID) == "" {
		return domain.Avatar{}, fmt.Errorf("user id is required")
	}
	if in.Size > s.maxFileSize {
		return domain.Avatar{}, ErrFileTooLarge
	}

	limited := io.LimitReader(in.Reader, s.maxFileSize+1)
	info, err := imaging.Decode(limited)
	if err != nil {
		return domain.Avatar{}, err
	}
	if int64(len(info.Data)) > s.maxFileSize {
		return domain.Avatar{}, ErrFileTooLarge
	}

	id := uuid.NewString()
	ext := extensionByMime(info.MimeType, filepath.Ext(in.FileName))
	key := fmt.Sprintf("avatars/%s/original%s", id, ext)
	now := time.Now().UTC()
	avatar := domain.Avatar{
		ID:               id,
		UserID:           in.UserID,
		FileName:         in.FileName,
		MimeType:         info.MimeType,
		SizeBytes:        int64(len(info.Data)),
		Dimensions:       domain.Dimensions{Width: info.Width, Height: info.Height},
		S3Key:            key,
		UploadStatus:     domain.UploadStatusCompleted,
		ProcessingStatus: domain.ProcessingStatusPending,
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	if err := s.storage.Upload(ctx, key, info.MimeType, avatar.SizeBytes, bytes.NewReader(info.Data)); err != nil {
		return domain.Avatar{}, err
	}
	event := domain.AvatarUploadEvent{
		MessageID: uuid.NewString(),
		AvatarID:  avatar.ID,
		UserID:    avatar.UserID,
		S3Key:     avatar.S3Key,
	}

	created, err := s.repo.CreateWithUploadEvent(ctx, avatar, event)
	if err != nil {
		if deleteErr := s.storage.Delete(ctx, key); deleteErr != nil {
			log.Printf("cleanup uploaded object %q after db error: %v", key, deleteErr)
		}
		return domain.Avatar{}, err
	}
	if err := s.publisher.PublishUpload(ctx, event); err != nil {
		log.Printf("publish upload event %s failed, event remains in outbox: %v", event.MessageID, err)
		return created, nil
	}
	if err := s.repo.MarkOutboxPublished(ctx, event.MessageID); err != nil {
		log.Printf("mark outbox message %s published: %v", event.MessageID, err)
	}
	return created, nil
}

// Get возвращает метаданные аватара и поток с оригинальным файлом из хранилища.
func (s *AvatarService) Get(ctx context.Context, id string) (domain.Avatar, io.ReadCloser, string, error) {
	avatar, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return domain.Avatar{}, nil, "", err
	}
	body, contentType, err := s.storage.Download(ctx, avatar.S3Key)
	return avatar, body, contentType, err
}

// GetLatestForUser возвращает последний не удаленный аватар пользователя вместе с файлом.
func (s *AvatarService) GetLatestForUser(ctx context.Context, userID string) (domain.Avatar, io.ReadCloser, string, error) {
	avatar, err := s.repo.GetLatestByUserID(ctx, userID)
	if err != nil {
		return domain.Avatar{}, nil, "", err
	}
	body, contentType, err := s.storage.Download(ctx, avatar.S3Key)
	return avatar, body, contentType, err
}

// Metadata возвращает только данные об аватаре без скачивания файла из S3.
func (s *AvatarService) Metadata(ctx context.Context, id string) (domain.Avatar, error) {
	return s.repo.GetByID(ctx, id)
}

// LatestMetadata возвращает последний аватар пользователя без скачивания файла из S3.
func (s *AvatarService) LatestMetadata(ctx context.Context, userID string) (domain.Avatar, error) {
	return s.repo.GetLatestByUserID(ctx, userID)
}

// List возвращает все не удаленные аватары пользователя, начиная с самых новых.
func (s *AvatarService) List(ctx context.Context, userID string) ([]domain.Avatar, error) {
	return s.repo.ListByUserID(ctx, userID)
}

// PublishOutboxMessage публикует сохраненное в БД событие и помечает его отправленным.
func (s *AvatarService) PublishOutboxMessage(ctx context.Context, message domain.OutboxMessage) error {
	switch message.RoutingKey {
	case "avatar.uploaded":
		var event domain.AvatarUploadEvent
		if err := json.Unmarshal(message.Payload, &event); err != nil {
			return err
		}
		if err := s.publisher.PublishUpload(ctx, event); err != nil {
			return err
		}
		return s.repo.MarkOutboxPublished(ctx, message.ID)
	case "avatar.deleted":
		var event domain.AvatarDeleteEvent
		if err := json.Unmarshal(message.Payload, &event); err != nil {
			return err
		}
		if err := s.publisher.PublishDelete(ctx, event); err != nil {
			return err
		}
		return s.repo.MarkOutboxPublished(ctx, message.ID)
	default:
		return fmt.Errorf("unknown outbox routing key: %s", message.RoutingKey)
	}
}

// FlushOutbox публикует пачку событий, которые остались в outbox после временных ошибок RabbitMQ.
func (s *AvatarService) FlushOutbox(ctx context.Context, limit int) error {
	messages, err := s.repo.PendingOutbox(ctx, limit)
	if err != nil {
		return err
	}
	for _, message := range messages {
		if err := s.PublishOutboxMessage(ctx, message); err != nil {
			return err
		}
	}
	return nil
}

// Delete делает soft delete в базе и публикует событие на удаление файлов из S3.
func (s *AvatarService) Delete(ctx context.Context, id, userID string) error {
	_, event, err := s.repo.SoftDeleteWithDeleteEvent(ctx, id, userID, uuid.NewString())
	if err != nil {
		return err
	}
	if err := s.publisher.PublishDelete(ctx, event); err != nil {
		log.Printf("publish delete event %s failed, event remains in outbox: %v", event.MessageID, err)
		return nil
	}
	if err := s.repo.MarkOutboxPublished(ctx, event.MessageID); err != nil {
		log.Printf("mark outbox message %s published: %v", event.MessageID, err)
	}
	return nil
}

func extensionByMime(mimeType, fallback string) string {
	switch mimeType {
	case "image/jpeg":
		return ".jpg"
	case "image/png":
		return ".png"
	case "image/webp":
		return ".webp"
	default:
		if fallback != "" {
			return fallback
		}
		return ".img"
	}
}
