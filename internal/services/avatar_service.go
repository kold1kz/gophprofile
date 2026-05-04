package services

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
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

	// ErrForbidden возвращается, когда пользователь пытается изменить чужой аватар.
	ErrForbidden = errors.New("forbidden")

	// ErrNotFound означает, что запрошенный аватар не найден или уже удален.
	ErrNotFound = errors.New("not found")
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
	created, err := s.repo.Create(ctx, avatar)
	if err != nil {
		_ = s.storage.Delete(ctx, key)
		return domain.Avatar{}, err
	}
	event := domain.AvatarUploadEvent{
		MessageID: uuid.NewString(),
		AvatarID:  created.ID,
		UserID:    created.UserID,
		S3Key:     created.S3Key,
	}
	if err := s.publisher.PublishUpload(ctx, event); err != nil {
		return domain.Avatar{}, err
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

// List возвращает все не удаленные аватары пользователя, начиная с самых новых.
func (s *AvatarService) List(ctx context.Context, userID string) ([]domain.Avatar, error) {
	return s.repo.ListByUserID(ctx, userID)
}

// Delete делает soft delete в базе и публикует событие на удаление файлов из S3.
func (s *AvatarService) Delete(ctx context.Context, id, userID string) error {
	avatar, err := s.repo.SoftDelete(ctx, id, userID)
	if err != nil {
		return err
	}
	keys := []string{avatar.S3Key}
	for _, thumb := range avatar.ThumbnailS3Keys {
		if thumb.S3Key != "" {
			keys = append(keys, thumb.S3Key)
		}
	}
	return s.publisher.PublishDelete(ctx, domain.AvatarDeleteEvent{
		MessageID: uuid.NewString(),
		AvatarID:  avatar.ID,
		S3Keys:    keys,
	})
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
