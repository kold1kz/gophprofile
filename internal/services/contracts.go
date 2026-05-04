package services

import (
	"context"
	"io"

	"gophprofile/internal/domain"
)

// AvatarRepository описывает операции с метаданными аватаров.
// Сервису не важно, где они хранятся, пока реализация соблюдает этот контракт.
type AvatarRepository interface {
	// Create сохраняет метаданные нового аватара.
	Create(ctx context.Context, avatar domain.Avatar) (domain.Avatar, error)
	// GetByID возвращает один активный аватар по ID.
	GetByID(ctx context.Context, id string) (domain.Avatar, error)
	// GetLatestByUserID возвращает самый свежий активный аватар пользователя.
	GetLatestByUserID(ctx context.Context, userID string) (domain.Avatar, error)
	// ListByUserID возвращает все активные аватары пользователя.
	ListByUserID(ctx context.Context, userID string) ([]domain.Avatar, error)
	// SoftDelete помечает аватар удаленным и проверяет владельца.
	SoftDelete(ctx context.Context, id, userID string) (domain.Avatar, error)
	// MarkProcessing пытается перевести аватар в статус processing.
	MarkProcessing(ctx context.Context, id string) (bool, error)
	// UpdateProcessed сохраняет результат успешной обработки.
	UpdateProcessed(ctx context.Context, id string, thumbnails []domain.Thumbnail) error
	// UpdateProcessingFailed фиксирует ошибку обработки.
	UpdateProcessingFailed(ctx context.Context, id string) error
	// ProcessedMessage проверяет идемпотентность обработки события.
	ProcessedMessage(ctx context.Context, messageID string) (bool, error)
	// SaveProcessedMessage запоминает успешно обработанное событие.
	SaveProcessedMessage(ctx context.Context, messageID string) error
	// Ping проверяет доступность хранилища метаданных.
	Ping(ctx context.Context) error
}

// ObjectStorage описывает работу с объектным хранилищем для оригиналов и миниатюр.
type ObjectStorage interface {
	// Upload сохраняет объект под ключом.
	Upload(ctx context.Context, key, contentType string, size int64, body io.Reader) error
	// Download открывает объект на чтение.
	Download(ctx context.Context, key string) (io.ReadCloser, string, error)
	// Delete удаляет объект по ключу.
	Delete(ctx context.Context, key string) error
	// PresignedGetURL создает временную ссылку для чтения объекта.
	PresignedGetURL(ctx context.Context, key string) (string, error)
	// Ping проверяет доступность объектного хранилища.
	Ping(ctx context.Context) error
}

// EventPublisher описывает публикацию доменных событий в очередь.
type EventPublisher interface {
	// PublishUpload публикует событие о загруженном аватаре.
	PublishUpload(ctx context.Context, event domain.AvatarUploadEvent) error
	// PublishDelete публикует событие о необходимости удалить файлы.
	PublishDelete(ctx context.Context, event domain.AvatarDeleteEvent) error
	// Ping проверяет доступность брокера событий.
	Ping(ctx context.Context) error
}
