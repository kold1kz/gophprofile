package domain

import "time"

const (
	// UploadStatusCompleted означает, что оригинальный файл успешно принят и сохранен.
	UploadStatusCompleted = "completed"

	// ProcessingStatusPending означает, что аватар ждет обработки worker'ом.
	ProcessingStatusPending = "pending"

	// ProcessingStatusProcessing означает, что worker уже взял аватар в работу.
	ProcessingStatusProcessing = "processing"

	// ProcessingStatusCompleted означает, что миниатюры успешно созданы.
	ProcessingStatusCompleted = "completed"

	// ProcessingStatusFailed означает, что обработка аватара завершилась ошибкой.
	ProcessingStatusFailed = "failed"
)

// Dimensions описывает исходные размеры загруженного изображения.
type Dimensions struct {
	Width  int `json:"width"`
	Height int `json:"height"`
}

// Thumbnail хранит информацию об одной сгенерированной миниатюре аватара.
type Thumbnail struct {
	Size  string `json:"size"`
	S3Key string `json:"s3_key,omitempty"`
	URL   string `json:"url"`
}

// Avatar описывает аватар пользователя: метаданные лежат в PostgreSQL, а сами файлы лежат в S3.
type Avatar struct {
	ID               string      `json:"id"`
	UserID           string      `json:"user_id"`
	FileName         string      `json:"file_name"`
	MimeType         string      `json:"mime_type"`
	SizeBytes        int64       `json:"size"`
	Dimensions       Dimensions  `json:"dimensions"`
	S3Key            string      `json:"-"`
	ThumbnailS3Keys  []Thumbnail `json:"thumbnails"`
	UploadStatus     string      `json:"upload_status"`
	ProcessingStatus string      `json:"status"`
	CreatedAt        time.Time   `json:"created_at"`
	UpdatedAt        time.Time   `json:"updated_at"`
	DeletedAt        *time.Time  `json:"deleted_at,omitempty"`
}

// AvatarUploadEvent отправляется в RabbitMQ после загрузки оригинального изображения.
// По этому событию worker генерирует миниатюры.
type AvatarUploadEvent struct {
	MessageID string `json:"message_id"`
	AvatarID  string `json:"avatar_id"`
	UserID    string `json:"user_id"`
	S3Key     string `json:"s3_key"`
}

// AvatarDeleteEvent отправляется в RabbitMQ после soft delete аватара.
// Worker использует список ключей, чтобы удалить файлы из S3.
type AvatarDeleteEvent struct {
	MessageID string   `json:"message_id"`
	AvatarID  string   `json:"avatar_id"`
	S3Keys    []string `json:"s3_keys"`
}
