package repository

import (
	"context"
	"encoding/json"
	"errors"

	"gophprofile/internal/domain"
	"gophprofile/internal/services"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresRepository хранит и читает метаданные аватаров в PostgreSQL.
type PostgresRepository struct {
	pool *pgxpool.Pool
}

// NewPostgres оборачивает готовый pgx pool в репозиторий приложения.
func NewPostgres(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

// Connect создает пул подключений к PostgreSQL и сразу проверяет, что база отвечает.
func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

// Create сохраняет новый аватар и возвращает его с датами, которые выставила база.
func (r *PostgresRepository) Create(ctx context.Context, avatar domain.Avatar) (domain.Avatar, error) {
	thumbs, err := json.Marshal(avatar.ThumbnailS3Keys)
	if err != nil {
		return domain.Avatar{}, err
	}
	err = r.pool.QueryRow(ctx, `
		INSERT INTO avatars (
			id, user_id, file_name, mime_type, size_bytes, width, height, s3_key,
			thumbnail_s3_keys, upload_status, processing_status, created_at, updated_at
		)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		RETURNING created_at, updated_at`,
		avatar.ID, avatar.UserID, avatar.FileName, avatar.MimeType, avatar.SizeBytes,
		avatar.Dimensions.Width, avatar.Dimensions.Height, avatar.S3Key, thumbs,
		avatar.UploadStatus, avatar.ProcessingStatus, avatar.CreatedAt, avatar.UpdatedAt,
	).Scan(&avatar.CreatedAt, &avatar.UpdatedAt)
	return avatar, err
}

// GetByID ищет не удаленный аватар по идентификатору.
func (r *PostgresRepository) GetByID(ctx context.Context, id string) (domain.Avatar, error) {
	return r.scanOne(ctx, `
		SELECT id, user_id, file_name, mime_type, size_bytes, width, height, s3_key,
			thumbnail_s3_keys, upload_status, processing_status, created_at, updated_at, deleted_at
		FROM avatars
		WHERE id=$1 AND deleted_at IS NULL`, id)
}

// GetLatestByUserID возвращает последний не удаленный аватар конкретного пользователя.
func (r *PostgresRepository) GetLatestByUserID(ctx context.Context, userID string) (domain.Avatar, error) {
	return r.scanOne(ctx, `
		SELECT id, user_id, file_name, mime_type, size_bytes, width, height, s3_key,
			thumbnail_s3_keys, upload_status, processing_status, created_at, updated_at, deleted_at
		FROM avatars
		WHERE user_id=$1 AND deleted_at IS NULL
		ORDER BY created_at DESC
		LIMIT 1`, userID)
}

// ListByUserID возвращает историю не удаленных аватаров пользователя.
func (r *PostgresRepository) ListByUserID(ctx context.Context, userID string) ([]domain.Avatar, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, user_id, file_name, mime_type, size_bytes, width, height, s3_key,
			thumbnail_s3_keys, upload_status, processing_status, created_at, updated_at, deleted_at
		FROM avatars
		WHERE user_id=$1 AND deleted_at IS NULL
		ORDER BY created_at DESC`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var avatars []domain.Avatar
	for rows.Next() {
		avatar, err := scanAvatar(rows)
		if err != nil {
			return nil, err
		}
		avatars = append(avatars, avatar)
	}
	return avatars, rows.Err()
}

// SoftDelete помечает аватар удаленным, но оставляет запись в базе для истории и аудита.
func (r *PostgresRepository) SoftDelete(ctx context.Context, id, userID string) (domain.Avatar, error) {
	avatar, err := r.GetByID(ctx, id)
	if err != nil {
		return domain.Avatar{}, err
	}
	if avatar.UserID != userID {
		return domain.Avatar{}, services.ErrForbidden
	}
	_, err = r.pool.Exec(ctx, `UPDATE avatars SET deleted_at=NOW(), updated_at=NOW() WHERE id=$1 AND deleted_at IS NULL`, id)
	return avatar, err
}

// MarkProcessing пытается взять аватар в обработку и защищает worker от двойной работы.
func (r *PostgresRepository) MarkProcessing(ctx context.Context, id string) (bool, error) {
	tag, err := r.pool.Exec(ctx, `
		UPDATE avatars
		SET processing_status=$2, updated_at=NOW()
		WHERE id=$1 AND deleted_at IS NULL AND processing_status IN ($3, $4)`,
		id, domain.ProcessingStatusProcessing, domain.ProcessingStatusPending, domain.ProcessingStatusFailed,
	)
	return tag.RowsAffected() == 1, err
}

// UpdateProcessed сохраняет готовые миниатюры и переводит аватар в статус completed.
func (r *PostgresRepository) UpdateProcessed(ctx context.Context, id string, thumbnails []domain.Thumbnail) error {
	payload, err := json.Marshal(thumbnails)
	if err != nil {
		return err
	}
	_, err = r.pool.Exec(ctx, `
		UPDATE avatars
		SET thumbnail_s3_keys=$2, processing_status=$3, updated_at=NOW()
		WHERE id=$1 AND deleted_at IS NULL`,
		id, payload, domain.ProcessingStatusCompleted,
	)
	return err
}

// UpdateProcessingFailed помечает аватар как failed, если worker не смог его обработать.
func (r *PostgresRepository) UpdateProcessingFailed(ctx context.Context, id string) error {
	_, err := r.pool.Exec(ctx, `UPDATE avatars SET processing_status=$2, updated_at=NOW() WHERE id=$1`, id, domain.ProcessingStatusFailed)
	return err
}

// ProcessedMessage проверяет, обрабатывали ли мы уже сообщение с таким MessageID.
func (r *PostgresRepository) ProcessedMessage(ctx context.Context, messageID string) (bool, error) {
	var exists bool
	err := r.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM processed_messages WHERE message_id=$1)`, messageID).Scan(&exists)
	return exists, err
}

// SaveProcessedMessage запоминает MessageID после успешной обработки события.
func (r *PostgresRepository) SaveProcessedMessage(ctx context.Context, messageID string) error {
	_, err := r.pool.Exec(ctx, `INSERT INTO processed_messages(message_id) VALUES ($1) ON CONFLICT DO NOTHING`, messageID)
	return err
}

// Ping проверяет, что PostgreSQL доступен.
func (r *PostgresRepository) Ping(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

func (r *PostgresRepository) scanOne(ctx context.Context, query string, args ...any) (domain.Avatar, error) {
	avatar, err := scanAvatar(r.pool.QueryRow(ctx, query, args...))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Avatar{}, services.ErrNotFound
	}
	return avatar, err
}

type scanner interface {
	Scan(dest ...any) error
}

func scanAvatar(row scanner) (domain.Avatar, error) {
	var avatar domain.Avatar
	var thumbs []byte
	err := row.Scan(
		&avatar.ID, &avatar.UserID, &avatar.FileName, &avatar.MimeType, &avatar.SizeBytes,
		&avatar.Dimensions.Width, &avatar.Dimensions.Height, &avatar.S3Key, &thumbs,
		&avatar.UploadStatus, &avatar.ProcessingStatus, &avatar.CreatedAt, &avatar.UpdatedAt, &avatar.DeletedAt,
	)
	if err != nil {
		return domain.Avatar{}, err
	}
	if len(thumbs) > 0 {
		if err := json.Unmarshal(thumbs, &avatar.ThumbnailS3Keys); err != nil {
			return domain.Avatar{}, err
		}
	}
	return avatar, nil
}
