package services

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"image"
	"image/color"
	"image/jpeg"
	"io"
	"sort"
	"testing"
	"time"

	"gophprofile/internal/domain"

	"github.com/stretchr/testify/require"
)

func TestAvatarServiceUploadPublishesEvent(t *testing.T) {
	repo := newMemoryRepo()
	storage := newMemoryStorage()
	publisher := &memoryPublisher{}
	service := NewAvatarService(repo, storage, publisher, 10<<20)

	avatar, err := service.Upload(context.Background(), UploadInput{
		UserID:   "user-1",
		FileName: "avatar.jpg",
		Reader:   bytes.NewReader(testJPEG(t)),
	})

	require.NoError(t, err)
	require.Equal(t, "user-1", avatar.UserID)
	require.Equal(t, "image/jpeg", avatar.MimeType)
	require.Equal(t, domain.ProcessingStatusPending, avatar.ProcessingStatus)
	require.Contains(t, storage.objects, avatar.S3Key)
	require.Len(t, publisher.uploads, 1)
	require.Equal(t, avatar.ID, publisher.uploads[0].AvatarID)
}

func TestAvatarServiceUploadValidationAndStorageErrors(t *testing.T) {
	t.Run("requires user id", func(t *testing.T) {
		service := NewAvatarService(newMemoryRepo(), newMemoryStorage(), &memoryPublisher{}, 10<<20)

		_, err := service.Upload(context.Background(), UploadInput{Reader: bytes.NewReader(testJPEG(t))})

		require.ErrorContains(t, err, "user id is required")
	})

	t.Run("rejects declared oversized file", func(t *testing.T) {
		service := NewAvatarService(newMemoryRepo(), newMemoryStorage(), &memoryPublisher{}, 10)

		_, err := service.Upload(context.Background(), UploadInput{
			UserID: "user-1",
			Size:   11,
			Reader: bytes.NewReader(testJPEG(t)),
		})

		require.ErrorIs(t, err, ErrFileTooLarge)
	})

	t.Run("returns storage upload error", func(t *testing.T) {
		storage := newMemoryStorage()
		storage.uploadErr = errors.New("s3 down")
		service := NewAvatarService(newMemoryRepo(), storage, &memoryPublisher{}, 10<<20)

		_, err := service.Upload(context.Background(), UploadInput{
			UserID: "user-1",
			Reader: bytes.NewReader(testJPEG(t)),
		})

		require.ErrorContains(t, err, "s3 down")
	})

	t.Run("cleans uploaded object after db error", func(t *testing.T) {
		repo := newMemoryRepo()
		repo.createErr = errors.New("db down")
		storage := newMemoryStorage()
		service := NewAvatarService(repo, storage, &memoryPublisher{}, 10<<20)

		_, err := service.Upload(context.Background(), UploadInput{
			UserID: "user-1",
			Reader: bytes.NewReader(testJPEG(t)),
		})

		require.ErrorContains(t, err, "db down")
		require.Empty(t, storage.objects)
	})
}

func TestAvatarServiceUploadRejectsUnsupportedFormat(t *testing.T) {
	service := NewAvatarService(newMemoryRepo(), newMemoryStorage(), &memoryPublisher{}, 10<<20)

	_, err := service.Upload(context.Background(), UploadInput{
		UserID:   "user-1",
		FileName: "avatar.txt",
		Reader:   bytes.NewBufferString("not an image"),
	})

	require.Error(t, err)
}

func TestAvatarServiceUploadKeepsOutboxWhenPublishFails(t *testing.T) {
	repo := newMemoryRepo()
	storage := newMemoryStorage()
	publisher := &memoryPublisher{uploadErr: errors.New("rabbit down")}
	service := NewAvatarService(repo, storage, publisher, 10<<20)

	avatar, err := service.Upload(context.Background(), UploadInput{
		UserID:   "user-1",
		FileName: "avatar.jpg",
		Reader:   bytes.NewReader(testJPEG(t)),
	})

	require.NoError(t, err)
	require.NotEmpty(t, avatar.ID)
	require.Contains(t, storage.objects, avatar.S3Key)
	require.Len(t, repo.outbox, 1)
	require.False(t, repo.outbox[0].published)
}

func TestAvatarServiceReadMethods(t *testing.T) {
	repo := newMemoryRepo()
	storage := newMemoryStorage()
	service := NewAvatarService(repo, storage, &memoryPublisher{}, 10<<20)
	avatar, err := service.Upload(context.Background(), UploadInput{
		UserID:   "user-1",
		FileName: "avatar.jpg",
		Reader:   bytes.NewReader(testJPEG(t)),
	})
	require.NoError(t, err)

	got, body, contentType, err := service.Get(context.Background(), avatar.ID)
	require.NoError(t, err)
	require.Equal(t, avatar.ID, got.ID)
	require.Equal(t, "image/jpeg", contentType)
	require.NoError(t, body.Close())

	got, body, _, err = service.GetLatestForUser(context.Background(), "user-1")
	require.NoError(t, err)
	require.Equal(t, avatar.ID, got.ID)
	require.NoError(t, body.Close())

	got, err = service.Metadata(context.Background(), avatar.ID)
	require.NoError(t, err)
	require.Equal(t, avatar.ID, got.ID)

	got, err = service.LatestMetadata(context.Background(), "user-1")
	require.NoError(t, err)
	require.Equal(t, avatar.ID, got.ID)

	items, err := service.List(context.Background(), "user-1")
	require.NoError(t, err)
	require.Len(t, items, 1)
}

func TestAvatarServiceFlushOutboxPublishesPendingMessages(t *testing.T) {
	repo := newMemoryRepo()
	publisher := &memoryPublisher{}
	service := NewAvatarService(repo, newMemoryStorage(), publisher, 10<<20)

	uploadEvent := domain.AvatarUploadEvent{
		MessageID: "message-1",
		AvatarID:  "avatar-1",
		UserID:    "user-1",
		S3Key:     "avatars/avatar-1/original.jpg",
	}
	uploadPayload, err := json.Marshal(uploadEvent)
	require.NoError(t, err)
	deleteEvent := domain.AvatarDeleteEvent{
		MessageID: "message-2",
		AvatarID:  "avatar-2",
		S3Keys:    []string{"avatars/avatar-2/original.jpg"},
	}
	deletePayload, err := json.Marshal(deleteEvent)
	require.NoError(t, err)
	repo.outbox = append(repo.outbox, memoryOutboxMessage{
		message: domain.OutboxMessage{ID: uploadEvent.MessageID, RoutingKey: "avatar.uploaded", Payload: uploadPayload},
	}, memoryOutboxMessage{
		message: domain.OutboxMessage{ID: deleteEvent.MessageID, RoutingKey: "avatar.deleted", Payload: deletePayload},
	})

	require.NoError(t, service.FlushOutbox(context.Background(), 10))

	require.Len(t, publisher.uploads, 1)
	require.Equal(t, uploadEvent.AvatarID, publisher.uploads[0].AvatarID)
	require.Len(t, publisher.deletes, 1)
	require.Equal(t, deleteEvent.AvatarID, publisher.deletes[0].AvatarID)
	require.True(t, repo.outbox[0].published)
	require.True(t, repo.outbox[1].published)
}

func TestAvatarServiceOutboxErrors(t *testing.T) {
	t.Run("unknown routing key", func(t *testing.T) {
		service := NewAvatarService(newMemoryRepo(), newMemoryStorage(), &memoryPublisher{}, 10<<20)

		err := service.PublishOutboxMessage(context.Background(), domain.OutboxMessage{RoutingKey: "unknown"})

		require.ErrorContains(t, err, "unknown outbox routing key")
	})

	t.Run("invalid payload", func(t *testing.T) {
		service := NewAvatarService(newMemoryRepo(), newMemoryStorage(), &memoryPublisher{}, 10<<20)

		err := service.PublishOutboxMessage(context.Background(), domain.OutboxMessage{
			RoutingKey: "avatar.uploaded",
			Payload:    []byte("{"),
		})

		require.Error(t, err)
	})

	t.Run("publish error", func(t *testing.T) {
		service := NewAvatarService(newMemoryRepo(), newMemoryStorage(), &memoryPublisher{uploadErr: errors.New("rabbit down")}, 10<<20)
		payload, err := json.Marshal(domain.AvatarUploadEvent{MessageID: "msg-1"})
		require.NoError(t, err)

		err = service.PublishOutboxMessage(context.Background(), domain.OutboxMessage{
			ID:         "msg-1",
			RoutingKey: "avatar.uploaded",
			Payload:    payload,
		})

		require.ErrorContains(t, err, "rabbit down")
	})

	t.Run("pending outbox error", func(t *testing.T) {
		repo := newMemoryRepo()
		repo.pendingErr = errors.New("db down")
		service := NewAvatarService(repo, newMemoryStorage(), &memoryPublisher{}, 10<<20)

		err := service.FlushOutbox(context.Background(), 10)

		require.ErrorContains(t, err, "db down")
	})
}

func TestAvatarServiceDeleteChecksOwnerAndPublishesDelete(t *testing.T) {
	repo := newMemoryRepo()
	storage := newMemoryStorage()
	publisher := &memoryPublisher{}
	service := NewAvatarService(repo, storage, publisher, 10<<20)
	avatar, err := service.Upload(context.Background(), UploadInput{
		UserID:   "owner",
		FileName: "avatar.jpg",
		Reader:   bytes.NewReader(testJPEG(t)),
	})
	require.NoError(t, err)

	err = service.Delete(context.Background(), avatar.ID, "other")
	require.ErrorIs(t, err, domain.ErrForbidden)

	err = service.Delete(context.Background(), avatar.ID, "owner")
	require.NoError(t, err)
	require.Len(t, publisher.deletes, 1)
	require.Equal(t, avatar.ID, publisher.deletes[0].AvatarID)
}

func TestAvatarServiceDeleteIncludesThumbnailKeysAndErrors(t *testing.T) {
	t.Run("includes thumbnail keys", func(t *testing.T) {
		repo := newMemoryRepo()
		publisher := &memoryPublisher{}
		service := NewAvatarService(repo, newMemoryStorage(), publisher, 10<<20)
		avatar := domain.Avatar{
			ID:        "avatar-1",
			UserID:    "user-1",
			S3Key:     "original.jpg",
			UpdatedAt: domainTime(),
			ThumbnailS3Keys: []domain.Thumbnail{
				{S3Key: "thumb-100.jpg"},
				{},
				{S3Key: "thumb-300.jpg"},
			},
		}
		repo.avatars[avatar.ID] = avatar

		require.NoError(t, service.Delete(context.Background(), avatar.ID, "user-1"))

		require.Len(t, publisher.deletes, 1)
		require.Equal(t, []string{"original.jpg", "thumb-100.jpg", "thumb-300.jpg"}, publisher.deletes[0].S3Keys)
	})

	t.Run("publisher delete error", func(t *testing.T) {
		repo := newMemoryRepo()
		publisher := &memoryPublisher{deleteErr: errors.New("rabbit down")}
		service := NewAvatarService(repo, newMemoryStorage(), publisher, 10<<20)
		repo.avatars["avatar-1"] = domain.Avatar{ID: "avatar-1", UserID: "user-1", S3Key: "original.jpg", UpdatedAt: domainTime()}

		err := service.Delete(context.Background(), "avatar-1", "user-1")

		require.NoError(t, err)
		require.Len(t, repo.outbox, 1)
		require.False(t, repo.outbox[0].published)
	})
}

func TestExtensionByMime(t *testing.T) {
	require.Equal(t, ".jpg", extensionByMime("image/jpeg", ""))
	require.Equal(t, ".png", extensionByMime("image/png", ""))
	require.Equal(t, ".webp", extensionByMime("image/webp", ""))
	require.Equal(t, ".gif", extensionByMime("application/octet-stream", ".gif"))
	require.Equal(t, ".img", extensionByMime("application/octet-stream", ""))
}

type memoryRepo struct {
	avatars    map[string]domain.Avatar
	outbox     []memoryOutboxMessage
	createErr  error
	pendingErr error
}

type memoryOutboxMessage struct {
	message   domain.OutboxMessage
	published bool
}

func newMemoryRepo() *memoryRepo {
	return &memoryRepo{avatars: map[string]domain.Avatar{}}
}

func (r *memoryRepo) CreateWithUploadEvent(_ context.Context, avatar domain.Avatar, event domain.AvatarUploadEvent) (domain.Avatar, error) {
	if r.createErr != nil {
		return domain.Avatar{}, r.createErr
	}
	r.avatars[avatar.ID] = avatar
	payload, err := json.Marshal(event)
	if err != nil {
		return domain.Avatar{}, err
	}
	r.outbox = append(r.outbox, memoryOutboxMessage{
		message: domain.OutboxMessage{ID: event.MessageID, RoutingKey: "avatar.uploaded", Payload: payload},
	})
	return avatar, nil
}

func (r *memoryRepo) GetByID(_ context.Context, id string) (domain.Avatar, error) {
	avatar, ok := r.avatars[id]
	if !ok || avatar.DeletedAt != nil {
		return domain.Avatar{}, domain.ErrNotFound
	}
	return avatar, nil
}

func (r *memoryRepo) GetLatestByUserID(_ context.Context, userID string) (domain.Avatar, error) {
	var items []domain.Avatar
	for _, avatar := range r.avatars {
		if avatar.UserID == userID && avatar.DeletedAt == nil {
			items = append(items, avatar)
		}
	}
	if len(items) == 0 {
		return domain.Avatar{}, domain.ErrNotFound
	}
	sort.Slice(items, func(i, j int) bool { return items[i].CreatedAt.After(items[j].CreatedAt) })
	return items[0], nil
}

func (r *memoryRepo) ListByUserID(_ context.Context, userID string) ([]domain.Avatar, error) {
	var items []domain.Avatar
	for _, avatar := range r.avatars {
		if avatar.UserID == userID && avatar.DeletedAt == nil {
			items = append(items, avatar)
		}
	}
	return items, nil
}

func (r *memoryRepo) SoftDelete(_ context.Context, id, userID string) (domain.Avatar, error) {
	return r.softDelete(id, userID)
}

func (r *memoryRepo) SoftDeleteWithDeleteEvent(_ context.Context, id, userID, messageID string) (domain.Avatar, domain.AvatarDeleteEvent, error) {
	avatar, err := r.softDelete(id, userID)
	if err != nil {
		return domain.Avatar{}, domain.AvatarDeleteEvent{}, err
	}
	event := domain.AvatarDeleteEvent{
		MessageID: messageID,
		AvatarID:  avatar.ID,
		S3Keys:    avatarS3Keys(avatar),
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return domain.Avatar{}, domain.AvatarDeleteEvent{}, err
	}
	r.outbox = append(r.outbox, memoryOutboxMessage{
		message: domain.OutboxMessage{ID: event.MessageID, RoutingKey: "avatar.deleted", Payload: payload},
	})
	return avatar, event, nil
}

func (r *memoryRepo) softDelete(id, userID string) (domain.Avatar, error) {
	avatar, err := r.GetByID(context.Background(), id)
	if err != nil {
		return domain.Avatar{}, err
	}
	if avatar.UserID != userID {
		return domain.Avatar{}, domain.ErrForbidden
	}
	now := avatar.UpdatedAt
	avatar.DeletedAt = &now
	r.avatars[id] = avatar
	return avatar, nil
}

func avatarS3Keys(avatar domain.Avatar) []string {
	keys := []string{avatar.S3Key}
	for _, thumb := range avatar.ThumbnailS3Keys {
		if thumb.S3Key != "" {
			keys = append(keys, thumb.S3Key)
		}
	}
	return keys
}

func (r *memoryRepo) MarkProcessing(_ context.Context, id string) (bool, error) { return true, nil }
func (r *memoryRepo) UpdateProcessed(_ context.Context, id string, thumbnails []domain.Thumbnail) error {
	return nil
}
func (r *memoryRepo) UpdateProcessingFailed(_ context.Context, id string) error { return nil }
func (r *memoryRepo) ProcessedMessage(_ context.Context, messageID string) (bool, error) {
	return false, nil
}
func (r *memoryRepo) SaveProcessedMessage(_ context.Context, messageID string) error { return nil }
func (r *memoryRepo) MarkOutboxPublished(_ context.Context, messageID string) error {
	for i := range r.outbox {
		if r.outbox[i].message.ID == messageID {
			r.outbox[i].published = true
		}
	}
	return nil
}
func (r *memoryRepo) PendingOutbox(_ context.Context, limit int) ([]domain.OutboxMessage, error) {
	if r.pendingErr != nil {
		return nil, r.pendingErr
	}
	var messages []domain.OutboxMessage
	for _, item := range r.outbox {
		if !item.published {
			messages = append(messages, item.message)
			if len(messages) == limit {
				break
			}
		}
	}
	return messages, nil
}
func (r *memoryRepo) Ping(_ context.Context) error { return nil }

type memoryStorage struct {
	objects   map[string][]byte
	uploadErr error
}

func newMemoryStorage() *memoryStorage {
	return &memoryStorage{objects: map[string][]byte{}}
}

func (s *memoryStorage) Upload(_ context.Context, key, contentType string, size int64, body io.Reader) error {
	if s.uploadErr != nil {
		return s.uploadErr
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	s.objects[key] = data
	return nil
}

func (s *memoryStorage) Download(_ context.Context, key string) (io.ReadCloser, string, error) {
	data, ok := s.objects[key]
	if !ok {
		return nil, "", domain.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), "image/jpeg", nil
}

func (s *memoryStorage) Delete(_ context.Context, key string) error {
	delete(s.objects, key)
	return nil
}
func (s *memoryStorage) PresignedGetURL(_ context.Context, key string) (string, error) {
	return "/objects/" + key, nil
}
func (s *memoryStorage) Ping(_ context.Context) error { return nil }

type memoryPublisher struct {
	uploads   []domain.AvatarUploadEvent
	deletes   []domain.AvatarDeleteEvent
	uploadErr error
	deleteErr error
}

func (p *memoryPublisher) PublishUpload(_ context.Context, event domain.AvatarUploadEvent) error {
	if p.uploadErr != nil {
		return p.uploadErr
	}
	p.uploads = append(p.uploads, event)
	return nil
}
func (p *memoryPublisher) PublishDelete(_ context.Context, event domain.AvatarDeleteEvent) error {
	if p.deleteErr != nil {
		return p.deleteErr
	}
	p.deletes = append(p.deletes, event)
	return nil
}
func (p *memoryPublisher) Ping(_ context.Context) error { return nil }

func domainTime() time.Time {
	return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
}

func testJPEG(t *testing.T) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 10, 10))
	for y := 0; y < 10; y++ {
		for x := 0; x < 10; x++ {
			img.Set(x, y, color.RGBA{R: 80, G: 120, B: 180, A: 255})
		}
	}
	var buf bytes.Buffer
	require.NoError(t, jpeg.Encode(&buf, img, nil))
	return buf.Bytes()
}
