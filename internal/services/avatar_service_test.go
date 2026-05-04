package services

import (
	"bytes"
	"context"
	"image"
	"image/color"
	"image/jpeg"
	"io"
	"sort"
	"testing"

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

func TestAvatarServiceUploadRejectsUnsupportedFormat(t *testing.T) {
	service := NewAvatarService(newMemoryRepo(), newMemoryStorage(), &memoryPublisher{}, 10<<20)

	_, err := service.Upload(context.Background(), UploadInput{
		UserID:   "user-1",
		FileName: "avatar.txt",
		Reader:   bytes.NewBufferString("not an image"),
	})

	require.Error(t, err)
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
	require.ErrorIs(t, err, ErrForbidden)

	err = service.Delete(context.Background(), avatar.ID, "owner")
	require.NoError(t, err)
	require.Len(t, publisher.deletes, 1)
	require.Equal(t, avatar.ID, publisher.deletes[0].AvatarID)
}

type memoryRepo struct {
	avatars map[string]domain.Avatar
}

func newMemoryRepo() *memoryRepo {
	return &memoryRepo{avatars: map[string]domain.Avatar{}}
}

func (r *memoryRepo) Create(_ context.Context, avatar domain.Avatar) (domain.Avatar, error) {
	r.avatars[avatar.ID] = avatar
	return avatar, nil
}

func (r *memoryRepo) GetByID(_ context.Context, id string) (domain.Avatar, error) {
	avatar, ok := r.avatars[id]
	if !ok || avatar.DeletedAt != nil {
		return domain.Avatar{}, ErrNotFound
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
		return domain.Avatar{}, ErrNotFound
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
	avatar, err := r.GetByID(context.Background(), id)
	if err != nil {
		return domain.Avatar{}, err
	}
	if avatar.UserID != userID {
		return domain.Avatar{}, ErrForbidden
	}
	now := avatar.UpdatedAt
	avatar.DeletedAt = &now
	r.avatars[id] = avatar
	return avatar, nil
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
func (r *memoryRepo) Ping(_ context.Context) error                                   { return nil }

type memoryStorage struct {
	objects map[string][]byte
}

func newMemoryStorage() *memoryStorage {
	return &memoryStorage{objects: map[string][]byte{}}
}

func (s *memoryStorage) Upload(_ context.Context, key, contentType string, size int64, body io.Reader) error {
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
		return nil, "", ErrNotFound
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
	uploads []domain.AvatarUploadEvent
	deletes []domain.AvatarDeleteEvent
}

func (p *memoryPublisher) PublishUpload(_ context.Context, event domain.AvatarUploadEvent) error {
	p.uploads = append(p.uploads, event)
	return nil
}
func (p *memoryPublisher) PublishDelete(_ context.Context, event domain.AvatarDeleteEvent) error {
	p.deletes = append(p.deletes, event)
	return nil
}
func (p *memoryPublisher) Ping(_ context.Context) error { return nil }

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
