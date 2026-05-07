package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"image"
	"image/color"
	"image/jpeg"
	"io"
	"testing"
	"time"

	"gophprofile/internal/domain"
	"gophprofile/internal/queue"
	imgpkg "gophprofile/pkg/imaging"

	amqp "github.com/rabbitmq/amqp091-go"
)

var errTest = errors.New("test error")

type fakeRepo struct {
	processed          bool
	processedErr       error
	markProcessing     bool
	markErr            error
	updateProcessedErr error
	updateFailedErr    error
	saveErr            error

	processedCalls    int
	markCalls         int
	updateFailedCalls int
	updateProcessedID string
	updatedThumbs     []domain.Thumbnail
	savedMessages     []string
}

func (r *fakeRepo) CreateWithUploadEvent(context.Context, domain.Avatar, domain.AvatarUploadEvent) (domain.Avatar, error) {
	return domain.Avatar{}, nil
}

func (r *fakeRepo) GetByID(context.Context, string) (domain.Avatar, error) {
	return domain.Avatar{}, nil
}

func (r *fakeRepo) GetLatestByUserID(context.Context, string) (domain.Avatar, error) {
	return domain.Avatar{}, nil
}

func (r *fakeRepo) ListByUserID(context.Context, string) ([]domain.Avatar, error) {
	return nil, nil
}

func (r *fakeRepo) SoftDelete(context.Context, string, string) (domain.Avatar, error) {
	return domain.Avatar{}, nil
}

func (r *fakeRepo) SoftDeleteWithDeleteEvent(context.Context, string, string, string) (domain.Avatar, domain.AvatarDeleteEvent, error) {
	return domain.Avatar{}, domain.AvatarDeleteEvent{}, nil
}

func (r *fakeRepo) MarkProcessing(context.Context, string) (bool, error) {
	r.markCalls++
	return r.markProcessing, r.markErr
}

func (r *fakeRepo) UpdateProcessed(_ context.Context, id string, thumbnails []domain.Thumbnail) error {
	r.updateProcessedID = id
	r.updatedThumbs = append([]domain.Thumbnail(nil), thumbnails...)
	return r.updateProcessedErr
}

func (r *fakeRepo) UpdateProcessingFailed(context.Context, string) error {
	r.updateFailedCalls++
	return r.updateFailedErr
}

func (r *fakeRepo) ProcessedMessage(context.Context, string) (bool, error) {
	r.processedCalls++
	return r.processed, r.processedErr
}

func (r *fakeRepo) SaveProcessedMessage(_ context.Context, messageID string) error {
	r.savedMessages = append(r.savedMessages, messageID)
	return r.saveErr
}

func (r *fakeRepo) MarkOutboxPublished(context.Context, string) error {
	return nil
}

func (r *fakeRepo) PendingOutbox(context.Context, int) ([]domain.OutboxMessage, error) {
	return nil, nil
}

func (r *fakeRepo) Ping(context.Context) error {
	return nil
}

type fakeStorage struct {
	downloadData map[string][]byte
	downloadErr  error
	uploadErr    error
	deleteErr    error
	presignErr   error

	uploaded []string
	deleted  []string
}

func (s *fakeStorage) Upload(_ context.Context, key, _ string, _ int64, body io.Reader) error {
	if s.uploadErr != nil {
		return s.uploadErr
	}
	if _, err := io.ReadAll(body); err != nil {
		return err
	}
	s.uploaded = append(s.uploaded, key)
	return nil
}

func (s *fakeStorage) Download(_ context.Context, key string) (io.ReadCloser, string, error) {
	if s.downloadErr != nil {
		return nil, "", s.downloadErr
	}
	data := s.downloadData[key]
	return io.NopCloser(bytes.NewReader(data)), "image/jpeg", nil
}

func (s *fakeStorage) Delete(_ context.Context, key string) error {
	if s.deleteErr != nil {
		return s.deleteErr
	}
	s.deleted = append(s.deleted, key)
	return nil
}

func (s *fakeStorage) PresignedGetURL(_ context.Context, key string) (string, error) {
	if s.presignErr != nil {
		return "", s.presignErr
	}
	return "http://minio/" + key, nil
}

func (s *fakeStorage) Ping(context.Context) error {
	return nil
}

type fakeEvents struct {
	deliveries <-chan amqp.Delivery
	errorsLeft int
	calls      int
}

func (e *fakeEvents) Consume() (<-chan amqp.Delivery, error) {
	e.calls++
	if e.errorsLeft > 0 {
		e.errorsLeft--
		return nil, errTest
	}
	return e.deliveries, nil
}

type fakeAcknowledger struct {
	acked    bool
	nacked   bool
	rejected bool
	requeue  bool
}

func (a *fakeAcknowledger) Ack(uint64, bool) error {
	a.acked = true
	return nil
}

func (a *fakeAcknowledger) Nack(_ uint64, _ bool, requeue bool) error {
	a.nacked = true
	a.requeue = requeue
	return nil
}

func (a *fakeAcknowledger) Reject(_ uint64, requeue bool) error {
	a.rejected = true
	a.requeue = requeue
	return nil
}

func TestNew(t *testing.T) {
	w := New(&fakeRepo{}, &fakeStorage{}, nil)
	if w.repo == nil || w.storage == nil {
		t.Fatal("expected worker dependencies to be assigned")
	}
	if w.events != nil {
		t.Fatal("expected nil events when nil queue is passed")
	}

	queueClient := &queue.RabbitMQ{}
	w = New(&fakeRepo{}, &fakeStorage{}, queueClient)
	if w.events == nil {
		t.Fatal("expected non-nil events when queue is passed")
	}
}

func TestRunReturnsContextErrorWhenConsumeFailsAndContextDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	events := &fakeEvents{errorsLeft: 1}
	w := &Worker{repo: &fakeRepo{}, storage: &fakeStorage{}, events: events}

	err := w.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
}

func TestRunRetriesConsumeAfterTemporaryError(t *testing.T) {
	deliveries := make(chan amqp.Delivery)
	events := &fakeEvents{deliveries: deliveries, errorsLeft: 1}
	w := &Worker{repo: &fakeRepo{}, storage: &fakeStorage{}, events: events}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		deadline := time.After(2 * time.Second)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-deadline:
				cancel()
				return
			case <-ticker.C:
				if events.calls >= 2 {
					cancel()
					return
				}
			}
		}
	}()

	err := w.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
	if events.calls != 2 {
		t.Fatalf("expected two consume attempts, got %d", events.calls)
	}
}

func TestRunReturnsConsumeError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	deliveries := make(chan amqp.Delivery)
	events := &fakeEvents{deliveries: deliveries}
	w := &Worker{repo: &fakeRepo{}, storage: &fakeStorage{}, events: events}

	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	err := w.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
}

func TestConsumeClosedChannelReturnsNil(t *testing.T) {
	deliveries := make(chan amqp.Delivery)
	close(deliveries)
	w := &Worker{repo: &fakeRepo{}, storage: &fakeStorage{}}

	if err := w.consume(context.Background(), deliveries); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestConsumeHandlesDelivery(t *testing.T) {
	deliveries := make(chan amqp.Delivery, 1)
	closeAfterSend := func() {
		deliveries <- delivery(t, queue.RoutingAvatarDeleted, domain.AvatarDeleteEvent{
			MessageID: "msg-1",
			S3Keys:    []string{"original.jpg"},
		}, &fakeAcknowledger{})
		close(deliveries)
	}
	closeAfterSend()

	repo := &fakeRepo{}
	storage := &fakeStorage{}
	w := &Worker{repo: repo, storage: storage}

	if err := w.consume(context.Background(), deliveries); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if len(storage.deleted) != 1 || storage.deleted[0] != "original.jpg" {
		t.Fatalf("expected delete event to be handled, got %#v", storage.deleted)
	}
}

func TestHandleDeliveryAckUpload(t *testing.T) {
	ack := &fakeAcknowledger{}
	repo := &fakeRepo{markProcessing: true}
	storage := imageStorage("original.jpg", jpegBytes(t))
	w := &Worker{repo: repo, storage: storage}

	w.handleDelivery(context.Background(), delivery(t, queue.RoutingAvatarUploaded, domain.AvatarUploadEvent{
		MessageID: "msg-1",
		AvatarID:  "avatar-1",
		S3Key:     "original.jpg",
	}, ack))

	if !ack.acked || ack.nacked {
		t.Fatalf("expected ack only, got ack=%v nack=%v", ack.acked, ack.nacked)
	}
}

func TestHandleDeliveryAckDelete(t *testing.T) {
	ack := &fakeAcknowledger{}
	storage := &fakeStorage{}
	w := &Worker{repo: &fakeRepo{}, storage: storage}

	w.handleDelivery(context.Background(), delivery(t, queue.RoutingAvatarDeleted, domain.AvatarDeleteEvent{
		MessageID: "msg-1",
		S3Keys:    []string{"a", "b"},
	}, ack))

	if !ack.acked || ack.nacked {
		t.Fatalf("expected ack only, got ack=%v nack=%v", ack.acked, ack.nacked)
	}
	if len(storage.deleted) != 2 {
		t.Fatalf("expected two deleted keys, got %d", len(storage.deleted))
	}
}

func TestHandleDeliveryNackOnBadJSON(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ack := &fakeAcknowledger{}
	w := &Worker{repo: &fakeRepo{}, storage: &fakeStorage{}}

	w.handleDelivery(ctx, amqp.Delivery{
		RoutingKey:   queue.RoutingAvatarUploaded,
		Body:         []byte("{"),
		Acknowledger: ack,
		DeliveryTag:  1,
	})

	if !ack.nacked || ack.acked || ack.requeue {
		t.Fatalf("expected nack without requeue, got ack=%v nack=%v requeue=%v", ack.acked, ack.nacked, ack.requeue)
	}
}

func TestHandleDeliveryNackOnBadDeleteJSON(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ack := &fakeAcknowledger{}
	w := &Worker{repo: &fakeRepo{}, storage: &fakeStorage{}}

	w.handleDelivery(ctx, amqp.Delivery{
		RoutingKey:   queue.RoutingAvatarDeleted,
		Body:         []byte("{"),
		Acknowledger: ack,
		DeliveryTag:  1,
	})

	if !ack.nacked || ack.acked || ack.requeue {
		t.Fatalf("expected nack without requeue, got ack=%v nack=%v requeue=%v", ack.acked, ack.nacked, ack.requeue)
	}
}

func TestHandleDeliveryNackOnUnknownRoutingKey(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ack := &fakeAcknowledger{}
	w := &Worker{repo: &fakeRepo{}, storage: &fakeStorage{}}

	w.handleDelivery(ctx, amqp.Delivery{
		RoutingKey:   "unknown",
		Body:         []byte("{}"),
		Acknowledger: ack,
		DeliveryTag:  1,
	})

	if !ack.nacked || ack.acked {
		t.Fatalf("expected nack only, got ack=%v nack=%v", ack.acked, ack.nacked)
	}
}

func TestHandleUploadEvent(t *testing.T) {
	baseEvent := domain.AvatarUploadEvent{MessageID: "msg-1", AvatarID: "avatar-1", S3Key: "original.jpg"}

	tests := []struct {
		name        string
		repo        *fakeRepo
		storage     *fakeStorage
		event       domain.AvatarUploadEvent
		wantErr     bool
		wantFailed  int
		wantSaved   int
		wantUploads int
	}{
		{
			name:    "processed message check error",
			repo:    &fakeRepo{processedErr: errTest},
			storage: imageStorage("original.jpg", jpegBytes(t)),
			event:   baseEvent,
			wantErr: true,
		},
		{
			name:    "already processed message skips work",
			repo:    &fakeRepo{processed: true},
			storage: imageStorage("original.jpg", jpegBytes(t)),
			event:   baseEvent,
		},
		{
			name:    "mark processing error",
			repo:    &fakeRepo{markErr: errTest},
			storage: imageStorage("original.jpg", jpegBytes(t)),
			event:   baseEvent,
			wantErr: true,
		},
		{
			name:      "not locked saves idempotency marker",
			repo:      &fakeRepo{markProcessing: false},
			storage:   imageStorage("original.jpg", jpegBytes(t)),
			event:     baseEvent,
			wantSaved: 1,
		},
		{
			name:      "not locked save marker error",
			repo:      &fakeRepo{markProcessing: false, saveErr: errTest},
			storage:   imageStorage("original.jpg", jpegBytes(t)),
			event:     baseEvent,
			wantErr:   true,
			wantSaved: 1,
		},
		{
			name:    "not locked without message returns nil",
			repo:    &fakeRepo{markProcessing: false},
			storage: imageStorage("original.jpg", jpegBytes(t)),
			event:   domain.AvatarUploadEvent{AvatarID: "avatar-1", S3Key: "original.jpg"},
		},
		{
			name:       "download error marks failed",
			repo:       &fakeRepo{markProcessing: true},
			storage:    &fakeStorage{downloadErr: errTest},
			event:      baseEvent,
			wantErr:    true,
			wantFailed: 1,
		},
		{
			name:       "download error with mark failure returns combined error",
			repo:       &fakeRepo{markProcessing: true, updateFailedErr: errTest},
			storage:    &fakeStorage{downloadErr: errors.New("download")},
			event:      baseEvent,
			wantErr:    true,
			wantFailed: 1,
		},
		{
			name:       "decode error marks failed",
			repo:       &fakeRepo{markProcessing: true},
			storage:    imageStorage("original.jpg", []byte("bad image")),
			event:      baseEvent,
			wantErr:    true,
			wantFailed: 1,
		},
		{
			name:       "decode error with mark failure returns combined error",
			repo:       &fakeRepo{markProcessing: true, updateFailedErr: errTest},
			storage:    imageStorage("original.jpg", []byte("bad image")),
			event:      baseEvent,
			wantErr:    true,
			wantFailed: 1,
		},
		{
			name:       "upload thumbnail error marks failed",
			repo:       &fakeRepo{markProcessing: true},
			storage:    imageStorageWithUploadErr("original.jpg", jpegBytes(t), errTest),
			event:      baseEvent,
			wantErr:    true,
			wantFailed: 1,
		},
		{
			name:       "upload thumbnail error with mark failure returns combined error",
			repo:       &fakeRepo{markProcessing: true, updateFailedErr: errTest},
			storage:    imageStorageWithUploadErr("original.jpg", jpegBytes(t), errors.New("upload")),
			event:      baseEvent,
			wantErr:    true,
			wantFailed: 1,
		},
		{
			name:        "presign error marks failed",
			repo:        &fakeRepo{markProcessing: true},
			storage:     imageStorageWithPresignErr("original.jpg", jpegBytes(t), errTest),
			event:       baseEvent,
			wantErr:     true,
			wantFailed:  1,
			wantUploads: 1,
		},
		{
			name:        "presign error with mark failure returns combined error",
			repo:        &fakeRepo{markProcessing: true, updateFailedErr: errTest},
			storage:     imageStorageWithPresignErr("original.jpg", jpegBytes(t), errors.New("presign")),
			event:       baseEvent,
			wantErr:     true,
			wantFailed:  1,
			wantUploads: 1,
		},
		{
			name:        "update processed error",
			repo:        &fakeRepo{markProcessing: true, updateProcessedErr: errTest},
			storage:     imageStorage("original.jpg", jpegBytes(t)),
			event:       baseEvent,
			wantErr:     true,
			wantUploads: 2,
		},
		{
			name:        "save processed message error",
			repo:        &fakeRepo{markProcessing: true, saveErr: errTest},
			storage:     imageStorage("original.jpg", jpegBytes(t)),
			event:       baseEvent,
			wantErr:     true,
			wantSaved:   1,
			wantUploads: 2,
		},
		{
			name:        "success with message",
			repo:        &fakeRepo{markProcessing: true},
			storage:     imageStorage("original.jpg", jpegBytes(t)),
			event:       baseEvent,
			wantSaved:   1,
			wantUploads: 2,
		},
		{
			name:        "success without message",
			repo:        &fakeRepo{markProcessing: true},
			storage:     imageStorage("original.jpg", jpegBytes(t)),
			event:       domain.AvatarUploadEvent{AvatarID: "avatar-1", S3Key: "original.jpg"},
			wantUploads: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Worker{repo: tt.repo, storage: tt.storage}
			err := w.HandleUploadEvent(context.Background(), tt.event)
			if (err != nil) != tt.wantErr {
				t.Fatalf("HandleUploadEvent() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.repo.updateFailedCalls != tt.wantFailed {
				t.Fatalf("expected %d failed marks, got %d", tt.wantFailed, tt.repo.updateFailedCalls)
			}
			if len(tt.repo.savedMessages) != tt.wantSaved {
				t.Fatalf("expected %d saved messages, got %d", tt.wantSaved, len(tt.repo.savedMessages))
			}
			if len(tt.storage.uploaded) != tt.wantUploads {
				t.Fatalf("expected %d uploads, got %d", tt.wantUploads, len(tt.storage.uploaded))
			}
			if !tt.wantErr && tt.wantUploads == 2 {
				if tt.repo.updateProcessedID != tt.event.AvatarID {
					t.Fatalf("expected avatar %q updated, got %q", tt.event.AvatarID, tt.repo.updateProcessedID)
				}
				if len(tt.repo.updatedThumbs) != 2 {
					t.Fatalf("expected two thumbnails, got %d", len(tt.repo.updatedThumbs))
				}
			}
		})
	}
}

func TestHandleUploadEventResizeErrors(t *testing.T) {
	originalResize := resizeJPEG
	t.Cleanup(func() { resizeJPEG = originalResize })

	for _, tt := range []struct {
		name          string
		updateFailErr error
	}{
		{name: "marks failed"},
		{name: "returns combined error when mark failed too", updateFailErr: errTest},
	} {
		t.Run(tt.name, func(t *testing.T) {
			resizeJPEG = func(image.Image, int, int) ([]byte, error) {
				return nil, errors.New("resize")
			}
			repo := &fakeRepo{markProcessing: true, updateFailedErr: tt.updateFailErr}
			w := &Worker{repo: repo, storage: imageStorage("original.jpg", jpegBytes(t))}

			err := w.HandleUploadEvent(context.Background(), domain.AvatarUploadEvent{
				MessageID: "msg-1",
				AvatarID:  "avatar-1",
				S3Key:     "original.jpg",
			})
			if err == nil {
				t.Fatal("expected error")
			}
			if repo.updateFailedCalls != 1 {
				t.Fatalf("expected one failed mark, got %d", repo.updateFailedCalls)
			}
		})
	}
}

func TestHandleDeleteEvent(t *testing.T) {
	tests := []struct {
		name        string
		repo        *fakeRepo
		storage     *fakeStorage
		event       domain.AvatarDeleteEvent
		wantErr     bool
		wantDeleted []string
		wantSaved   int
	}{
		{
			name:    "processed message check error",
			repo:    &fakeRepo{processedErr: errTest},
			storage: &fakeStorage{},
			event:   domain.AvatarDeleteEvent{MessageID: "msg-1", S3Keys: []string{"a"}},
			wantErr: true,
		},
		{
			name:    "already processed skips delete",
			repo:    &fakeRepo{processed: true},
			storage: &fakeStorage{},
			event:   domain.AvatarDeleteEvent{MessageID: "msg-1", S3Keys: []string{"a"}},
		},
		{
			name:    "delete error",
			repo:    &fakeRepo{},
			storage: &fakeStorage{deleteErr: errTest},
			event:   domain.AvatarDeleteEvent{MessageID: "msg-1", S3Keys: []string{"a"}},
			wantErr: true,
		},
		{
			name:        "save marker error",
			repo:        &fakeRepo{saveErr: errTest},
			storage:     &fakeStorage{},
			event:       domain.AvatarDeleteEvent{MessageID: "msg-1", S3Keys: []string{"", "a"}},
			wantErr:     true,
			wantDeleted: []string{"a"},
			wantSaved:   1,
		},
		{
			name:        "success with message skips empty keys",
			repo:        &fakeRepo{},
			storage:     &fakeStorage{},
			event:       domain.AvatarDeleteEvent{MessageID: "msg-1", S3Keys: []string{"", "a", "b"}},
			wantDeleted: []string{"a", "b"},
			wantSaved:   1,
		},
		{
			name:        "success without message",
			repo:        &fakeRepo{},
			storage:     &fakeStorage{},
			event:       domain.AvatarDeleteEvent{S3Keys: []string{"a"}},
			wantDeleted: []string{"a"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Worker{repo: tt.repo, storage: tt.storage}
			err := w.HandleDeleteEvent(context.Background(), tt.event)
			if (err != nil) != tt.wantErr {
				t.Fatalf("HandleDeleteEvent() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !equalStrings(tt.storage.deleted, tt.wantDeleted) {
				t.Fatalf("deleted keys = %#v, want %#v", tt.storage.deleted, tt.wantDeleted)
			}
			if len(tt.repo.savedMessages) != tt.wantSaved {
				t.Fatalf("saved messages = %d, want %d", len(tt.repo.savedMessages), tt.wantSaved)
			}
		})
	}
}

func TestRetry(t *testing.T) {
	t.Run("success first try", func(t *testing.T) {
		calls := 0
		err := retry(context.Background(), 3, func() error {
			calls++
			return nil
		})
		if err != nil || calls != 1 {
			t.Fatalf("expected one successful call, got calls=%d err=%v", calls, err)
		}
	})

	t.Run("context canceled after error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := retry(ctx, 3, func() error { return errTest })
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context canceled, got %v", err)
		}
	})

	t.Run("returns last error", func(t *testing.T) {
		start := time.Now()
		err := retry(context.Background(), 1, func() error { return errTest })
		if !errors.Is(err, errTest) {
			t.Fatalf("expected test error, got %v", err)
		}
		if time.Since(start) < 300*time.Millisecond {
			t.Fatal("expected retry delay before returning last error")
		}
	})
}

func delivery(t *testing.T, routingKey string, payload any, ack amqp.Acknowledger) amqp.Delivery {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal delivery: %v", err)
	}
	return amqp.Delivery{
		RoutingKey:   routingKey,
		Body:         body,
		Acknowledger: ack,
		DeliveryTag:  1,
	}
}

func imageStorage(key string, data []byte) *fakeStorage {
	return &fakeStorage{downloadData: map[string][]byte{key: data}}
}

func imageStorageWithUploadErr(key string, data []byte, err error) *fakeStorage {
	storage := imageStorage(key, data)
	storage.uploadErr = err
	return storage
}

func imageStorageWithPresignErr(key string, data []byte, err error) *fakeStorage {
	storage := imageStorage(key, data)
	storage.presignErr = err
	return storage
}

func jpegBytes(t *testing.T) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 10, 10))
	for y := 0; y < 10; y++ {
		for x := 0; x < 10; x++ {
			img.Set(x, y, color.RGBA{R: 200, G: 100, B: 50, A: 255})
		}
	}
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, img, nil); err != nil {
		t.Fatalf("encode jpeg: %v", err)
	}
	return buf.Bytes()
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestDecodeHookCanBeRestored(t *testing.T) {
	original := decodeImage
	t.Cleanup(func() { decodeImage = original })

	decodeImage = func(io.Reader) (imgpkg.ImageInfo, error) {
		return imgpkg.ImageInfo{}, errTest
	}
	repo := &fakeRepo{markProcessing: true}
	w := &Worker{repo: repo, storage: imageStorage("original.jpg", jpegBytes(t))}

	err := w.HandleUploadEvent(context.Background(), domain.AvatarUploadEvent{
		AvatarID: "avatar-1",
		S3Key:    "original.jpg",
	})
	if !errors.Is(err, errTest) {
		t.Fatalf("expected hook error, got %v", err)
	}
}
