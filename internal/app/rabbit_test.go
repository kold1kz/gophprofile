package app

import (
	"context"
	"errors"
	"testing"

	"gophprofile/internal/queue"

	"github.com/stretchr/testify/require"
)

func TestConnectRabbitSuccess(t *testing.T) {
	original := newRabbitMQ
	t.Cleanup(func() { newRabbitMQ = original })

	want := &queue.RabbitMQ{}
	newRabbitMQ = func(url, exchange, queueName string) (*queue.RabbitMQ, error) {
		require.Equal(t, "amqp://localhost", url)
		require.Equal(t, "events", exchange)
		require.Equal(t, "avatars", queueName)
		return want, nil
	}

	got, err := ConnectRabbit(context.Background(), "amqp://localhost", "events", "avatars")

	require.NoError(t, err)
	require.Same(t, want, got)
}

func TestConnectRabbitReturnsContextError(t *testing.T) {
	original := newRabbitMQ
	t.Cleanup(func() { newRabbitMQ = original })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	newRabbitMQ = func(string, string, string) (*queue.RabbitMQ, error) {
		return nil, errors.New("rabbit down")
	}

	got, err := ConnectRabbit(ctx, "amqp://localhost", "events", "avatars")

	require.Nil(t, got)
	require.ErrorIs(t, err, context.Canceled)
}
