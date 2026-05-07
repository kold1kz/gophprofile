package queue

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRabbitMQCloseWithoutConnection(t *testing.T) {
	require.NoError(t, (&RabbitMQ{}).Close())
}

func TestRabbitMQPingReturnsContextError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := (&RabbitMQ{}).Ping(ctx)

	require.ErrorIs(t, err, context.Canceled)
}

func TestRabbitMQPublishReturnsMarshalError(t *testing.T) {
	err := (&RabbitMQ{}).publish(context.Background(), RoutingAvatarUploaded, "msg-1", func() {})

	require.Error(t, err)
}
