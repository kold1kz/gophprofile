package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewMinIORejectsInvalidEndpoint(t *testing.T) {
	got, err := NewMinIO(context.Background(), "", "", "access", "secret", "avatars", false)

	require.Nil(t, got)
	require.Error(t, err)
}

func TestMinIOEndpointFromURL(t *testing.T) {
	endpoint, secure, err := minioEndpointFromURL("https://storage.example.test:9000")
	require.NoError(t, err)
	require.Equal(t, "storage.example.test:9000", endpoint)
	require.True(t, secure)

	endpoint, secure, err = minioEndpointFromURL("http://localhost:9000")
	require.NoError(t, err)
	require.Equal(t, "localhost:9000", endpoint)
	require.False(t, secure)

	_, _, err = minioEndpointFromURL("localhost:9000")
	require.Error(t, err)

	_, _, err = minioEndpointFromURL("http://")
	require.Error(t, err)
}
