package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHealthCheck(t *testing.T) {
	health := Health{
		DB:     pingComponent{},
		S3:     pingComponent{err: errors.New("s3 down")},
		Broker: nil,
	}

	result := health.Check(httptest.NewRequest(http.MethodGet, "/health", nil))

	require.Equal(t, "ok", result["db"])
	require.Equal(t, "error: s3 down", result["s3"])
	require.Equal(t, "not_configured", result["broker"])
}

type pingComponent struct {
	err error
}

func (p pingComponent) Ping(context.Context) error {
	return p.err
}
