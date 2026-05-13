package api

import (
	"context"
	"net/http"
	"time"
)

// Component описывает внешний ресурс, который можно проверить через Ping.
type Component interface {
	Ping(ctx context.Context) error
}

// Health объединяет проверки всех инфраструктурных зависимостей приложения.
type Health struct {
	DB     Component
	S3     Component
	Broker Component
}

// Check проверяет компоненты с коротким таймаутом и возвращает понятный статус по каждому из них.
func (h Health) Check(r *http.Request) map[string]string {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	result := map[string]string{}
	check := func(name string, component Component) {
		if component == nil {
			result[name] = "not_configured"
			return
		}
		if err := component.Ping(ctx); err != nil {
			result[name] = "error: " + err.Error()
			return
		}
		result[name] = "ok"
	}
	check("db", h.DB)
	check("s3", h.S3)
	check("broker", h.Broker)
	return result
}
