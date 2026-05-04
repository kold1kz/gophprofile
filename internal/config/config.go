package config

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config хранит все настройки, которые нужны серверу и воркеру для старта.
// Значения приходят из .env, переменных окружения или из дефолтов ниже.
type Config struct {
	HTTPAddr       string
	PublicBaseURL  string
	DatabaseURL    string
	PostgresDSN    string
	S3Endpoint     string
	S3AccessKey    string
	S3SecretKey    string
	S3Bucket       string
	S3UseSSL       bool
	RabbitURL      string
	RabbitExchange string
	RabbitQueue    string
	MaxFileSize    int64
	ShutdownDelay  time.Duration
}

// Load собирает конфигурацию приложения из .env и окружения.
// DATABASE_URL имеет приоритет, а DATABASE_DSN используется как запасное имя для строки подключения.
func Load() Config {
	loadDotEnv(".env")

	databaseURL := getenv("DATABASE_URL", "")
	if databaseURL == "" {
		databaseURL = getenv("DATABASE_DSN", "postgres://root1:root@localhost:5432/gophprofile")
	}

	return Config{
		HTTPAddr:       getenv("HTTP_ADDR", ":8080"),
		PublicBaseURL:  getenv("PUBLIC_BASE_URL", "http://localhost:8080"),
		DatabaseURL:    databaseURL,
		PostgresDSN:    databaseURL,
		S3Endpoint:     getenv("S3_ENDPOINT", "localhost:9000"),
		S3AccessKey:    getenv("S3_ACCESS_KEY", "admin"),
		S3SecretKey:    getenv("S3_SECRET_KEY", "admin"),
		S3Bucket:       getenv("S3_BUCKET", "avatars"),
		S3UseSSL:       getenvBool("S3_USE_SSL", false),
		RabbitURL:      getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
		RabbitExchange: getenv("RABBITMQ_EXCHANGE", "avatars.exchange"),
		RabbitQueue:    getenv("RABBITMQ_QUEUE", "avatars.worker"),
		MaxFileSize:    getenvInt64("MAX_FILE_SIZE", 10<<20),
		ShutdownDelay:  10 * time.Second,
	}
}

func loadDotEnv(path string) {
	file, err := os.Open(path)
	if err != nil {
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		key, value, ok := parseDotEnvLine(scanner.Text())
		if !ok {
			continue
		}
		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		_ = os.Setenv(key, value)
	}
}

func parseDotEnvLine(line string) (string, string, bool) {
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "#") {
		return "", "", false
	}
	line = strings.TrimPrefix(line, "export ")

	key, value, found := strings.Cut(line, "=")
	if !found {
		return "", "", false
	}

	key = strings.TrimSpace(key)
	if key == "" {
		return "", "", false
	}

	value = strings.TrimSpace(value)
	if len(value) >= 2 {
		quote := value[0]
		if (quote == '"' || quote == '\'') && value[len(value)-1] == quote {
			return key, value[1 : len(value)-1], true
		}
	}

	if comment := strings.Index(value, " #"); comment >= 0 {
		value = strings.TrimSpace(value[:comment])
	}

	return key, value, true
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getenvBool(key string, fallback bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func getenvInt64(key string, fallback int64) int64 {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}
