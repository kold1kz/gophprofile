# GophProfile

MVP сервиса аватарок на Go: REST API, Chi, PostgreSQL, MinIO S3, RabbitMQ worker и простой веб-интерфейс.

## Запуск

```bash
docker compose up --build
```

После запуска:

- API: `http://localhost:8080/api/v1`
- Web upload: `http://localhost:8080/web/upload`
- Health: `http://localhost:8080/health`
- PostgreSQL: `localhost:15432`
- RabbitMQ UI: `http://localhost:15672` (`guest` / `guest`)
- MinIO UI: `http://localhost:9001` (`minioadmin` / `minioadmin`)

## Основные эндпоинты

```bash
curl -F file=@avatar.jpg -H 'X-User-ID: user-1' http://localhost:8080/api/v1/avatars
curl http://localhost:8080/api/v1/users/user-1/avatars
curl http://localhost:8080/api/v1/users/user-1/avatar --output avatar.jpg
```
