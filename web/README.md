# Web frontend

Статический frontend лежит в `web/static` и раздается основным Go-сервером.

Главная страница доступна по адресу:

```text
http://localhost:8080/
```

Frontend использует API текущего сервиса:

- `POST /api/v1/avatars` для загрузки аватара;
- `GET /api/v1/users/{user_id}/avatars` для галереи пользователя;
- `GET /api/v1/avatars/{avatar_id}` для отображения изображения.
