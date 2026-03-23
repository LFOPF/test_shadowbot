# ShadowBot

ShadowBot — Telegram-бот для запроса и перевода глав Shadow Slave с тремя отдельными процессами: `bot-api`, `worker` и `scheduler`. Процессы координируются через Redis, тяжелый парсинг выполняется Playwright, перевод — через OpenRouter, публикация — в Telegraph.

## Что было сломано

До рефакторинга почти вся логика жила в одном `bot.py`, а `bot_api.py`, `worker.py`, `scheduler.py` были только thin wrapper'ами над монолитом. Это приводило к проблемам:

- роли физически не были разделены;
- использовались глобальные singleton-переменные для Redis, Bot, Playwright и HTTP-сессии;
- любой процесс мог инициализировать лишние зависимости;
- lifecycle браузера и сетевых клиентов было трудно предсказать и корректно завершать;
- отладка очередей, кэша, lock'ов и waiters была сложной.

## Что теперь реализовано

Новая структура:

```text
app/
  bot/app.py                # aiogram handlers и UX
  config.py                 # env + валидация ролей
  core/lifecycle.py         # bootstrap / shutdown
  core/state.py             # DI-container
  queues/jobs.py            # Redis queues + dedupe helpers
  repositories/redis_repo.py# Redis state, cache, waiters, locks
  services/chapter_flow.py  # выдача главы пользователю и постановка в очередь
  services/http.py          # shared aiohttp lifecycle
  services/monitor.py       # scheduler monitoring
  services/notifications.py # notifier loop
  services/parser.py        # Playwright lifecycle + parsing
  services/telegraph.py     # Telegraph publishing
  services/translation.py   # OpenRouter + glossary
  services/worker_pipeline.py # heavy chapter pipeline
```

Архитектура по ролям:

- `python bot_api.py` — только Telegram polling, FSM, подписки, закладки, постановка задач в очередь и выдача готовых ссылок.
- `python worker.py` — только heavy pipeline: поиск главы, Playwright, перевод, Telegraph, обновление кэша, lock'ов и статусов.
- `python scheduler.py` — только scheduler/notifier: мониторинг новых глав и доставка уведомлений.

## Основной flow

1. Пользователь запрашивает главу в Telegram.
2. `bot-api` проверяет кэш Telegraph URL и chapter status в Redis.
3. Если перевода нет, бот ставит job в Redis queue и сохраняет waiters.
4. `worker` берет job, ищет главу, берет chapter-level lock, повторно использует original/translated cache при наличии.
5. `worker` переводит текст через OpenRouter, публикует в Telegraph, сохраняет `telegraph_url`, status, error и translation signature.
6. `scheduler` отправляет адресное уведомление ожидающим пользователям или broadcast подписчикам.

## Redis-координация, которая сохранена

Сохранены и приведены в рабочее состояние:

- chapter status cache;
- original text cache;
- translated text cache;
- Telegraph URL cache;
- translation signature invalidation;
- waiters для нескольких пользователей на одну главу;
- dedupe ключи очередей;
- chapter translation lock;
- subscribers, blocked users, bookmarks, monitor state.

## Переменные окружения

### Обязательные для `bot-api`

- `BOT_TOKEN`
- `REDIS_URL`

### Обязательные для `worker`

- `OPENROUTER_API_KEY`
- `TELEGRAPH_ACCESS_TOKEN`
- `REDIS_URL`

### Обязательные для `scheduler`

- `BOT_TOKEN`
- `REDIS_URL`

### Полезные дополнительные

- `ADMIN_ID`
- `TARGET_URL`
- `NOVEL_ID`
- `CHECK_INTERVAL`
- `BROWSER_IDLE_TIMEOUT`
- `PLAYWRIGHT_CONCURRENCY`
- `GLOSSARY_CACHE_TTL`
- `TRANSLATION_CACHE_VERSION`
- `JOB_DEDUP_TTL`
- `BOT_READY_WAIT_TIMEOUT`
- `OPENROUTER_MODEL`

Скопируйте шаблон:

```bash
cp .env.example .env
```

## Локальный запуск без Docker

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium-headless-shell
```

Запустите Redis любым удобным способом, например:

```bash
docker run --rm -p 6379:6379 redis:7-alpine
```

После настройки `.env` поднимайте роли в отдельных терминалах:

```bash
python bot_api.py
python worker.py
python scheduler.py
```

## Локальный запуск через Docker Compose

```bash
cp .env.example .env
# заполните .env своими токенами

docker compose up --build
```

Compose поднимает:

- `redis`
- `bot-api`
- `worker`
- `scheduler`

## Docker / один образ для трех ролей

Один и тот же образ используется для всех ролей. Роль определяется через `SHADOWBOT_ROLE`.

Примеры:

```bash
docker build -t shadowbot .

docker run --rm --env-file .env -e SHADOWBOT_ROLE=bot-api shadowbot

docker run --rm --env-file .env -e SHADOWBOT_ROLE=worker shadowbot

docker run --rm --env-file .env -e SHADOWBOT_ROLE=scheduler shadowbot
```

## Типовые сценарии и поведение

### Повторный запрос той же главы

Если глава уже переведена, `bot-api` мгновенно вернет URL из Redis без повторного pipeline.

### Одновременные запросы одной главы

- первый запрос ставит job и инициирует перевод;
- остальные пользователи попадают в waiters;
- worker не запускает второй expensive перевод для той же главы, пока активен lock;
- после завершения notifier рассылает один и тот же результат всем ожидающим.

### Новые главы

`scheduler` периодически мониторит `TARGET_URL`, кладет новые главы в очередь и рассылает уведомления подписчикам после готовности перевода.

## Типовые ошибки

- `Для роли ... не заданы обязательные переменные` — не заполнены env для конкретной роли.
- `OpenRouter HTTP ...` — проверьте `OPENROUTER_API_KEY`, лимиты и сеть.
- `Telegraph error` — проверьте `TELEGRAPH_ACCESS_TOKEN`.
- ошибки Playwright — убедитесь, что выполнен `playwright install chromium-headless-shell` или используйте Docker image из репозитория.
- если бот не выдает главы, проверьте что одновременно работают `worker` и `scheduler`, а не только `bot-api`.

## Компромиссы рефакторинга

- транспорт очередей оставлен на Redis Lists, но вынесен в отдельный queue layer с dedupe-ключами;
- сохранена бизнес-логика исходного проекта, но без общих глобальных объектов;
- `bot.py` оставлен как compatibility entrypoint для старого сценария запуска через `SHADOWBOT_ROLE`, однако реальный рекомендуемый запуск — через `bot_api.py`, `worker.py`, `scheduler.py`.
