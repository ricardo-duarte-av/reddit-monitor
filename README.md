# reddit-monitor

A small Go bot that watches a subreddit, asks OpenAI whether each new post is a
ROM request (or whatever your prompt describes), and posts an alert to a Matrix
room. Moderators act on the alert by reacting to it: 🔒 locks the Reddit thread,
💣 removes it.

## How it works

```
Reddit /new  ──poll──▶  OpenAI (o4-mini)  ──true?──▶  Matrix alert + 🔒 💣 buttons
                                                              │
                                       moderator reacts ──────┤
                                                              ▼
                                                  Reddit lock / remove
```

* **Polling** — every `bot.poll_interval` (default 2m) the newest 10 posts are
  fetched and handed to 3 worker goroutines. Post IDs are recorded in SQLite so
  each post is analyzed exactly once.
* **Classification** — the post title and body are substituted into your
  configured prompt (`%s`) and sent to OpenAI; the model must answer `true` or
  `false`.
* **Alerting** — a positive answer produces a formatted message in the Matrix
  room, preceded by a separator line, with 🔒 and 💣 pre-added as reaction
  "buttons". The mapping from Matrix event ID to Reddit post is stored in
  SQLite.
* **Actions** — a Matrix sync loop listens for `m.reaction` events on the bot's
  own alerts. Each action is claimed atomically in SQLite, so several people
  reacting with the same emoji only trigger one Reddit call. On success the bot
  adds a confirmation reaction to the alert; on failure it releases the claim so
  a later reaction can retry.
* **Restart safety** — the Matrix sync token and filter ID are persisted in
  SQLite, so reactions sent while the bot was down are still picked up on the
  next start.

## Requirements

* Go 1.23+ (cgo enabled — the SQLite driver is `mattn/go-sqlite3`)
* A Reddit script app, with the account being a **moderator** of the target
  subreddit (needed for lock/remove; the bot logs a warning at startup if not)
* An OpenAI API key
* A Matrix account with an access token, and a room to post into

## Setup

```sh
git clone https://github.com/ricardo-duarte-av/reddit-monitor.git
cd reddit-monitor
cp config.yaml.example config.yaml
$EDITOR config.yaml
go build -o reddit-monitor .
./reddit-monitor
```

`config.yaml` is read from the working directory, and `.gitignore`d — it holds
your credentials.

## Configuration

| Key | Description |
| --- | --- |
| `reddit.client_id` / `client_secret` | From your Reddit script app |
| `reddit.username` / `password` | Moderator account credentials |
| `openai.api_key` | OpenAI API key |
| `matrix.server` | Homeserver URL, e.g. `https://matrix.org` |
| `matrix.user` | Localpart; the full MXID is resolved via `whoami` |
| `matrix.token` | Access token |
| `matrix.room_id` | Room the alerts go to; the bot joins it at startup |
| `bot.subreddit` | Subreddit name, without `r/` |
| `bot.poll_interval` | Go duration, e.g. `90s`, `5m`. Defaults to `2m` |
| `bot.prompt` | Prompt template; `%s` is replaced with the post title and body |
| `sqlite.db_path` | Path to the state database, created on first run |

## Running as a service

`reddit-monitor.service` is a systemd unit for running the bot under your own
user. Adjust `User`, `WorkingDirectory` and `ExecStart` to your paths, then:

```sh
sudo cp reddit-monitor.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now reddit-monitor
```

## Files

| File | Purpose |
| --- | --- |
| `reddit-monitor.go` | Config, SQLite schema and helpers, OpenAI analysis, polling loop, `main` |
| `matrix.go` | Matrix client: sync store, sending alerts/notices/reactions, reaction handling |
| `reddit_actions.go` | Reddit moderation calls: lock, remove, mod permission check |
