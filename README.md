# ducklake-realtime

Realtime change delivery on top of [DuckLake](https://ducklake.select) and [ducklake-cdc](https://github.com/elei-io/ducklake-cdc-extension).

```
DuckLake --> ducklake-cdc worker --> disk log (NATS JetStream) --> clients
   capture        schema boundary            resume
```

Design and test plan: [docs/design.md](docs/design.md).

| Area | Question | Folder |
|---|---|---|
| Capture | Are DuckLake commits turned into an ordered stream, with no loss on crash? | `capture/` |
| Schema boundary | What happens at an `ALTER TABLE`, and how does a client recover? | `schema_boundary/` |
| Resume | Does a reconnecting client catch up from the log without touching DuckLake? | `resume/` |

## Setup

Requires `uv` and Docker.

```powershell
docker compose up -d      # Postgres (DuckLake catalog) + NATS JetStream
uv sync                   # Python environment
```

| Service | Address |
|---|---|
| Postgres | `localhost:5432` (user, password, database: `ducklake`) |
| NATS | `localhost:4222` (JetStream with file storage) |

## Status

- [ ] Confirm the `ducklake-cdc` extension installs and runs
- [ ] Capture
- [ ] Schema boundary
- [ ] Resume
