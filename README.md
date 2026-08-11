# ephemeral-relay

A [khatru](https://github.com/fiatjaf/khatru) relay that accepts only a
configured set of event kinds and hard-deletes everything past a fixed age.

Built for live-stream chat: chat messages, reactions, reposts and zap receipts
live for a few hours and are then gone — like chat that scrolls away. Profiles
(kind 0) are exempt by default so identities outlive the retention window. The
relay's NIP-11 description states the policy plainly.

The retention loop follows the pattern proven in
[wot-relay](https://github.com/bitvora/wot-relay)'s `MAX_AGE_DAYS`: a periodic
kind-scoped query for events older than the cutoff, each hit hard-deleted from
the store.

## Configuration

Via environment or `.env` (see `.env.example`):

| Variable | Default | Meaning |
|---|---|---|
| `ALLOWED_KINDS` | `0,5,7,16,1311,1312,1313,9735` | Only these kinds are accepted |
| `RETENTION_SECONDS` | `10800` (3 h) | Events older than this are deleted |
| `PURGE_INTERVAL_SECONDS` | `600` | How often the purge runs |
| `RETENTION_EXEMPT_KINDS` | `0` | Kinds kept indefinitely |
| `RATE_LIMIT_EVENTS_PER_SEC` / `RATE_LIMIT_BURST` | `10` / `50` | Per-IP write rate limit |
| `TRUSTED_IPS` | — | IPs exempt from the rate limit (e.g. a bridge fanning many streams through one egress) |
| `PORT` | `3335` | Listen port |
| `DB_PATH` | `db/` | Badger storage path |
| `RELAY_NAME` / `RELAY_PUBKEY` / `RELAY_ICON` / `RELAY_DESCRIPTION` | — | NIP-11 identity |

Storage is [badger](https://github.com/dgraph-io/badger) via
[eventstore](https://github.com/fiatjaf/eventstore); a single Go binary, no
external services.

## Run

```bash
cp .env.example .env   # edit as needed
go build -o ephemeral-relay . && ./ephemeral-relay
```

Or docker:

```bash
docker compose up -d   # localhost:7448
```

## E2E test

Start a relay with short retention, then run the checker against it:

```bash
RETENTION_SECONDS=8 PURGE_INTERVAL_SECONDS=2 PORT=3336 DB_PATH=/tmp/eph-e2e ./ephemeral-relay &
go run ./e2e -relay ws://localhost:3336 -retention 8
```

Checks: kind 1 rejected; kind 1311 accepted and served; after the retention
window the 1311 is purged while a kind 0 profile survives.

## Why not NIP-40?

NIP-40 expiration tags put the lifetime decision in each event author's hands.
For a relay whose *contract* is transience — everything here is short-lived,
say so up front — blanket kind-scoped retention is simpler and stronger: no
tag to remember, no compliant-client dependency, one uniform guarantee.
