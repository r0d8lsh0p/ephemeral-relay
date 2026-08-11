package main

import (
	"context"
	"fmt"
	"iter"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"fiatjaf.com/nostr"
	"fiatjaf.com/nostr/eventstore/lmdb"
	"fiatjaf.com/nostr/khatru"
	"fiatjaf.com/nostr/khatru/policies"
	"fiatjaf.com/nostr/nip40"
	"github.com/joho/godotenv"
)

// ephemeral-relay: a khatru relay that accepts only a configured set of event
// kinds and hard-deletes stored events past a fixed age. Built for live-stream
// chat: everything on this relay is transient by design, and the NIP-11
// description says so.
//
// NIP-40 goes further here than khatru's built-in expiration manager (which
// only deletes on an hourly tick): already-expired events are rejected at
// publish, expired events are hidden from queries at the exact second, and
// the sweep hard-deletes them on the configured cadence.

const purgeBatchSize = 1000

type Config struct {
	RelayName        string
	RelayPubkey      string
	RelayDescription string
	RelayIcon        string
	RelayContact     string
	Port             string
	DBPath           string
	AllowedKinds     []nostr.Kind
	RetentionSecs    int64
	PurgeIntervalSec int64
	ExemptKinds      []nostr.Kind
	RateLimitPerSec  int64
	RateLimitBurst   int64
	TrustedIPs       []string
}

var config Config

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		log.Fatalf("invalid %s: %q", key, v)
	}
	return n
}

func envKinds(key, fallback string) []nostr.Kind {
	raw := env(key, fallback)
	parts := strings.Split(raw, ",")
	kinds := make([]nostr.Kind, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		n, err := strconv.ParseUint(p, 10, 16)
		if err != nil {
			log.Fatalf("invalid kind in %s: %q", key, p)
		}
		kinds = append(kinds, nostr.Kind(n))
	}
	if len(kinds) == 0 {
		log.Fatalf("%s must contain at least one kind", key)
	}
	return kinds
}

func envList(key string) []string {
	raw := os.Getenv(key)
	if raw == "" {
		return nil
	}
	items := make([]string, 0)
	for _, p := range strings.Split(raw, ",") {
		if p = strings.TrimSpace(p); p != "" {
			items = append(items, p)
		}
	}
	return items
}

func loadConfig() Config {
	_ = godotenv.Load(".env")

	cfg := Config{
		RelayName:        env("RELAY_NAME", "ephemeral relay"),
		RelayPubkey:      env("RELAY_PUBKEY", ""),
		RelayIcon:        env("RELAY_ICON", ""),
		RelayContact:     env("RELAY_CONTACT", ""),
		Port:             env("PORT", "3335"),
		DBPath:           env("DB_PATH", "db/"),
		AllowedKinds:     envKinds("ALLOWED_KINDS", "0,5,7,16,1311,1312,1313,9735,10312"),
		RetentionSecs:    envInt("RETENTION_SECONDS", 3*60*60),
		PurgeIntervalSec: envInt("PURGE_INTERVAL_SECONDS", 10*60),
		ExemptKinds:      envKinds("RETENTION_EXEMPT_KINDS", "0"),
		RateLimitPerSec:  envInt("RATE_LIMIT_EVENTS_PER_SEC", 10),
		RateLimitBurst:   envInt("RATE_LIMIT_BURST", 50),
		TrustedIPs:       envList("TRUSTED_IPS"),
	}
	cfg.RelayDescription = env("RELAY_DESCRIPTION", fmt.Sprintf(
		"Accepts kinds %s only. All events except kinds %s are deleted after %s.",
		joinKinds(cfg.AllowedKinds), joinKinds(cfg.ExemptKinds),
		(time.Duration(cfg.RetentionSecs)*time.Second).String(),
	))
	if cfg.RetentionSecs <= 0 {
		log.Fatal("RETENTION_SECONDS must be positive")
	}
	if cfg.PurgeIntervalSec <= 0 {
		log.Fatal("PURGE_INTERVAL_SECONDS must be positive")
	}
	return cfg
}

func joinKinds(kinds []nostr.Kind) string {
	parts := make([]string, len(kinds))
	for i, k := range kinds {
		parts[i] = strconv.Itoa(int(k))
	}
	return strings.Join(parts, ",")
}

// rateLimitUntrusted applies the per-IP event rate limit, exempting trusted
// IPs — our own bridge funnels 50+ streams' chat through one egress and must
// not be throttled like a single anonymous client.
func rateLimitUntrusted(cfg Config) func(ctx context.Context, event nostr.Event) (bool, string) {
	limiter := policies.EventIPRateLimiter(int(cfg.RateLimitPerSec), time.Second, int(cfg.RateLimitBurst))
	trusted := make(map[string]bool, len(cfg.TrustedIPs))
	for _, ip := range cfg.TrustedIPs {
		trusted[ip] = true
	}
	return func(ctx context.Context, event nostr.Event) (bool, string) {
		if trusted[khatru.GetIP(ctx)] {
			return false, ""
		}
		return limiter(ctx, event)
	}
}

func isExpired(evt nostr.Event, now nostr.Timestamp) bool {
	exp := nip40.GetExpiration(evt.Tags)
	return exp > 0 && exp <= now
}

// rejectExpired refuses events that arrive already past their NIP-40
// expiration.
func rejectExpired(ctx context.Context, evt nostr.Event) (bool, string) {
	if isExpired(evt, nostr.Now()) {
		return true, "invalid: event is already expired (NIP-40)"
	}
	return false, ""
}

// filterExpired wraps the relay's stored-events query so expired events are
// never served, regardless of when the deletion sweep last ran.
func filterExpired(query func(ctx context.Context, filter nostr.Filter) iter.Seq[nostr.Event]) func(ctx context.Context, filter nostr.Filter) iter.Seq[nostr.Event] {
	return func(ctx context.Context, filter nostr.Filter) iter.Seq[nostr.Event] {
		return func(yield func(nostr.Event) bool) {
			now := nostr.Now()
			for evt := range query(ctx, filter) {
				if isExpired(evt, now) {
					continue
				}
				if !yield(evt) {
					return
				}
			}
		}
	}
}

// retainedKinds returns the kinds subject to the age purge: allowed minus exempt.
func retainedKinds(cfg Config) []nostr.Kind {
	exempt := make(map[nostr.Kind]bool, len(cfg.ExemptKinds))
	for _, k := range cfg.ExemptKinds {
		exempt[k] = true
	}
	kinds := make([]nostr.Kind, 0, len(cfg.AllowedKinds))
	for _, k := range cfg.AllowedKinds {
		if !exempt[k] {
			kinds = append(kinds, k)
		}
	}
	return kinds
}

// purgeOldEvents hard-deletes retained-kind events past the age cutoff.
// Queries the store directly (not the relay hooks) so that the expired-event
// query filter cannot hide anything from the sweep.
func purgeOldEvents(db *lmdb.LMDBBackend, cfg Config) {
	cutoff := nostr.Timestamp(time.Now().Unix() - cfg.RetentionSecs)
	kinds := retainedKinds(cfg)

	for {
		filter := nostr.Filter{
			Until: cutoff,
			Kinds: kinds,
			Limit: purgeBatchSize, // batches, to bound memory
		}
		count := 0
		for evt := range db.QueryEvents(filter, purgeBatchSize) {
			if err := db.DeleteEvent(evt.ID); err != nil {
				log.Printf("purge delete %s failed: %v", evt.ID.Hex(), err)
			}
			count++
		}
		if count > 0 {
			log.Printf("purged %d events older than %d", count, cutoff)
		}
		if count < purgeBatchSize {
			return
		}
	}
}

// purgeExpiredEvents hard-deletes events whose NIP-40 expiration has passed,
// whatever their kind or age. Walks the store in created_at-descending batches;
// the whole store is at most a retention window of chat, so this stays cheap.
func purgeExpiredEvents(db *lmdb.LMDBBackend, cfg Config) {
	now := nostr.Now()
	until := now

	for {
		filter := nostr.Filter{
			Until: until,
			Kinds: cfg.AllowedKinds,
			Limit: purgeBatchSize,
		}
		count, deleted := 0, 0
		oldest := until
		for evt := range db.QueryEvents(filter, purgeBatchSize) {
			if evt.CreatedAt < oldest {
				oldest = evt.CreatedAt
			}
			if isExpired(evt, now) {
				if err := db.DeleteEvent(evt.ID); err != nil {
					log.Printf("expiry delete %s failed: %v", evt.ID.Hex(), err)
				} else {
					deleted++
				}
			}
			count++
		}
		if deleted > 0 {
			log.Printf("purged %d expired events (NIP-40)", deleted)
		}
		if count < purgeBatchSize || oldest >= until {
			return
		}
		until = oldest - 1
	}
}

func runPurgeLoop(db *lmdb.LMDBBackend, cfg Config) {
	ticker := time.NewTicker(time.Duration(cfg.PurgeIntervalSec) * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		purgeOldEvents(db, cfg)
		purgeExpiredEvents(db, cfg)
	}
}

// warnOrphanedBadgerFiles logs when DB_PATH still holds files from the
// pre-nostrlib badger backend. lmdb ignores them, so any events inside are
// orphaned — never served, never purged, just dead bytes. Safe to delete;
// this relay's contents are ephemeral by design.
func warnOrphanedBadgerFiles(path string) {
	for _, name := range []string{"KEYREGISTRY", "DISCARD"} {
		if _, err := os.Stat(filepath.Join(path, name)); err == nil {
			log.Printf("WARNING: %s contains files from the old badger backend (pre-lmdb); their events are orphaned and the files can be deleted", path)
			return
		}
	}
}

func main() {
	config = loadConfig()

	relay := khatru.NewRelay()
	relay.Info.Name = config.RelayName
	relay.Info.Description = config.RelayDescription
	relay.Info.Icon = config.RelayIcon
	relay.Info.Contact = config.RelayContact
	relay.Info.Software = "https://github.com/r0d8lsh0p/ephemeral-relay"
	relay.Info.Version = "0.1.0"
	relay.Info.AddSupportedNIP("40")
	if config.RelayPubkey != "" {
		pk, err := nostr.PubKeyFromHex(config.RelayPubkey)
		if err != nil {
			log.Fatalf("invalid RELAY_PUBKEY: %v", err)
		}
		relay.Info.PubKey = &pk
	}

	warnOrphanedBadgerFiles(config.DBPath)
	db := &lmdb.LMDBBackend{Path: config.DBPath}
	if err := db.Init(); err != nil {
		log.Fatalf("lmdb init failed: %v", err)
	}

	relay.UseEventstore(db, purgeBatchSize)
	relay.QueryStored = filterExpired(relay.QueryStored)

	relay.OnEvent = policies.SeqEvent(
		policies.RestrictToSpecifiedKinds(false, config.AllowedKinds...),
		rejectExpired,
		policies.PreventLargeContent(65535),
		policies.PreventTimestampsInThePast(2*time.Hour),
		policies.PreventTimestampsInTheFuture(30*time.Minute),
		rateLimitUntrusted(config),
	)

	purgeOldEvents(db, config) // clear backlog before serving
	purgeExpiredEvents(db, config)
	go runPurgeLoop(db, config)

	log.Printf("ephemeral-relay on :%s — kinds %s, retention %ds (exempt %s)",
		config.Port, joinKinds(config.AllowedKinds), config.RetentionSecs, joinKinds(config.ExemptKinds))
	if err := http.ListenAndServe(":"+config.Port, relay); err != nil {
		log.Fatal(err)
	}
}
