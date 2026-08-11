package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/fiatjaf/eventstore/badger"
	"github.com/fiatjaf/khatru"
	"github.com/fiatjaf/khatru/policies"
	"github.com/joho/godotenv"
	"github.com/nbd-wtf/go-nostr"
)

// ephemeral-relay: a khatru relay that accepts only a configured set of event
// kinds and hard-deletes stored events past a fixed age. Built for live-stream
// chat: everything on this relay is transient by design, and the NIP-11
// description says so.
//
// The purge follows the wot-relay pattern (QueryEvents older than the cutoff,
// DeleteEvent each hit), scoped to the retained kinds so that exempt kinds
// (profiles, by default) survive.

type Config struct {
	RelayName        string
	RelayPubkey      string
	RelayDescription string
	RelayIcon        string
	RelayContact     string
	Port             string
	DBPath           string
	AllowedKinds     []uint16
	RetentionSecs    int64
	PurgeIntervalSec int64
	ExemptKinds      []uint16
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

func envKinds(key, fallback string) []uint16 {
	raw := env(key, fallback)
	parts := strings.Split(raw, ",")
	kinds := make([]uint16, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		n, err := strconv.ParseUint(p, 10, 16)
		if err != nil {
			log.Fatalf("invalid kind in %s: %q", key, p)
		}
		kinds = append(kinds, uint16(n))
	}
	if len(kinds) == 0 {
		log.Fatalf("%s must contain at least one kind", key)
	}
	return kinds
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
		(time.Duration(cfg.RetentionSecs) * time.Second).String(),
	))
	if cfg.RetentionSecs <= 0 {
		log.Fatal("RETENTION_SECONDS must be positive")
	}
	if cfg.PurgeIntervalSec <= 0 {
		log.Fatal("PURGE_INTERVAL_SECONDS must be positive")
	}
	return cfg
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

// rateLimitUntrusted applies the per-IP event rate limit, exempting trusted
// IPs — our own bridge funnels 50+ streams' chat through one egress and must
// not be throttled like a single anonymous client.
func rateLimitUntrusted(cfg Config) func(ctx context.Context, event *nostr.Event) (bool, string) {
	limiter := policies.EventIPRateLimiter(int(cfg.RateLimitPerSec), time.Second, int(cfg.RateLimitBurst))
	trusted := make(map[string]bool, len(cfg.TrustedIPs))
	for _, ip := range cfg.TrustedIPs {
		trusted[ip] = true
	}
	return func(ctx context.Context, event *nostr.Event) (bool, string) {
		if trusted[khatru.GetIP(ctx)] {
			return false, ""
		}
		return limiter(ctx, event)
	}
}

func joinKinds(kinds []uint16) string {
	parts := make([]string, len(kinds))
	for i, k := range kinds {
		parts[i] = strconv.Itoa(int(k))
	}
	return strings.Join(parts, ",")
}

// expirationOf returns the NIP-40 expiration timestamp, if the event carries
// a valid one.
func expirationOf(evt *nostr.Event) (nostr.Timestamp, bool) {
	tag := evt.Tags.GetFirst([]string{"expiration"})
	if tag == nil || len(*tag) < 2 {
		return 0, false
	}
	ts, err := strconv.ParseInt((*tag)[1], 10, 64)
	if err != nil {
		return 0, false
	}
	return nostr.Timestamp(ts), true
}

func isExpired(evt *nostr.Event, now nostr.Timestamp) bool {
	exp, ok := expirationOf(evt)
	return ok && exp <= now
}

// rejectExpired refuses events that arrive already past their NIP-40
// expiration.
func rejectExpired(ctx context.Context, evt *nostr.Event) (bool, string) {
	if isExpired(evt, nostr.Now()) {
		return true, "invalid: event is already expired (NIP-40)"
	}
	return false, ""
}

// filterExpired wraps a store's QueryEvents so expired events are never
// served, regardless of when the deletion sweep last ran.
func filterExpired(query func(context.Context, nostr.Filter) (chan *nostr.Event, error)) func(context.Context, nostr.Filter) (chan *nostr.Event, error) {
	return func(ctx context.Context, filter nostr.Filter) (chan *nostr.Event, error) {
		ch, err := query(ctx, filter)
		if err != nil {
			return nil, err
		}
		out := make(chan *nostr.Event)
		go func() {
			defer close(out)
			now := nostr.Now()
			for evt := range ch {
				if isExpired(evt, now) {
					continue
				}
				select {
				case out <- evt:
				case <-ctx.Done():
					return
				}
			}
		}()
		return out, nil
	}
}

// retainedKinds returns the kinds subject to the age purge: allowed minus exempt.
func retainedKinds(cfg Config) []int {
	exempt := make(map[uint16]bool, len(cfg.ExemptKinds))
	for _, k := range cfg.ExemptKinds {
		exempt[k] = true
	}
	kinds := make([]int, 0, len(cfg.AllowedKinds))
	for _, k := range cfg.AllowedKinds {
		if !exempt[k] {
			kinds = append(kinds, int(k))
		}
	}
	return kinds
}

// purgeOldEvents hard-deletes retained-kind events past the age cutoff.
// Queries the store directly (not the relay hooks) so that the expired-event
// query filter cannot hide anything from the sweep.
func purgeOldEvents(db *badger.BadgerBackend, cfg Config) {
	ctx := context.TODO()
	cutoff := nostr.Timestamp(time.Now().Unix() - cfg.RetentionSecs)
	kinds := retainedKinds(cfg)

	for {
		filter := nostr.Filter{
			Until: &cutoff,
			Kinds: kinds,
			Limit: 1000, // batches, to bound memory
		}
		ch, err := db.QueryEvents(ctx, filter)
		if err != nil {
			log.Printf("purge query failed: %v", err)
			return
		}
		count := 0
		for evt := range ch {
			if err := db.DeleteEvent(ctx, evt); err != nil {
				log.Printf("purge delete %s failed: %v", evt.ID, err)
			}
			count++
		}
		if count > 0 {
			log.Printf("purged %d events older than %d", count, cutoff)
		}
		if count < 1000 {
			return
		}
	}
}

// purgeExpiredEvents hard-deletes events whose NIP-40 expiration has passed,
// whatever their kind or age. Walks the store in created_at-descending batches;
// the whole store is at most a retention window of chat, so this stays cheap.
func purgeExpiredEvents(db *badger.BadgerBackend, cfg Config) {
	ctx := context.TODO()
	now := nostr.Now()
	until := now
	allKinds := make([]int, len(cfg.AllowedKinds))
	for i, k := range cfg.AllowedKinds {
		allKinds[i] = int(k)
	}

	for {
		filter := nostr.Filter{
			Until: &until,
			Kinds: allKinds,
			Limit: 1000,
		}
		ch, err := db.QueryEvents(ctx, filter)
		if err != nil {
			log.Printf("expiry sweep query failed: %v", err)
			return
		}
		count, deleted := 0, 0
		oldest := until
		for evt := range ch {
			if evt.CreatedAt < oldest {
				oldest = evt.CreatedAt
			}
			if isExpired(evt, now) {
				if err := db.DeleteEvent(ctx, evt); err != nil {
					log.Printf("expiry delete %s failed: %v", evt.ID, err)
				} else {
					deleted++
				}
			}
			count++
		}
		if deleted > 0 {
			log.Printf("purged %d expired events (NIP-40)", deleted)
		}
		if count < 1000 || oldest >= until {
			return
		}
		until = oldest - 1
	}
}

func runPurgeLoop(db *badger.BadgerBackend, cfg Config) {
	ticker := time.NewTicker(time.Duration(cfg.PurgeIntervalSec) * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		purgeOldEvents(db, cfg)
		purgeExpiredEvents(db, cfg)
	}
}

func main() {
	config = loadConfig()

	relay := khatru.NewRelay()
	relay.Info.Name = config.RelayName
	relay.Info.PubKey = config.RelayPubkey
	relay.Info.Description = config.RelayDescription
	relay.Info.Icon = config.RelayIcon
	relay.Info.Contact = config.RelayContact
	relay.Info.Software = "https://github.com/r0d8lsh0p/ephemeral-relay"
	relay.Info.Version = "0.1.0"
	relay.Info.AddSupportedNIP(40)

	db := badger.BadgerBackend{Path: config.DBPath}
	if err := db.Init(); err != nil {
		log.Fatalf("badger init failed: %v", err)
	}

	relay.StoreEvent = append(relay.StoreEvent, db.SaveEvent)
	relay.QueryEvents = append(relay.QueryEvents, filterExpired(db.QueryEvents))
	relay.DeleteEvent = append(relay.DeleteEvent, db.DeleteEvent)

	relay.RejectEvent = append(relay.RejectEvent,
		policies.RestrictToSpecifiedKinds(false, config.AllowedKinds...),
		rejectExpired,
		policies.PreventLargeTags(200),
		policies.PreventTimestampsInThePast(2*time.Hour),
		policies.PreventTimestampsInTheFuture(30*time.Minute),
		rateLimitUntrusted(config),
	)

	purgeOldEvents(&db, config) // clear backlog before serving
	purgeExpiredEvents(&db, config)
	go runPurgeLoop(&db, config)

	log.Printf("ephemeral-relay on :%s — kinds %s, retention %ds (exempt %s)",
		config.Port, joinKinds(config.AllowedKinds), config.RetentionSecs, joinKinds(config.ExemptKinds))
	if err := http.ListenAndServe(":"+config.Port, relay); err != nil {
		log.Fatal(err)
	}
}
