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
	Port             string
	DBPath           string
	AllowedKinds     []uint16
	RetentionSecs    int64
	PurgeIntervalSec int64
	ExemptKinds      []uint16
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
		Port:             env("PORT", "3335"),
		DBPath:           env("DB_PATH", "db/"),
		AllowedKinds:     envKinds("ALLOWED_KINDS", "0,5,7,16,1311,1312,1313,9735"),
		RetentionSecs:    envInt("RETENTION_SECONDS", 3*60*60),
		PurgeIntervalSec: envInt("PURGE_INTERVAL_SECONDS", 10*60),
		ExemptKinds:      envKinds("RETENTION_EXEMPT_KINDS", "0"),
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

func joinKinds(kinds []uint16) string {
	parts := make([]string, len(kinds))
	for i, k := range kinds {
		parts[i] = strconv.Itoa(int(k))
	}
	return strings.Join(parts, ",")
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

func purgeOldEvents(relay *khatru.Relay, cfg Config) {
	ctx := context.TODO()
	cutoff := nostr.Timestamp(time.Now().Unix() - cfg.RetentionSecs)
	kinds := retainedKinds(cfg)

	for {
		filter := nostr.Filter{
			Until: &cutoff,
			Kinds: kinds,
			Limit: 1000, // batches, to bound memory
		}
		ch, err := relay.QueryEvents[0](ctx, filter)
		if err != nil {
			log.Printf("purge query failed: %v", err)
			return
		}
		count := 0
		for evt := range ch {
			for _, del := range relay.DeleteEvent {
				if err := del(ctx, evt); err != nil {
					log.Printf("purge delete %s failed: %v", evt.ID, err)
				}
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

func runPurgeLoop(relay *khatru.Relay, cfg Config) {
	ticker := time.NewTicker(time.Duration(cfg.PurgeIntervalSec) * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		purgeOldEvents(relay, cfg)
	}
}

func main() {
	config = loadConfig()

	relay := khatru.NewRelay()
	relay.Info.Name = config.RelayName
	relay.Info.PubKey = config.RelayPubkey
	relay.Info.Description = config.RelayDescription
	relay.Info.Icon = config.RelayIcon
	relay.Info.Software = "https://github.com/r0d8lsh0p/ephemeral-relay"
	relay.Info.Version = "0.1.0"

	db := badger.BadgerBackend{Path: config.DBPath}
	if err := db.Init(); err != nil {
		log.Fatalf("badger init failed: %v", err)
	}

	relay.StoreEvent = append(relay.StoreEvent, db.SaveEvent)
	relay.QueryEvents = append(relay.QueryEvents, db.QueryEvents)
	relay.DeleteEvent = append(relay.DeleteEvent, db.DeleteEvent)

	relay.RejectEvent = append(relay.RejectEvent,
		policies.RestrictToSpecifiedKinds(false, config.AllowedKinds...),
		policies.PreventLargeTags(200),
		policies.PreventTimestampsInThePast(2*time.Hour),
		policies.PreventTimestampsInTheFuture(30*time.Minute),
		policies.EventIPRateLimiter(10, time.Second, 50),
	)

	purgeOldEvents(relay, config) // clear backlog before serving
	go runPurgeLoop(relay, config)

	log.Printf("ephemeral-relay on :%s — kinds %s, retention %ds (exempt %s)",
		config.Port, joinKinds(config.AllowedKinds), config.RetentionSecs, joinKinds(config.ExemptKinds))
	if err := http.ListenAndServe(":"+config.Port, relay); err != nil {
		log.Fatal(err)
	}
}
