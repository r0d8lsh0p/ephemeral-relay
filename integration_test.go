package main

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"fiatjaf.com/nostr"
	"fiatjaf.com/nostr/eventstore/slicestore"
	"fiatjaf.com/nostr/khatru"
	"fiatjaf.com/nostr/khatru/policies"
)

// In-process integration tests: a real khatru relay over httptest with the
// in-memory slicestore — no docker, no lmdb, no waiting on wall-clock
// retention. The e2e/ checker still covers the full binary + lmdb + docker.

func testConfig() Config {
	return Config{
		AllowedKinds:  []nostr.Kind{0, 7, 1311, 9735},
		ExemptKinds:   []nostr.Kind{0},
		RetentionSecs: 3600,
	}
}

func signedEvent(t *testing.T, sk nostr.SecretKey, kind nostr.Kind, content string, createdAt nostr.Timestamp, tags nostr.Tags) nostr.Event {
	t.Helper()
	evt := nostr.Event{
		PubKey:    nostr.GetPublicKey(sk),
		CreatedAt: createdAt,
		Kind:      kind,
		Tags:      tags,
		Content:   content,
	}
	if err := evt.Sign(sk); err != nil {
		t.Fatalf("sign: %v", err)
	}
	return evt
}

func TestPurgeOldEvents(t *testing.T) {
	store := &slicestore.SliceStore{}
	if err := store.Init(); err != nil {
		t.Fatal(err)
	}
	cfg := testConfig()
	sk := nostr.Generate()
	now := nostr.Now()

	fresh := signedEvent(t, sk, 1311, "fresh chat", now, nostr.Tags{{"a", "30311:x:s"}})
	stale := signedEvent(t, sk, 1311, "stale chat", now-7200, nostr.Tags{{"a", "30311:x:s"}})
	staleProfile := signedEvent(t, sk, 0, `{"name":"old"}`, now-7200, nil)
	for _, evt := range []nostr.Event{fresh, stale, staleProfile} {
		if err := store.SaveEvent(evt); err != nil {
			t.Fatal(err)
		}
	}

	purgeOldEvents(store, cfg)

	remaining := map[nostr.ID]bool{}
	for evt := range store.QueryEvents(nostr.Filter{}, 100) {
		remaining[evt.ID] = true
	}
	if !remaining[fresh.ID] {
		t.Error("fresh event must survive the age purge")
	}
	if remaining[stale.ID] {
		t.Error("stale retained-kind event must be deleted")
	}
	if !remaining[staleProfile.ID] {
		t.Error("stale exempt-kind (0) event must survive")
	}
}

func TestPurgeExpiredEvents(t *testing.T) {
	store := &slicestore.SliceStore{}
	if err := store.Init(); err != nil {
		t.Fatal(err)
	}
	cfg := testConfig()
	sk := nostr.Generate()
	now := nostr.Now()

	expired := signedEvent(t, sk, 1311, "expired", now-100,
		nostr.Tags{{"a", "30311:x:s"}, {"expiration", fmt.Sprintf("%d", now-10)}})
	unexpired := signedEvent(t, sk, 1311, "not yet", now-100,
		nostr.Tags{{"a", "30311:x:s"}, {"expiration", fmt.Sprintf("%d", now+3600)}})
	tagless := signedEvent(t, sk, 1311, "no tag", now-100, nostr.Tags{{"a", "30311:x:s"}})
	for _, evt := range []nostr.Event{expired, unexpired, tagless} {
		if err := store.SaveEvent(evt); err != nil {
			t.Fatal(err)
		}
	}

	purgeExpiredEvents(store, cfg)

	remaining := map[nostr.ID]bool{}
	for evt := range store.QueryEvents(nostr.Filter{}, 100) {
		remaining[evt.ID] = true
	}
	if remaining[expired.ID] {
		t.Error("expired event must be hard-deleted")
	}
	if !remaining[unexpired.ID] {
		t.Error("future-expiry event must survive")
	}
	if !remaining[tagless.ID] {
		t.Error("tagless event must survive the expiry sweep")
	}
}

func TestFilterExpiredHidesAtQueryTime(t *testing.T) {
	store := &slicestore.SliceStore{}
	if err := store.Init(); err != nil {
		t.Fatal(err)
	}
	sk := nostr.Generate()
	now := nostr.Now()

	expired := signedEvent(t, sk, 1311, "expired but unswept", now-100,
		nostr.Tags{{"a", "30311:x:s"}, {"expiration", fmt.Sprintf("%d", now-1)}})
	alive := signedEvent(t, sk, 1311, "alive", now-100, nostr.Tags{{"a", "30311:x:s"}})
	for _, evt := range []nostr.Event{expired, alive} {
		if err := store.SaveEvent(evt); err != nil {
			t.Fatal(err)
		}
	}

	query := filterExpired(func(ctx context.Context, filter nostr.Filter) iter.Seq[nostr.Event] {
		return store.QueryEvents(filter, 100)
	})
	served := map[nostr.ID]bool{}
	for evt := range query(context.Background(), nostr.Filter{}) {
		served[evt.ID] = true
	}
	if served[expired.ID] {
		t.Error("expired event must be hidden even before any sweep deletes it")
	}
	if !served[alive.ID] {
		t.Error("unexpired event must be served")
	}
}

// TestRelayOverWebsocket runs the full policy chain against a real websocket
// relay in-process, mirroring main()'s wiring on a slicestore.
func TestRelayOverWebsocket(t *testing.T) {
	cfg := testConfig()
	store := &slicestore.SliceStore{}
	if err := store.Init(); err != nil {
		t.Fatal(err)
	}

	relay := khatru.NewRelay()
	relay.UseEventstore(store, maxQueryLimit)
	relay.QueryStored = filterExpired(relay.QueryStored)
	relay.OnEvent = policies.SeqEvent(
		policies.RestrictToSpecifiedKinds(false, cfg.AllowedKinds...),
		rejectExpired,
		preventLargeTags(200),
	)

	srv := httptest.NewServer(relay)
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client, err := nostr.RelayConnect(ctx, url, nostr.RelayOptions{})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	sk := nostr.Generate()

	t.Run("disallowed kind rejected", func(t *testing.T) {
		evt := signedEvent(t, sk, 1, "a note", nostr.Now(), nil)
		if err := client.Publish(ctx, evt); err == nil || !strings.Contains(err.Error(), "kind") {
			t.Errorf("expected kind rejection, got %v", err)
		}
	})

	t.Run("pre-expired rejected", func(t *testing.T) {
		evt := signedEvent(t, sk, 1311, "born dead", nostr.Now(),
			nostr.Tags{{"a", "30311:x:s"}, {"expiration", fmt.Sprintf("%d", nostr.Now()-60)}})
		if err := client.Publish(ctx, evt); err == nil || !strings.Contains(err.Error(), "expired") {
			t.Errorf("expected expiry rejection, got %v", err)
		}
	})

	t.Run("oversized indexable tag rejected", func(t *testing.T) {
		evt := signedEvent(t, sk, 1311, "big tag", nostr.Now(),
			nostr.Tags{{"a", strings.Repeat("x", 201)}})
		if err := client.Publish(ctx, evt); err == nil || !strings.Contains(err.Error(), "tags") {
			t.Errorf("expected tag-size rejection, got %v", err)
		}
	})

	t.Run("allowed chat accepted and served", func(t *testing.T) {
		evt := signedEvent(t, sk, 1311, "hello", nostr.Now(), nostr.Tags{{"a", "30311:x:s"}})
		if err := client.Publish(ctx, evt); err != nil {
			t.Fatalf("publish: %v", err)
		}
		found := false
		for got := range client.QueryEvents(nostr.Filter{Kinds: []nostr.Kind{1311}}) {
			if got.ID == evt.ID {
				found = true
			}
		}
		if !found {
			t.Error("published event not served")
		}
	})

	t.Run("replaceable kind 0 deduplicates", func(t *testing.T) {
		first := signedEvent(t, sk, 0, `{"name":"v1"}`, nostr.Now()-2, nil)
		second := signedEvent(t, sk, 0, `{"name":"v2"}`, nostr.Now(), nil)
		if err := client.Publish(ctx, first); err != nil {
			t.Fatalf("publish v1: %v", err)
		}
		if err := client.Publish(ctx, second); err != nil {
			t.Fatalf("publish v2: %v", err)
		}
		var served []nostr.Event
		for got := range client.QueryEvents(nostr.Filter{Kinds: []nostr.Kind{0}, Authors: []nostr.PubKey{nostr.GetPublicKey(sk)}}) {
			served = append(served, got)
		}
		if len(served) != 1 || served[0].Content != `{"name":"v2"}` {
			t.Errorf("expected only latest kind 0, got %d event(s)", len(served))
		}
	})
}

// TestDemandOverWebsocket exercises the demand tracker through real
// subscriptions: mirror main()'s wiring (listener hooks + mux route), then
// verify entries appear per distinct filter, aggregate across clients, and
// decrement on unsubscribe. Consumer-side selection (finding entries whose
// filter names a given #a) is done exactly as a bridge would.
func TestDemandOverWebsocket(t *testing.T) {
	store := &slicestore.SliceStore{}
	if err := store.Init(); err != nil {
		t.Fatal(err)
	}
	relay := khatru.NewRelay()
	relay.UseEventstore(store, maxQueryLimit)

	tracker := newDemandTracker(nil, time.Minute, time.Now)
	relay.OnListenerAdded = func(ws *khatru.WebSocket, ssid int, id string, filter nostr.Filter) {
		tracker.listenerAdded(filter)
	}
	relay.OnListenerRemoved = func(ws *khatru.WebSocket, ssid int, id string, filter nostr.Filter) {
		tracker.listenerRemoved(filter)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /demand", tracker.handler())
	mux.Handle("/", relay)

	srv := httptest.NewServer(mux)
	defer srv.Close()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	fetchDemand := func() []demandItem {
		t.Helper()
		resp, err := http.Get(srv.URL + "/demand")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var body struct {
			Demand []demandItem `json:"demand"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		return body.Demand
	}

	const room = "30311:deadbeef:demand-stream"

	// consumer-side selection, as a bridge would do it
	activeForRoom := func() int {
		for _, it := range fetchDemand() {
			if slices.Contains(it.Filter.Tags["a"], room) {
				return it.Active
			}
		}
		return -1
	}

	waitFor := func(cond func() bool, what string) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if cond() {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for %s", what)
	}

	viewer, err := nostr.RelayConnect(ctx, wsURL, nostr.RelayOptions{})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer viewer.Close()

	// a firehose subscription is its own entry, with no #a to select on
	firehose, err := viewer.Subscribe(ctx, nostr.Filter{Kinds: []nostr.Kind{1311}}, nostr.SubscriptionOptions{})
	if err != nil {
		t.Fatalf("firehose subscribe: %v", err)
	}
	defer firehose.Unsub()
	waitFor(func() bool { return len(fetchDemand()) == 1 }, "firehose entry to appear")
	if activeForRoom() != -1 {
		t.Fatal("firehose entry must not match #a selection")
	}

	// a scoped subscription is a second, selectable entry
	sub, err := viewer.Subscribe(ctx, nostr.Filter{
		Kinds: []nostr.Kind{1311},
		Tags:  nostr.TagMap{"a": []string{room}},
	}, nostr.SubscriptionOptions{})
	if err != nil {
		t.Fatalf("scoped subscribe: %v", err)
	}
	waitFor(func() bool { return activeForRoom() == 1 }, "scoped entry with active=1")

	// an identical filter from a second client aggregates, not duplicates
	viewer2, err := nostr.RelayConnect(ctx, wsURL, nostr.RelayOptions{})
	if err != nil {
		t.Fatalf("connect 2: %v", err)
	}
	defer viewer2.Close()
	sub2, err := viewer2.Subscribe(ctx, nostr.Filter{
		Kinds: []nostr.Kind{1311},
		Tags:  nostr.TagMap{"a": []string{room}},
	}, nostr.SubscriptionOptions{})
	if err != nil {
		t.Fatalf("scoped subscribe 2: %v", err)
	}
	waitFor(func() bool { return activeForRoom() == 2 && len(fetchDemand()) == 2 }, "aggregation to active=2 in one entry")

	// unsubscribing decrements; the entry stays visible with active=0
	sub.Unsub()
	sub2.Unsub()
	waitFor(func() bool { return activeForRoom() == 0 }, "unsubscribes to drop active to 0")
}
