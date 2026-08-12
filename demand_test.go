package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"fiatjaf.com/nostr"
)

func aFilter(kinds []nostr.Kind, aTags ...string) nostr.Filter {
	f := nostr.Filter{Kinds: kinds}
	if len(aTags) > 0 {
		f.Tags = nostr.TagMap{"a": aTags}
	}
	return f
}

func TestCanonicalKey(t *testing.T) {
	pk1 := nostr.GetPublicKey(nostr.Generate())
	pk2 := nostr.GetPublicKey(nostr.Generate())

	t.Run("element order does not matter", func(t *testing.T) {
		a := nostr.Filter{Kinds: []nostr.Kind{1311, 7}, Authors: []nostr.PubKey{pk1, pk2},
			Tags: nostr.TagMap{"a": []string{"x", "y"}, "p": []string{"z"}}}
		b := nostr.Filter{Kinds: []nostr.Kind{7, 1311}, Authors: []nostr.PubKey{pk2, pk1},
			Tags: nostr.TagMap{"p": []string{"z"}, "a": []string{"y", "x"}}}
		if canonicalKey(a) != canonicalKey(b) {
			t.Error("order-permuted filters must share a key")
		}
	})

	t.Run("different filters differ", func(t *testing.T) {
		a := aFilter([]nostr.Kind{1311}, "room-1")
		b := aFilter([]nostr.Kind{1311}, "room-2")
		c := aFilter([]nostr.Kind{1311})
		if canonicalKey(a) == canonicalKey(b) || canonicalKey(a) == canonicalKey(c) {
			t.Error("distinct filters must not collide")
		}
	})

	t.Run("delimiter injection cannot forge another filter's key", func(t *testing.T) {
		// PoC cases from review: space-join and #-separator ambiguities
		a := nostr.Filter{Tags: nostr.TagMap{"t": []string{"a b"}}}
		b := nostr.Filter{Tags: nostr.TagMap{"t": []string{"a", "b"}}}
		if canonicalKey(a) == canonicalKey(b) {
			t.Error("space inside a tag value must not merge with two values")
		}
		c := nostr.Filter{Tags: nostr.TagMap{"a": []string{`x"]|#"b":["y`}}}
		d := nostr.Filter{Tags: nostr.TagMap{"a": []string{"x"}, "b": []string{"y"}}}
		if canonicalKey(c) == canonicalKey(d) {
			t.Error("delimiter characters inside a tag value must not forge a second tag")
		}
	})

	t.Run("nil and empty kinds differ", func(t *testing.T) {
		if canonicalKey(nostr.Filter{}) == canonicalKey(nostr.Filter{Kinds: []nostr.Kind{}}) {
			t.Error("nil kinds (everything) must not collide with empty kinds (nothing)")
		}
	})

	t.Run("limit and since distinguish", func(t *testing.T) {
		a := nostr.Filter{Kinds: []nostr.Kind{1311}, Limit: 10}
		b := nostr.Filter{Kinds: []nostr.Kind{1311}, Limit: 20}
		if canonicalKey(a) == canonicalKey(b) {
			t.Error("limit must distinguish filters")
		}
	})
}

func TestDemandTracker(t *testing.T) {
	clock := time.Unix(1_000_000, 0)
	now := func() time.Time { return clock }
	const room = "30311:pubkey:stream-1"

	t.Run("every subscription is an entry by default", func(t *testing.T) {
		d := newDemandTracker(nil, time.Minute, now)
		d.listenerAdded(aFilter([]nostr.Kind{1311}, room)) // scoped
		d.listenerAdded(aFilter([]nostr.Kind{1311}))       // firehose
		d.listenerAdded(nostr.Filter{Kinds: []nostr.Kind{0}})
		if snap := d.snapshot(); len(snap) != 3 {
			t.Fatalf("expected 3 entries, got %+v", snap)
		}
	})

	t.Run("identical filters from different clients aggregate", func(t *testing.T) {
		d := newDemandTracker(nil, time.Minute, now)
		d.listenerAdded(aFilter([]nostr.Kind{1311}, room))
		d.listenerAdded(aFilter([]nostr.Kind{1311}, room))
		snap := d.snapshot()
		if len(snap) != 1 || snap[0].Active != 2 {
			t.Fatalf("expected one entry with active=2, got %+v", snap)
		}
	})

	t.Run("kind scoping excludes non-matching subscriptions", func(t *testing.T) {
		d := newDemandTracker([]nostr.Kind{1311}, time.Minute, now)
		d.listenerAdded(nostr.Filter{Kinds: []nostr.Kind{0}})
		d.listenerAdded(aFilter([]nostr.Kind{1311}, room))
		snap := d.snapshot()
		if len(snap) != 1 || len(snap[0].Filter.Kinds) != 1 || snap[0].Filter.Kinds[0] != 1311 {
			t.Fatalf("only the 1311 subscription should be tracked, got %+v", snap)
		}
	})

	t.Run("kindless filter always tracked (asks for everything)", func(t *testing.T) {
		d := newDemandTracker([]nostr.Kind{1311}, time.Minute, now)
		d.listenerAdded(nostr.Filter{Tags: nostr.TagMap{"a": []string{room}}})
		if snap := d.snapshot(); len(snap) != 1 {
			t.Fatalf("kindless filter should be tracked, got %+v", snap)
		}
	})

	t.Run("refcount down, entry survives within stale window, then pruned", func(t *testing.T) {
		d := newDemandTracker(nil, time.Minute, now)
		f := aFilter([]nostr.Kind{1311}, room)
		d.listenerAdded(f)
		d.listenerRemoved(f)
		snap := d.snapshot()
		if len(snap) != 1 || snap[0].Active != 0 {
			t.Fatalf("zero-active entry should remain within stale window, got %+v", snap)
		}
		clock = clock.Add(2 * time.Minute)
		if snap := d.snapshot(); len(snap) != 0 {
			t.Fatalf("stale entry should be pruned, got %+v", snap)
		}
	})

	t.Run("empty non-nil kinds never tracked (matches nothing)", func(t *testing.T) {
		d := newDemandTracker(nil, time.Minute, now)
		d.listenerAdded(nostr.Filter{Kinds: []nostr.Kind{}, Tags: nostr.TagMap{"a": []string{room}}})
		if snap := d.snapshot(); len(snap) != 0 {
			t.Fatalf("kinds:[] can never receive events and must not be tracked, got %+v", snap)
		}
	})

	t.Run("over-cap eviction drops oldest zero-active, keeps active", func(t *testing.T) {
		d := newDemandTracker(nil, time.Hour, now) // stale window irrelevant: eviction must work regardless
		live := aFilter([]nostr.Kind{1311}, "keep-me")
		d.listenerAdded(live)
		for i := 0; i < maxDemandEntries+50; i++ {
			f := aFilter([]nostr.Kind{1311}, fmt.Sprintf("churn-%d", i))
			clock = clock.Add(time.Millisecond)
			d.listenerAdded(f)
			d.listenerRemoved(f)
		}
		d.mu.Lock()
		size := len(d.entries)
		_, liveKept := d.entries[canonicalKey(live)]
		d.mu.Unlock()
		if size > maxDemandEntries {
			t.Fatalf("entries = %d, cap %d not enforced", size, maxDemandEntries)
		}
		if !liveKept {
			t.Fatal("active entry must never be evicted")
		}
	})

	t.Run("removal never goes negative", func(t *testing.T) {
		d := newDemandTracker(nil, time.Minute, now)
		f := aFilter([]nostr.Kind{1311}, room)
		d.listenerRemoved(f) // remove with no prior add
		d.listenerAdded(f)
		if snap := d.snapshot(); snap[0].Active != 1 {
			t.Fatalf("active = %d, want 1", snap[0].Active)
		}
	})
}

func TestDemandHandler(t *testing.T) {
	clock := time.Unix(1_000_000, 0)
	d := newDemandTracker(nil, time.Minute, func() time.Time { return clock })
	d.listenerAdded(aFilter([]nostr.Kind{1311}, "30311:pk:room"))

	t.Run("open access when no token configured", func(t *testing.T) {
		rec := httptest.NewRecorder()
		withAuthToken("", d.handler())(rec, httptest.NewRequest(http.MethodGet, "/demand", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d", rec.Code)
		}
		var body struct {
			Demand []demandItem `json:"demand"`
		}
		if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if len(body.Demand) != 1 || body.Demand[0].Active != 1 {
			t.Fatalf("body = %+v", body)
		}
		if got := body.Demand[0].Filter.Tags["a"]; len(got) != 1 || got[0] != "30311:pk:room" {
			t.Fatalf("filter round-trip lost the #a tag: %+v", body.Demand[0].Filter)
		}
	})

	t.Run("token required when configured", func(t *testing.T) {
		rec := httptest.NewRecorder()
		withAuthToken("s3cret", d.handler())(rec, httptest.NewRequest(http.MethodGet, "/demand", nil))
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("status without token = %d, want 401", rec.Code)
		}
		rec = httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/demand", nil)
		req.Header.Set("Authorization", "Bearer s3cret")
		withAuthToken("s3cret", d.handler())(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status with token = %d, want 200", rec.Code)
		}
	})
}
