package main

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"fiatjaf.com/nostr"
)

// Demand tracking: expose the subscriptions currently open on this relay, so
// an operator's own services — a bot, a bridge mirroring another platform's
// chat — can do expensive work only while someone is actually listening. The
// relay stays a passive observer: it never rejects or alters subscriptions,
// it just counts them.
//
// Reporting is deliberately unopinionated: every open subscription's filter
// is an entry, identical filters aggregate, and whoever polls the endpoint
// selects the entries it cares about (a chat bridge greps entries whose
// filter names its `#a` tags). DEMAND_KINDS optionally scopes which
// subscriptions are worth tracking at all.

// maxDemandEntries bounds tracker memory under filter-diverse floods; when
// exceeded, stale zero-subscriber entries are pruned eagerly.
const maxDemandEntries = 10000

type demandEntry struct {
	filter   nostr.Filter
	active   int
	lastSeen time.Time
}

type demandTracker struct {
	mu      sync.Mutex
	entries map[string]*demandEntry
	kinds   []nostr.Kind // empty = every subscription is tracked
	stale   time.Duration
	now     func() time.Time
}

func newDemandTracker(kinds []nostr.Kind, stale time.Duration, now func() time.Time) *demandTracker {
	return &demandTracker{
		entries: make(map[string]*demandEntry),
		kinds:   kinds,
		stale:   stale,
		now:     now,
	}
}

// tracked reports whether a filter qualifies for demand tracking. A nil
// Kinds asks for all kinds, so it always qualifies; a non-nil empty Kinds
// matches nothing (khatru never dispatches to it) and is never tracked.
func (d *demandTracker) tracked(filter nostr.Filter) bool {
	if filter.Kinds != nil && len(filter.Kinds) == 0 {
		return false
	}
	if len(d.kinds) == 0 || filter.Kinds == nil {
		return true
	}
	for _, k := range filter.Kinds {
		if slices.Contains(d.kinds, k) {
			return true
		}
	}
	return false
}

// canonicalKey builds a deterministic identity for a filter so that identical
// subscriptions from different clients aggregate into one entry, regardless
// of element order. (The library's JSON marshaling iterates the tag map in
// Go's random order, so it cannot serve as the key.)
//
// Every attacker-supplied string (tag keys, tag values, search) is
// strconv.Quote'd so that delimiters inside values cannot forge another
// filter's key. Kinds/authors/ids are numeric or fixed-alphabet hex and safe
// as-is. LimitZero is included; explicit limit:0 and omitted limit differ.
func canonicalKey(f nostr.Filter) string {
	var b strings.Builder

	kinds := slices.Clone(f.Kinds)
	slices.Sort(kinds)
	fmt.Fprintf(&b, "kinds(nil:%t):%v", f.Kinds == nil, kinds)

	authors := make([]string, len(f.Authors))
	for i, pk := range f.Authors {
		authors[i] = pk.Hex()
	}
	slices.Sort(authors)
	fmt.Fprintf(&b, "|authors:%v", authors)

	ids := make([]string, len(f.IDs))
	for i, id := range f.IDs {
		ids[i] = id.Hex()
	}
	slices.Sort(ids)
	fmt.Fprintf(&b, "|ids:%v", ids)

	tagKeys := make([]string, 0, len(f.Tags))
	for k := range f.Tags {
		tagKeys = append(tagKeys, k)
	}
	slices.Sort(tagKeys)
	for _, k := range tagKeys {
		vals := slices.Clone(f.Tags[k])
		slices.Sort(vals)
		fmt.Fprintf(&b, "|#%s:[", strconv.Quote(k))
		for i, v := range vals {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(strconv.Quote(v))
		}
		b.WriteByte(']')
	}

	fmt.Fprintf(&b, "|since:%d|until:%d|limit:%d|limitzero:%t|search:%s",
		f.Since, f.Until, f.Limit, f.LimitZero, strconv.Quote(f.Search))
	return b.String()
}

func (d *demandTracker) listenerAdded(filter nostr.Filter) {
	if !d.tracked(filter) {
		return
	}
	key := canonicalKey(filter)
	d.mu.Lock()
	defer d.mu.Unlock()
	now := d.now()
	if len(d.entries) >= maxDemandEntries {
		d.evictLocked(now)
	}
	e := d.entries[key]
	if e == nil {
		e = &demandEntry{filter: filter}
		d.entries[key] = e
	}
	e.active++
	e.lastSeen = now
}

func (d *demandTracker) listenerRemoved(filter nostr.Filter) {
	if !d.tracked(filter) {
		return
	}
	key := canonicalKey(filter)
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.entries[key]; e != nil && e.active > 0 {
		e.active--
		e.lastSeen = d.now()
	}
}

// evictLocked enforces maxDemandEntries: stale zero-subscriber entries go
// first, then the oldest zero-subscriber entries regardless of staleness.
// Active entries are never evicted — each corresponds to a subscription
// khatru itself holds in memory, so they are bounded by the relay's own
// connection limits, not by this cap.
func (d *demandTracker) evictLocked(now time.Time) {
	for k, e := range d.entries {
		if e.active == 0 && now.Sub(e.lastSeen) > d.stale {
			delete(d.entries, k)
		}
	}
	for len(d.entries) >= maxDemandEntries {
		oldestKey := ""
		var oldest time.Time
		for k, e := range d.entries {
			if e.active == 0 && (oldestKey == "" || e.lastSeen.Before(oldest)) {
				oldestKey, oldest = k, e.lastSeen
			}
		}
		if oldestKey == "" {
			return // every entry is active; nothing evictable
		}
		delete(d.entries, oldestKey)
	}
}

type demandItem struct {
	Filter   nostr.Filter `json:"filter"`
	Active   int          `json:"active"`
	LastSeen time.Time    `json:"last_seen"`
}

// snapshot returns current demand, pruning entries with no active
// subscription whose last activity is older than the stale window.
func (d *demandTracker) snapshot() []demandItem {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := d.now()
	keys := make([]string, 0, len(d.entries))
	for k, e := range d.entries {
		if e.active == 0 && now.Sub(e.lastSeen) > d.stale {
			delete(d.entries, k)
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	items := make([]demandItem, 0, len(keys))
	for _, k := range keys {
		e := d.entries[k]
		items = append(items, demandItem{Filter: e.filter, Active: e.active, LastSeen: e.lastSeen})
	}
	return items
}

// handler serves GET /demand. Gate it with withAuthToken on
// publicly-reachable relays — the response reveals what clients are
// subscribed to.
func (d *demandTracker) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"demand": d.snapshot()})
	}
}

// withAuthToken guards an HTTP endpoint with a shared bearer token
// (AUTH_TOKEN): requests must carry "Authorization: Bearer <token>". With an
// empty token the endpoint is open — for deployments where the network is the
// auth. Comparison is constant-time.
func withAuthToken(token string, next http.HandlerFunc) http.HandlerFunc {
	if token == "" {
		return next
	}
	expected := []byte("Bearer " + token)
	return func(w http.ResponseWriter, r *http.Request) {
		got := []byte(r.Header.Get("Authorization"))
		if len(got) != len(expected) || subtle.ConstantTimeCompare(got, expected) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}
