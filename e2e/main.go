// End-to-end test against a running ephemeral-relay.
//
// Run the relay with short retention first, e.g.:
//
//	RETENTION_SECONDS=8 PURGE_INTERVAL_SECONDS=2 ./ephemeral-relay
//
// then:
//
//	go run ./e2e -relay ws://localhost:3335
//
// Verifies:
//  1. kind 1 is rejected (not in the allowlist)
//  2. kind 1311 is accepted and served
//  3. kind 0 is accepted
//  4. after the retention window, the 1311 is purged but the kind 0 survives
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"fiatjaf.com/nostr"
)

var failures int

func check(name string, ok bool, detail string) {
	if ok {
		fmt.Printf("PASS  %s\n", name)
		return
	}
	failures++
	fmt.Printf("FAIL  %s — %s\n", name, detail)
}

func publish(ctx context.Context, url string, sk nostr.SecretKey, kind nostr.Kind, content string, tags nostr.Tags) (nostr.ID, error) {
	relay, err := nostr.RelayConnect(ctx, url, nostr.RelayOptions{})
	if err != nil {
		return nostr.ID{}, err
	}
	defer relay.Close()

	evt := nostr.Event{
		PubKey:    nostr.GetPublicKey(sk),
		CreatedAt: nostr.Now(),
		Kind:      kind,
		Tags:      tags,
		Content:   content,
	}
	if err := evt.Sign(sk); err != nil {
		return nostr.ID{}, err
	}
	return evt.ID, relay.Publish(ctx, evt)
}

func queryIDs(ctx context.Context, url string, filter nostr.Filter) (map[nostr.ID]bool, error) {
	relay, err := nostr.RelayConnect(ctx, url, nostr.RelayOptions{})
	if err != nil {
		return nil, err
	}
	defer relay.Close()

	ids := make(map[nostr.ID]bool)
	for evt := range relay.QueryEvents(filter) {
		ids[evt.ID] = true
	}
	return ids, nil
}

func main() {
	url := flag.String("relay", "ws://localhost:3335", "relay websocket URL")
	retention := flag.Int("retention", 8, "relay's RETENTION_SECONDS (test waits past it)")
	burstOnly := flag.Bool("burst-only", false, "only run the rate-limit burst check")
	burstTrusted := flag.Bool("burst-trusted", false, "expect the burst to fully succeed (our IP is in TRUSTED_IPS)")
	nip40 := flag.Bool("nip40", false, "only run the NIP-40 expiration check")
	ttl := flag.Int("ttl", 180, "NIP-40 TTL in seconds for the expiring event")
	nip70 := flag.Bool("nip70", false, "only run the NIP-70 protected-event check")
	demand := flag.Bool("demand", false, "only run the demand-endpoint check")
	authToken := flag.String("auth-token", "", "AUTH_TOKEN bearer for gated endpoints (tests auth when set)")
	flag.Parse()

	if *burstOnly {
		runBurstCheck(*url, *burstTrusted)
		if failures > 0 {
			os.Exit(1)
		}
		return
	}
	if *nip40 {
		runNip40Check(*url, *ttl)
		if failures > 0 {
			os.Exit(1)
		}
		return
	}
	if *nip70 {
		runNip70Check(*url)
		if failures > 0 {
			os.Exit(1)
		}
		return
	}
	if *demand {
		runDemandCheck(*url, *authToken)
		if failures > 0 {
			os.Exit(1)
		}
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	chatterSK := nostr.Generate()
	chatterPK := nostr.GetPublicKey(chatterSK)

	// 1. kind 1 must be rejected
	_, err := publish(ctx, *url, chatterSK, 1, "this should be refused", nil)
	check("kind 1 rejected", err != nil && strings.Contains(err.Error(), "kind"),
		fmt.Sprintf("expected kind-restriction error, got %v", err))

	// 2. kind 1311 accepted
	chatID, err := publish(ctx, *url, chatterSK, 1311, "hello from the e2e test",
		nostr.Tags{{"a", "30311:deadbeef:test-stream"}})
	check("kind 1311 accepted", err == nil, fmt.Sprintf("publish failed: %v", err))

	// 3. kind 0 accepted
	profileID, err := publish(ctx, *url, chatterSK, 0, `{"name":"e2e chatter"}`, nil)
	check("kind 0 accepted", err == nil, fmt.Sprintf("publish failed: %v", err))

	// both served right now
	ids, err := queryIDs(ctx, *url, nostr.Filter{Authors: []nostr.PubKey{chatterPK}})
	if err != nil {
		log.Fatalf("query failed: %v", err)
	}
	check("1311 served before expiry", ids[chatID], "1311 missing from query")
	check("kind 0 served", ids[profileID], "profile missing from query")

	// 4. wait past retention + a purge cycle
	wait := time.Duration(*retention+6) * time.Second
	fmt.Printf("...waiting %s for retention + purge...\n", wait)
	time.Sleep(wait)

	ids, err = queryIDs(ctx, *url, nostr.Filter{Authors: []nostr.PubKey{chatterPK}})
	if err != nil {
		log.Fatalf("query failed: %v", err)
	}
	check("1311 purged after retention", !ids[chatID], "1311 still served after retention window")
	check("kind 0 survives purge", ids[profileID], "profile was deleted — exemption broken")

	if failures > 0 {
		fmt.Printf("\n%d failure(s)\n", failures)
		os.Exit(1)
	}
	fmt.Println("\nall checks passed")
}

// runNip40Check publishes a 1311 with a short NIP-40 TTL and a control 1311
// without one, then polls until the expiring event vanishes while the control
// must survive. Also verifies an already-expired event is rejected outright.
// Run against a relay whose RETENTION_SECONDS comfortably exceeds the TTL so
// the age purge cannot be what deletes the event.
func runNip40Check(url string, ttlSecs int) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(ttlSecs)*time.Second+3*time.Minute)
	defer cancel()

	sk := nostr.Generate()
	pk := nostr.GetPublicKey(sk)
	expiry := time.Now().Add(time.Duration(ttlSecs) * time.Second).Unix()

	// already-expired events must be refused at the door
	_, err := publish(ctx, url, sk, 1311, "born dead",
		nostr.Tags{{"a", "30311:deadbeef:nip40-stream"}, {"expiration", fmt.Sprintf("%d", time.Now().Unix()-60)}})
	check("pre-expired event rejected", err != nil && strings.Contains(err.Error(), "expired"),
		fmt.Sprintf("expected expiry rejection, got %v", err))

	expiringID, err := publish(ctx, url, sk, 1311, "I am mortal",
		nostr.Tags{{"a", "30311:deadbeef:nip40-stream"}, {"expiration", fmt.Sprintf("%d", expiry)}})
	check("expiring 1311 accepted", err == nil, fmt.Sprintf("publish failed: %v", err))

	controlID, err := publish(ctx, url, sk, 1311, "I am the control",
		nostr.Tags{{"a", "30311:deadbeef:nip40-stream"}})
	check("control 1311 accepted", err == nil, fmt.Sprintf("publish failed: %v", err))

	ids, err := queryIDs(ctx, url, nostr.Filter{Authors: []nostr.PubKey{pk}})
	if err != nil {
		log.Fatalf("query failed: %v", err)
	}
	check("expiring event served before TTL", ids[expiringID], "expiring event missing pre-TTL")
	check("control served", ids[controlID], "control missing")

	// poll until the expiring event disappears
	fmt.Printf("...polling every 10s for the %ds TTL to lapse...\n", ttlSecs)
	deadline := time.Now().Add(time.Duration(ttlSecs)*time.Second + 2*time.Minute)
	gone := false
	for time.Now().Before(deadline) {
		time.Sleep(10 * time.Second)
		ids, err = queryIDs(ctx, url, nostr.Filter{Authors: []nostr.PubKey{pk}})
		if err != nil {
			log.Fatalf("poll query failed: %v", err)
		}
		elapsed := time.Until(time.Unix(expiry, 0)) * -1
		fmt.Printf("   t%+ds: expiring=%v control=%v\n", int(elapsed.Seconds()), ids[expiringID], ids[controlID])
		if !ids[expiringID] {
			gone = true
			check("expiring event gone no earlier than its TTL", time.Now().Unix() >= expiry,
				"event vanished before its expiration time")
			break
		}
	}
	check("expiring event deleted after TTL", gone, "event still served 2m past expiry")
	check("control survives", ids[controlID], "control was deleted — NIP-40 sweep overreached")
}

// runNip70Check verifies protected-event (NIP-70 "-" tag) handling, enforced
// by khatru core: unauthenticated publishes get an AUTH challenge, the author
// can publish after authenticating, and a *different* authenticated client
// rebroadcasting the same signed event is refused — the anti-gossip property.
func runNip70Check(url string) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	authorSK := nostr.Generate()
	authorPK := nostr.GetPublicKey(authorSK)
	strangerSK := nostr.Generate()

	evt := nostr.Event{
		PubKey:    authorPK,
		CreatedAt: nostr.Now(),
		Kind:      1311,
		Tags:      nostr.Tags{{"a", "30311:deadbeef:nip70-stream"}, {"-"}},
		Content:   "protected chat message",
	}
	if err := evt.Sign(authorSK); err != nil {
		log.Fatalf("sign failed: %v", err)
	}

	// 1. unauthenticated publish → auth-required
	relay1, err := nostr.RelayConnect(ctx, url, nostr.RelayOptions{})
	if err != nil {
		log.Fatalf("connect failed: %v", err)
	}
	err = relay1.Publish(ctx, evt)
	check("protected event challenged when unauthenticated",
		err != nil && strings.Contains(err.Error(), "auth-required"),
		fmt.Sprintf("expected auth-required, got %v", err))

	// 2. authenticate as the author → accepted
	err = relay1.Auth(ctx, func(ctx context.Context, authEvent *nostr.Event) error { return authEvent.Sign(authorSK) })
	check("author NIP-42 auth accepted", err == nil, fmt.Sprintf("auth failed: %v", err))
	err = relay1.Publish(ctx, evt)
	check("author can publish protected event", err == nil, fmt.Sprintf("publish failed: %v", err))
	relay1.Close()

	// 3. a different authenticated client rebroadcasts the same signed event → blocked
	relay2, err := nostr.RelayConnect(ctx, url, nostr.RelayOptions{})
	if err != nil {
		log.Fatalf("connect failed: %v", err)
	}
	defer relay2.Close()
	_ = relay2.Publish(ctx, evt) // provoke the AUTH challenge
	_ = relay2.Auth(ctx, func(ctx context.Context, authEvent *nostr.Event) error { return authEvent.Sign(strangerSK) })
	err = relay2.Publish(ctx, evt)
	check("rebroadcast by non-author refused",
		err != nil && strings.Contains(err.Error(), "author"),
		fmt.Sprintf("expected author-mismatch rejection, got %v", err))

	// 4. the accepted event is served normally
	ids, err := queryIDs(ctx, url, nostr.Filter{Authors: []nostr.PubKey{authorPK}})
	if err != nil {
		log.Fatalf("query failed: %v", err)
	}
	check("protected event served", ids[evt.ID], "protected event missing from query")
}

// runDemandCheck verifies the GET /demand endpoint against a running relay
// (start it with DEMAND_ENDPOINT=true): every open subscription appears as a
// filter entry; a consumer selects the ones it cares about (here, by #a). A
// firehose is present but unselectable; unsubscribing drops active to 0; with
// a token configured, unauthenticated requests get 401.
func runDemandCheck(url string, token string) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	demandURL := strings.Replace(url, "ws", "http", 1) + "/demand"

	type item struct {
		Filter map[string]any `json:"filter"`
		Active int            `json:"active"`
	}
	fetch := func(withToken bool) (int, []item) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, demandURL, nil)
		if withToken && token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Fatalf("GET /demand failed: %v", err)
		}
		defer resp.Body.Close()
		var body struct {
			Demand []item `json:"demand"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&body)
		return resp.StatusCode, body.Demand
	}

	hasA := func(it item, room string) bool {
		vals, _ := it.Filter["#a"].([]any)
		for _, v := range vals {
			if v == room {
				return true
			}
		}
		return false
	}
	activeFor := func(room string) int {
		_, items := fetch(true)
		for _, it := range items {
			if hasA(it, room) {
				return it.Active
			}
		}
		return -1
	}

	waitFor := func(cond func() bool, what string) {
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if cond() {
				check(what, true, "")
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
		check(what, false, "timed out")
	}

	if token != "" {
		status, _ := fetch(false)
		check("demand endpoint rejects missing token", status == http.StatusUnauthorized,
			fmt.Sprintf("status %d, want 401", status))
	}
	status, _ := fetch(true)
	check("demand endpoint reachable", status == http.StatusOK, fmt.Sprintf("status %d", status))

	relay, err := nostr.RelayConnect(ctx, url, nostr.RelayOptions{})
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer relay.Close()

	const room = "30311:deadbeef:demand-e2e"

	firehose, err := relay.Subscribe(ctx, nostr.Filter{Kinds: []nostr.Kind{1311}}, nostr.SubscriptionOptions{})
	if err != nil {
		log.Fatalf("firehose subscribe: %v", err)
	}
	defer firehose.Unsub()
	waitFor(func() bool { _, items := fetch(true); return len(items) >= 1 }, "firehose appears as an entry")
	check("firehose entry not selectable by #a", activeFor(room) == -1, "firehose matched #a selection")

	sub, err := relay.Subscribe(ctx, nostr.Filter{
		Kinds: []nostr.Kind{1311},
		Tags:  nostr.TagMap{"a": []string{room}},
	}, nostr.SubscriptionOptions{})
	if err != nil {
		log.Fatalf("scoped subscribe: %v", err)
	}
	waitFor(func() bool { return activeFor(room) == 1 }, "scoped subscription selectable with active=1")

	sub.Unsub()
	waitFor(func() bool { return activeFor(room) == 0 }, "unsubscribe drops active to 0")
}

// runBurstCheck publishes 80 chat events as fast as possible over one
// connection — simulating the bridge's single egress fanning in 50+ streams'
// chat. Trusted mode expects zero rejections; untrusted expects the limiter
// to bite (default burst allowance is 50).
//
// Run both modes against a locally-run relay binary so the client arrives
// from 127.0.0.1: that makes the untrusted check prove localhost gets no
// special exemption (a hardcoded one in an upstream limiter once slipped
// through when this check only ran behind docker's NAT), and it's what makes
// the trusted check's full acceptance attributable to TRUSTED_IPS.
func runBurstCheck(url string, expectAllAccepted bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	relay, err := nostr.RelayConnect(ctx, url, nostr.RelayOptions{})
	if err != nil {
		log.Fatalf("connect failed: %v", err)
	}
	defer relay.Close()

	sk := nostr.Generate()
	pk := nostr.GetPublicKey(sk)
	accepted, rejected := 0, 0
	for i := 0; i < 80; i++ {
		evt := nostr.Event{
			PubKey:    pk,
			CreatedAt: nostr.Now(),
			Kind:      1311,
			Tags:      nostr.Tags{{"a", "30311:deadbeef:burst-stream"}},
			Content:   fmt.Sprintf("burst message %d", i),
		}
		if err := evt.Sign(sk); err != nil {
			log.Fatalf("sign failed: %v", err)
		}
		if err := relay.Publish(ctx, evt); err != nil {
			rejected++
		} else {
			accepted++
		}
	}
	fmt.Printf("burst: %d accepted, %d rejected\n", accepted, rejected)
	if expectAllAccepted {
		check("trusted burst fully accepted", rejected == 0,
			fmt.Sprintf("%d of 80 rejected despite trusted IP", rejected))
	} else {
		check("untrusted burst rate-limited", rejected > 0, "80 rapid events all accepted — limiter inactive")
	}
}
