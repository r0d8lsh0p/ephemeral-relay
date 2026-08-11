// End-to-end test against a running ephemeral-relay.
//
// Run the relay with short retention first, e.g.:
//   RETENTION_SECONDS=8 PURGE_INTERVAL_SECONDS=2 ./ephemeral-relay
// then:
//   go run ./e2e -relay ws://localhost:3335
//
// Verifies:
//  1. kind 1 is rejected (not in the allowlist)
//  2. kind 1311 is accepted and served
//  3. kind 0 is accepted
//  4. after the retention window, the 1311 is purged but the kind 0 survives
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/nbd-wtf/go-nostr"
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

func publish(ctx context.Context, url string, sk string, kind int, content string, tags nostr.Tags) (string, error) {
	relay, err := nostr.RelayConnect(ctx, url)
	if err != nil {
		return "", err
	}
	defer relay.Close()

	pub, _ := nostr.GetPublicKey(sk)
	evt := nostr.Event{
		PubKey:    pub,
		CreatedAt: nostr.Now(),
		Kind:      kind,
		Tags:      tags,
		Content:   content,
	}
	if err := evt.Sign(sk); err != nil {
		return "", err
	}
	return evt.ID, relay.Publish(ctx, evt)
}

func queryIDs(ctx context.Context, url string, filter nostr.Filter) (map[string]bool, error) {
	relay, err := nostr.RelayConnect(ctx, url)
	if err != nil {
		return nil, err
	}
	defer relay.Close()

	events, err := relay.QuerySync(ctx, filter)
	if err != nil {
		return nil, err
	}
	ids := make(map[string]bool, len(events))
	for _, evt := range events {
		ids[evt.ID] = true
	}
	return ids, nil
}

func main() {
	url := flag.String("relay", "ws://localhost:3335", "relay websocket URL")
	retention := flag.Int("retention", 8, "relay's RETENTION_SECONDS (test waits past it)")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	chatterSK := nostr.GeneratePrivateKey()
	chatterPK, _ := nostr.GetPublicKey(chatterSK)

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
	ids, err := queryIDs(ctx, *url, nostr.Filter{Authors: []string{chatterPK}})
	if err != nil {
		log.Fatalf("query failed: %v", err)
	}
	check("1311 served before expiry", ids[chatID], "1311 missing from query")
	check("kind 0 served", ids[profileID], "profile missing from query")

	// 4. wait past retention + a purge cycle
	wait := time.Duration(*retention+6) * time.Second
	fmt.Printf("...waiting %s for retention + purge...\n", wait)
	time.Sleep(wait)

	ids, err = queryIDs(ctx, *url, nostr.Filter{Authors: []string{chatterPK}})
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
