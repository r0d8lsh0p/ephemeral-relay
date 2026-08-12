package main

import (
	"strings"
	"testing"
	"time"

	"fiatjaf.com/nostr"
)

func TestIsExpired(t *testing.T) {
	now := nostr.Timestamp(1_000_000)
	cases := []struct {
		name string
		tags nostr.Tags
		want bool
	}{
		{"no tag", nostr.Tags{{"a", "30311:x:y"}}, false},
		{"future expiry", nostr.Tags{{"expiration", "1000001"}}, false},
		{"exactly now", nostr.Tags{{"expiration", "1000000"}}, true},
		{"past expiry", nostr.Tags{{"expiration", "999999"}}, true},
		{"zero counts as expired", nostr.Tags{{"expiration", "0"}}, true},
		{"unparseable ignored", nostr.Tags{{"expiration", "soon"}}, false},
		{"empty value ignored", nostr.Tags{{"expiration"}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			evt := nostr.Event{Kind: 1311, Tags: tc.tags}
			if got := isExpired(evt, now); got != tc.want {
				t.Errorf("isExpired(%v) = %v, want %v", tc.tags, got, tc.want)
			}
		})
	}
}

func TestEnvKinds(t *testing.T) {
	t.Run("default used when unset", func(t *testing.T) {
		got := envKinds("EPH_TEST_KINDS_UNSET", "1,2,3")
		if len(got) != 3 || got[0] != 1 || got[2] != 3 {
			t.Errorf("got %v", got)
		}
	})
	t.Run("env overrides with spaces and trailing comma", func(t *testing.T) {
		t.Setenv("EPH_TEST_KINDS", " 0, 1311 ,9735,")
		got := envKinds("EPH_TEST_KINDS", "1")
		want := []nostr.Kind{0, 1311, 9735}
		if len(got) != len(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("got %v, want %v", got, want)
			}
		}
	})
}

func TestEnvList(t *testing.T) {
	t.Setenv("EPH_TEST_LIST", " 10.0.0.1 ,, 10.0.0.2 ")
	got := envList("EPH_TEST_LIST")
	if len(got) != 2 || got[0] != "10.0.0.1" || got[1] != "10.0.0.2" {
		t.Errorf("got %v", got)
	}
	if envList("EPH_TEST_LIST_UNSET") != nil {
		t.Error("expected nil for unset var")
	}
}

func TestRetainedKinds(t *testing.T) {
	cfg := Config{
		AllowedKinds: []nostr.Kind{0, 7, 1311, 9735},
		ExemptKinds:  []nostr.Kind{0, 9735},
	}
	got := retainedKinds(cfg)
	if len(got) != 2 || got[0] != 7 || got[1] != 1311 {
		t.Errorf("retainedKinds = %v, want [7 1311]", got)
	}
}

func TestJoinKinds(t *testing.T) {
	if s := joinKinds([]nostr.Kind{0, 1311, 9735}); s != "0,1311,9735" {
		t.Errorf("got %q", s)
	}
}

func TestPreventLargeTags(t *testing.T) {
	policy := preventLargeTags(10)
	big := strings.Repeat("x", 11)
	cases := []struct {
		name   string
		tags   nostr.Tags
		reject bool
	}{
		{"small indexable value ok", nostr.Tags{{"a", "short"}}, false},
		{"large indexable value rejected", nostr.Tags{{"a", big}}, true},
		{"large non-indexable value ok", nostr.Tags{{"expiration", big}}, false},
		{"valueless tag ok", nostr.Tags{{"-"}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reject, _ := policy(t.Context(), nostr.Event{Tags: tc.tags})
			if reject != tc.reject {
				t.Errorf("reject = %v, want %v", reject, tc.reject)
			}
		})
	}
}

func TestIPLimiter(t *testing.T) {
	clock := time.Unix(1_000_000, 0)
	now := func() time.Time { return clock }

	t.Run("burst then refill", func(t *testing.T) {
		l := newIPLimiter(10, 3, nil, now)
		for i := 0; i < 3; i++ {
			if !l.allow("1.2.3.4") {
				t.Fatalf("request %d within burst should pass", i)
			}
		}
		if l.allow("1.2.3.4") {
			t.Fatal("request past burst allowance should be limited")
		}
		clock = clock.Add(100 * time.Millisecond) // refills 1 token at 10/s
		if !l.allow("1.2.3.4") {
			t.Fatal("request after refill should pass")
		}
		if l.allow("1.2.3.4") {
			t.Fatal("refill grants exactly one token, second should fail")
		}
	})

	t.Run("localhost gets no special treatment", func(t *testing.T) {
		l := newIPLimiter(10, 2, nil, now)
		l.allow("127.0.0.1")
		l.allow("127.0.0.1")
		if l.allow("127.0.0.1") {
			t.Fatal("localhost must be rate-limited like any other IP")
		}
	})

	t.Run("trusted IP bypasses entirely", func(t *testing.T) {
		l := newIPLimiter(10, 2, map[string]bool{"10.0.0.9": true}, now)
		for i := 0; i < 100; i++ {
			if !l.allow("10.0.0.9") {
				t.Fatal("trusted IP must never be limited")
			}
		}
	})

	t.Run("buckets are per-IP", func(t *testing.T) {
		l := newIPLimiter(10, 1, nil, now)
		if !l.allow("1.1.1.1") {
			t.Fatal("first IP first request should pass")
		}
		if !l.allow("2.2.2.2") {
			t.Fatal("second IP must have its own bucket")
		}
		if l.allow("1.1.1.1") {
			t.Fatal("first IP should be exhausted")
		}
	})

	t.Run("token cap at burst", func(t *testing.T) {
		l := newIPLimiter(10, 2, nil, now)
		l.allow("3.3.3.3")
		clock = clock.Add(time.Hour) // long idle must not exceed burst cap
		if !l.allow("3.3.3.3") || !l.allow("3.3.3.3") {
			t.Fatal("burst tokens should be available after idle")
		}
		if l.allow("3.3.3.3") {
			t.Fatal("tokens must cap at burst, not accumulate for an hour")
		}
	})
}
