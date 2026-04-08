package discord

import (
	"testing"
	"time"
)

func TestSlidingCounterAllow(t *testing.T) {
	c := newSlidingCounter(2, time.Minute)
	now := time.Now().UTC()

	if !c.Allow("k1", now) {
		t.Fatalf("first call should pass")
	}
	if !c.Allow("k1", now) {
		t.Fatalf("second call should pass")
	}
	if c.Allow("k1", now) {
		t.Fatalf("third call should be rate-limited")
	}
	if !c.Allow("k1", now.Add(2*time.Minute)) {
		t.Fatalf("window should reset after duration")
	}
}
