package discord

import (
	"sync"
	"time"
)

type slidingCounter struct {
	mu     sync.Mutex
	window time.Duration
	max    int
	bucket map[string]counterBucket
}

type counterBucket struct {
	windowStart time.Time
	count       int
}

func newSlidingCounter(max int, window time.Duration) *slidingCounter {
	if window <= 0 {
		window = time.Minute
	}
	if max <= 0 {
		return nil
	}
	return &slidingCounter{
		window: window,
		max:    max,
		bucket: make(map[string]counterBucket),
	}
}

func (s *slidingCounter) Allow(key string, now time.Time) bool {
	if s == nil {
		return true
	}
	if key == "" {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	b := s.bucket[key]
	if b.windowStart.IsZero() || now.Sub(b.windowStart) >= s.window {
		b.windowStart = now
		b.count = 0
	}
	if b.count >= s.max {
		s.bucket[key] = b
		return false
	}
	b.count++
	s.bucket[key] = b
	return true
}
