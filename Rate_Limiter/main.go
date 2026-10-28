package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

func main() {}

type RateLimiter struct {
	tick   time.Duration
	events []time.Time
	size   int
	mu     *sync.Mutex
	done   chan struct{}
}

func NewRateLimiter(ctx context.Context, size int, t time.Duration) (*RateLimiter, error) {
	if size <= 0 {
		return nil, fmt.Errorf("invalid size")
	}
	if t <= 0 {
		return nil, fmt.Errorf("invalid duration")
	}
	r := &RateLimiter{
		tick:   t,
		events: make([]time.Time, 0, size),
		size:   size,
		mu:     &sync.Mutex{},
		done:   make(chan struct{}),
	}
	go func() {
		<-ctx.Done()
		close(r.done)
	}()
	return r, nil
}

type res struct {
	b   bool
	err error
}

// sliding window
// более медленный но защищен от всплесков активности
func (r *RateLimiter) Add(t time.Time) (bool, error) {
	target := t.Add(-r.tick)
	select {
	case <-r.done:
		return false, fmt.Errorf("limiter close!")
	default:
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	idx := -1
	for i := range r.events {
		if r.events[i].Equal(target) || r.events[i].Before(target) {
			idx = i
		} else {
			break
		}
	}
	select {
	case <-r.done:
		return false, fmt.Errorf("limiter close!")
	default:
		r.events = r.events[idx+1:]
		if len(r.events) < r.size {
			r.events = r.events[idx:]
			return true, nil
		}
		return false, nil
	}
}

// token bucket
// более быстрый но не защищен от всплесков активности
type RateLimiterBucket struct {
	tick   time.Duration
	tokens int
	max    int
	mu     *sync.Mutex
	last   time.Time
	done   chan struct{}
}

func NewRateLimiterBucket(ctx context.Context, max int, t time.Duration) (*RateLimiterBucket, error) {
	if max <= 0 {
		return nil, fmt.Errorf("invalid size")
	}
	if t <= 0 {
		return nil, fmt.Errorf("invalid duration")
	}
	r := &RateLimiterBucket{
		tick:   t,
		tokens: max,
		max:    max,
		mu:     &sync.Mutex{},
		last:   time.Now(),
		done:   make(chan struct{}),
	}
	go func() {
		<-ctx.Done()
		close(r.done)
	}()
	return r, nil
}
func (r *RateLimiterBucket) Add(t time.Time) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	select {
	case <-r.done:
		return false, fmt.Errorf("limiter closed")
	default:
	}

	if t.Before(r.last) {
		return false, fmt.Errorf("time moved backwards")
	}

	newTokens := int(t.Sub(r.last) / r.tick)

	if newTokens > 0 {
		available := r.max - r.tokens

		if newTokens >= available {
			r.tokens = r.max
			r.last = t
		} else {
			r.tokens += newTokens
			r.last = r.last.Add(
				time.Duration(newTokens) * r.tick,
			)
		}
	}

	if r.tokens == 0 {
		return false, nil
	}

	r.tokens--
	return true, nil
}
