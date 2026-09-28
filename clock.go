// Package goloyaltyledger implements an in-memory loyalty-points ledger with
// expiring batches.
//
// Points are issued into immutable batches, each carrying its own expiry
// instant and its own remaining / consumed / expired counters. Consumption
// spends the batches that expire first (FEFO). A consumption that spans
// several batches is atomic: either all required points are reserved or
// nothing is. Reversal restores the exact per-batch allocations of the
// original consumption rather than adding to a single global balance; the
// portion that has already expired is preserved on the audit trail but can
// never become spendable again.
//
// Every time boundary ("has this batch expired yet?") is evaluated against
// one Clock held by the Ledger, so issuance, consumption, reversal and
// expiry advancement share a single notion of "now".
package goloyaltyledger

import (
	"sync"
	"time"
)

// Clock is the single source of "current time" used by a Ledger.
// Implementations must be safe for concurrent use.
type Clock interface {
	Now() time.Time
}

// systemClock returns the wall-clock time.
type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// SystemClock returns a Clock backed by time.Now.
func SystemClock() Clock { return systemClock{} }

// FakeClock is a manually-controlled Clock intended for tests.
// It is safe for concurrent use.
type FakeClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewFakeClock returns a FakeClock initialized to t.
func NewFakeClock(t time.Time) *FakeClock {
	return &FakeClock{now: t}
}

// Now returns the controlled current time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// SetNow moves the clock to t (backward movement is allowed).
func (c *FakeClock) SetNow(t time.Time) {
	c.mu.Lock()
	c.now = t
	c.mu.Unlock()
}

// Advance moves the clock forward by d.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}
