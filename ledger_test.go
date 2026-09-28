package goloyaltyledger

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testTime(hour int) time.Time {
	return time.Date(2026, 1, 1, hour, 0, 0, 0, time.UTC)
}

func mustIssue(t *testing.T, l *Ledger, member string, amount int64, exp time.Time, ref string) *IssueResult {
	t.Helper()
	res, err := l.Issue(IssueRequest{MemberID: member, Amount: amount, ExpiresAt: exp, RefNo: ref})
	if err != nil {
		t.Fatalf("Issue: unexpected error: %v", err)
	}
	return res
}

func assertBalance(t *testing.T, l *Ledger, member string, want Balance) {
	t.Helper()
	got, err := l.Balance(member)
	if err != nil {
		t.Fatalf("Balance: %v", err)
	}
	if *got != want {
		t.Fatalf("Balance = %+v, want %+v", *got, want)
	}
}

func TestIssueAndBalance(t *testing.T) {
	clk := NewFakeClock(testTime(0))
	l := NewLedger(clk)

	mustIssue(t, l, "m1", 100, testTime(2), "i1")
	assertBalance(t, l, "m1", Balance{Available: 100, Consumed: 0, Expired: 0, Issued: 100})

	// Different members are independent.
	assertBalance(t, l, "m2", Balance{})
}

func TestConsumeIsFEFOAndAtomicAcrossBatches(t *testing.T) {
	clk := NewFakeClock(testTime(0))
	l := NewLedger(clk)

	b1 := mustIssue(t, l, "m", 30, testTime(2), "i1") // expires first
	b2 := mustIssue(t, l, "m", 50, testTime(5), "i2")

	res, err := l.Consume(ConsumeRequest{MemberID: "m", Amount: 40, RefNo: "c1"})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if len(res.Allocations) != 2 {
		t.Fatalf("want 2 allocations, got %d: %+v", len(res.Allocations), res.Allocations)
	}
	if res.Allocations[0] != (Allocation{BatchID: b1.BatchID, Amount: 30}) {
		t.Fatalf("first allocation = %+v, want all 30 of earliest batch", res.Allocations[0])
	}
	if res.Allocations[1] != (Allocation{BatchID: b2.BatchID, Amount: 10}) {
		t.Fatalf("second allocation = %+v, want 10 of later batch", res.Allocations[1])
	}

	batches, _ := l.Batches("m")
	if batches[0].Remaining != 0 || batches[0].Consumed != 30 {
		t.Fatalf("earliest batch state = %+v", batches[0])
	}
	if batches[1].Remaining != 40 || batches[1].Consumed != 10 {
		t.Fatalf("later batch state = %+v", batches[1])
	}
	assertBalance(t, l, "m", Balance{Available: 40, Consumed: 40, Expired: 0, Issued: 80})
}

func TestConsumeInsufficientIsAtomic(t *testing.T) {
	clk := NewFakeClock(testTime(0))
	l := NewLedger(clk)
	mustIssue(t, l, "m", 30, testTime(2), "i1")
	mustIssue(t, l, "m", 20, testTime(5), "i2")

	_, err := l.Consume(ConsumeRequest{MemberID: "m", Amount: 51, RefNo: "c-big"})
	if !errors.Is(err, ErrInsufficientPoints) {
		t.Fatalf("want ErrInsufficientPoints, got %v", err)
	}
	var ie *insufficientError
	if !errors.As(err, &ie) || ie.requested != 51 || ie.available != 50 {
		t.Fatalf("error detail = %v, want requested=51 available=50", err)
	}

	// Nothing changed: no consume entry and balance intact.
	assertBalance(t, l, "m", Balance{Available: 50, Consumed: 0, Expired: 0, Issued: 50})
	entries, _ := l.Entries("m")
	for _, e := range entries {
		if e.Type == EntryConsume {
			t.Fatalf("unexpected consume entry after failed consumption: %+v", e)
		}
	}
}

func TestConsumeIgnoresExpiredBatches(t *testing.T) {
	clk := NewFakeClock(testTime(0))
	l := NewLedger(clk)
	mustIssue(t, l, "m", 30, testTime(2), "i1")
	mustIssue(t, l, "m", 20, testTime(5), "i2")

	clk.SetNow(testTime(2)) // batch 1 reaches its expiry instant (half-open)
	_, err := l.Consume(ConsumeRequest{MemberID: "m", Amount: 21, RefNo: "c1"})
	if !errors.Is(err, ErrInsufficientPoints) {
		t.Fatalf("want ErrInsufficientPoints, got %v", err)
	}

	res, err := l.Consume(ConsumeRequest{MemberID: "m", Amount: 20, RefNo: "c2"})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if len(res.Allocations) != 1 || res.Allocations[0].Amount != 20 {
		t.Fatalf("want single 20-point allocation, got %+v", res.Allocations)
	}
	assertBalance(t, l, "m", Balance{Available: 0, Consumed: 20, Expired: 30, Issued: 50})
}

func TestExpiryBoundaryIsHalfOpen(t *testing.T) {
	base := testTime(0)
	clk := NewFakeClock(base)
	l := NewLedger(clk)
	exp := base.Add(time.Hour)
	mustIssue(t, l, "m", 10, exp, "i1")

	clk.SetNow(exp.Add(-time.Nanosecond))
	assertBalance(t, l, "m", Balance{Available: 10, Consumed: 0, Expired: 0, Issued: 10})

	clk.SetNow(exp) // exactly at expiry instant -> expired
	assertBalance(t, l, "m", Balance{Available: 0, Consumed: 0, Expired: 10, Issued: 10})
}

func TestConsumeIdempotentReturnsFirstResult(t *testing.T) {
	clk := NewFakeClock(testTime(0))
	l := NewLedger(clk)
	mustIssue(t, l, "m", 100, testTime(5), "i1")

	first, err := l.Consume(ConsumeRequest{MemberID: "m", Amount: 40, RefNo: "X"})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	second, err := l.Consume(ConsumeRequest{MemberID: "m", Amount: 40, RefNo: "X"})
	if err != nil {
		t.Fatalf("idempotent resubmission: %v", err)
	}
	if first.ConsumptionID != second.ConsumptionID || first.EntryID != second.EntryID {
		t.Fatalf("resubmission returned different record: %+v vs %+v", first, second)
	}
	if len(second.Allocations) != 1 || second.Allocations[0] != first.Allocations[0] {
		t.Fatalf("resubmission allocations differ: %+v vs %+v", first.Allocations, second.Allocations)
	}
	assertBalance(t, l, "m", Balance{Available: 60, Consumed: 40, Expired: 0, Issued: 100})

	// Same external number, different amount -> conflict.
	_, err = l.Consume(ConsumeRequest{MemberID: "m", Amount: 41, RefNo: "X"})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("want ErrConflict for reused number with different amount, got %v", err)
	}
}

func TestConsumeIdempotentUnderConcurrentDuplicates(t *testing.T) {
	clk := NewFakeClock(testTime(0))
	l := NewLedger(clk)
	mustIssue(t, l, "m", 100, testTime(5), "i1")

	const n = 20
	var wg sync.WaitGroup
	ids := make([]string, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := l.Consume(ConsumeRequest{MemberID: "m", Amount: 50, RefNo: "DUP"})
			ids[i], errs[i] = res.ConsumptionID, err
		}(i)
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("goroutine %d: %v", i, errs[i])
		}
		if ids[i] != ids[0] {
			t.Fatalf("goroutine %d got consumption %s, want %s", i, ids[i], ids[0])
		}
	}
	// Exactly one deduction.
	assertBalance(t, l, "m", Balance{Available: 50, Consumed: 50, Expired: 0, Issued: 100})
}

func TestConcurrentConsumesNeverOverdraw(t *testing.T) {
	clk := NewFakeClock(testTime(0))
	l := NewLedger(clk)
	mustIssue(t, l, "m", 1000, testTime(24*100), "i1")

	const goroutines, each = 16, 100
	var wg sync.WaitGroup
	var spent int64
	var failed int64
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				_, err := l.Consume(ConsumeRequest{
					MemberID: "m",
					Amount:   7,
					RefNo:    fmt.Sprintf("c-%d-%d", g, i),
				})
				switch {
				case err == nil:
					atomic.AddInt64(&spent, 7)
				case errors.Is(err, ErrInsufficientPoints):
					atomic.AddInt64(&failed, 1)
				default:
					t.Errorf("unexpected error: %v", err)
				}
			}
		}(g)
	}
	wg.Wait()

	if spent > 1000 {
		t.Fatalf("spent %d exceeds issued 1000", spent)
	}
	bal, _ := l.Balance("m")
	if bal.Available+bal.Consumed != 1000 {
		t.Fatalf("available+consumed = %d+%d, want 1000", bal.Available, bal.Consumed)
	}
	if bal.Available != 1000-spent {
		t.Fatalf("available = %d, want %d", bal.Available, 1000-spent)
	}
}

func TestReverseFullRestoresOriginalBatches(t *testing.T) {
	clk := NewFakeClock(testTime(0))
	l := NewLedger(clk)
	b1 := mustIssue(t, l, "m", 30, testTime(2), "i1")
	b2 := mustIssue(t, l, "m", 50, testTime(5), "i2")

	res, _ := l.Consume(ConsumeRequest{MemberID: "m", Amount: 40, RefNo: "c1"})
	rev, err := l.Reverse(ReverseRequest{MemberID: "m", ConsumptionRefNo: "c1", Amount: 40, RefNo: "r1"})
	if err != nil {
		t.Fatalf("Reverse: %v", err)
	}
	if rev.RestoredAvailable != 40 || rev.RestoredExpired != 0 {
		t.Fatalf("reverse = %+v", rev)
	}
	wantAlloc := []Allocation{{BatchID: b1.BatchID, Amount: 30}, {BatchID: b2.BatchID, Amount: 10}}
	if fmt.Sprint(rev.Allocations) != fmt.Sprint(wantAlloc) {
		t.Fatalf("reverse allocations = %+v, want %+v", rev.Allocations, wantAlloc)
	}
	// Same per-batch split as the original consumption, same result object.
	if fmt.Sprint(rev.Allocations) != fmt.Sprint(res.Allocations) {
		t.Fatalf("reverse split %+v != consume split %+v", rev.Allocations, res.Allocations)
	}
	assertBalance(t, l, "m", Balance{Available: 80, Consumed: 0, Expired: 0, Issued: 80})

	// Cumulative reversal must not exceed the original consumption.
	_, err = l.Reverse(ReverseRequest{MemberID: "m", ConsumptionRefNo: "c1", Amount: 1, RefNo: "r2"})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("want ErrConflict for over-reversal, got %v", err)
	}
}

func TestReversePartialUnwindsInOriginalOrder(t *testing.T) {
	clk := NewFakeClock(testTime(0))
	l := NewLedger(clk)
	b1 := mustIssue(t, l, "m", 30, testTime(2), "i1")
	b2 := mustIssue(t, l, "m", 50, testTime(5), "i2")
	l.Consume(ConsumeRequest{MemberID: "m", Amount: 40, RefNo: "c1"})

	// Reverse 25: entirely out of the first (earliest) batch's 30.
	rev1, err := l.Reverse(ReverseRequest{MemberID: "m", ConsumptionRefNo: "c1", Amount: 25, RefNo: "r1"})
	if err != nil {
		t.Fatalf("Reverse: %v", err)
	}
	if fmt.Sprint(rev1.Allocations) != fmt.Sprint([]Allocation{{BatchID: b1.BatchID, Amount: 25}}) {
		t.Fatalf("rev1 allocations = %+v", rev1.Allocations)
	}

	// Reverse the remaining 15: 5 left from batch 1, then 10 from batch 2.
	rev2, err := l.Reverse(ReverseRequest{MemberID: "m", ConsumptionRefNo: "c1", Amount: 15, RefNo: "r2"})
	if err != nil {
		t.Fatalf("Reverse: %v", err)
	}
	if fmt.Sprint(rev2.Allocations) != fmt.Sprint([]Allocation{
		{BatchID: b1.BatchID, Amount: 5},
		{BatchID: b2.BatchID, Amount: 10},
	}) {
		t.Fatalf("rev2 allocations = %+v", rev2.Allocations)
	}
	assertBalance(t, l, "m", Balance{Available: 80, Consumed: 0, Expired: 0, Issued: 80})

	// Batches retain their identity amounts.
	batches, _ := l.Batches("m")
	for _, b := range batches {
		if b.Remaining != b.Total || b.Consumed != 0 {
			t.Fatalf("batch %s not fully restored: %+v", b.ID, b)
		}
	}
}

func TestReverseAfterExpiryKeepsTraceButNotSpendable(t *testing.T) {
	clk := NewFakeClock(testTime(0))
	l := NewLedger(clk)

	// b1 expires early; b2 later. A 100-point spend takes 60 + 40.
	mustIssue(t, l, "m", 60, testTime(2), "i1")
	mustIssue(t, l, "m", 100, testTime(5), "i2")
	l.Consume(ConsumeRequest{MemberID: "m", Amount: 100, RefNo: "c1"})

	// Advance past b1's expiry. b1 had no remainder left (fully consumed),
	// but its consumed 60 must not resurrect when reversed.
	clk.SetNow(testTime(3))
	rev, err := l.Reverse(ReverseRequest{MemberID: "m", ConsumptionRefNo: "c1", Amount: 100, RefNo: "r1"})
	if err != nil {
		t.Fatalf("Reverse: %v", err)
	}
	if rev.RestoredAvailable != 40 || rev.RestoredExpired != 60 {
		t.Fatalf("reverse = %+v, want available 40 / expired 60", rev)
	}
	// b2 still holds its unspent 60 plus the restored 40; b1 is fully expired.
	assertBalance(t, l, "m", Balance{Available: 100, Consumed: 0, Expired: 60, Issued: 160})

	// The late-reversed portion is visible on the audit trail.
	var found bool
	entries, _ := l.Entries("m")
	for _, e := range entries {
		if e.Type == EntryReverse && e.ConsumptionRefNo == "c1" {
			found = true
			if e.Amount != 100 || e.RestoredExpired != 60 || e.RestoredAvailable != 40 {
				t.Fatalf("reverse entry = %+v", e)
			}
		}
	}
	if !found {
		t.Fatal("reverse entry missing from ledger trail")
	}
}

func TestReverseAfterRemainingExpiresThenFullReverse(t *testing.T) {
	clk := NewFakeClock(testTime(0))
	l := NewLedger(clk)
	mustIssue(t, l, "m", 100, testTime(2), "i1")

	// Consume 60; the other 40 expires before reversal.
	l.Consume(ConsumeRequest{MemberID: "m", Amount: 60, RefNo: "c1"})
	clk.SetNow(testTime(2))
	adv, err := l.AdvanceExpiry("m")
	if err != nil {
		t.Fatalf("AdvanceExpiry: %v", err)
	}
	if adv.Expired != 40 || adv.Entries != 1 {
		t.Fatalf("expiry result = %+v, want 40/1", adv)
	}

	// Reversing the consumed 60 after expiry adds to expired, not available.
	rev, err := l.Reverse(ReverseRequest{MemberID: "m", ConsumptionRefNo: "c1", Amount: 60, RefNo: "r1"})
	if err != nil {
		t.Fatalf("Reverse: %v", err)
	}
	if rev.RestoredAvailable != 0 || rev.RestoredExpired != 60 {
		t.Fatalf("reverse = %+v", rev)
	}
	assertBalance(t, l, "m", Balance{Available: 0, Consumed: 0, Expired: 100, Issued: 100})
}

func TestReverseIdempotentAndNotFound(t *testing.T) {
	clk := NewFakeClock(testTime(0))
	l := NewLedger(clk)
	mustIssue(t, l, "m", 100, testTime(5), "i1")
	l.Consume(ConsumeRequest{MemberID: "m", Amount: 30, RefNo: "c1"})

	first, err := l.Reverse(ReverseRequest{MemberID: "m", ConsumptionRefNo: "c1", Amount: 10, RefNo: "r1"})
	if err != nil {
		t.Fatalf("Reverse: %v", err)
	}
	second, err := l.Reverse(ReverseRequest{MemberID: "m", ConsumptionRefNo: "c1", Amount: 10, RefNo: "r1"})
	if err != nil {
		t.Fatalf("idempotent reverse: %v", err)
	}
	if first.EntryID != second.EntryID {
		t.Fatalf("reversal resubmission created another entry: %s vs %s", first.EntryID, second.EntryID)
	}
	assertBalance(t, l, "m", Balance{Available: 80, Consumed: 20, Expired: 0, Issued: 100})

	_, err = l.Reverse(ReverseRequest{MemberID: "m", ConsumptionRefNo: "missing", Amount: 1, RefNo: "r2"})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}

	// Same reversal number against a different consumption -> conflict.
	l.Consume(ConsumeRequest{MemberID: "m", Amount: 10, RefNo: "c2"})
	_, err = l.Reverse(ReverseRequest{MemberID: "m", ConsumptionRefNo: "c2", Amount: 10, RefNo: "r1"})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("want ErrConflict for reversal number reuse, got %v", err)
	}
}

func TestIssueIdempotent(t *testing.T) {
	clk := NewFakeClock(testTime(0))
	l := NewLedger(clk)
	first := mustIssue(t, l, "m", 50, testTime(5), "SAME")
	second, err := l.Issue(IssueRequest{MemberID: "m", Amount: 50, ExpiresAt: testTime(5), RefNo: "SAME"})
	if err != nil {
		t.Fatalf("idempotent issue: %v", err)
	}
	if first.BatchID != second.BatchID {
		t.Fatalf("resubmission created a second batch: %s vs %s", first.BatchID, second.BatchID)
	}
	assertBalance(t, l, "m", Balance{Available: 50, Issued: 50})

	_, err = l.Issue(IssueRequest{MemberID: "m", Amount: 51, ExpiresAt: testTime(5), RefNo: "SAME"})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("want ErrConflict, got %v", err)
	}
}

func TestLedgerEntriesAuditTrail(t *testing.T) {
	clk := NewFakeClock(testTime(0))
	l := NewLedger(clk)
	mustIssue(t, l, "m", 30, testTime(2), "i1")
	l.Consume(ConsumeRequest{MemberID: "m", Amount: 10, RefNo: "c1"})

	clk.SetNow(testTime(2))
	entries, err := l.Entries("m")
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("want issue/consume/expiry entries, got %d: %+v", len(entries), entries)
	}
	if entries[0].Type != EntryIssue || entries[0].Amount != 30 {
		t.Fatalf("entry0 = %+v", entries[0])
	}
	if entries[1].Type != EntryConsume || entries[1].Amount != 10 || entries[1].RefNo != "c1" {
		t.Fatalf("entry1 = %+v", entries[1])
	}
	if entries[2].Type != EntryExpiry || entries[2].Amount != 20 {
		t.Fatalf("entry2 = %+v", entries[2])
	}
	// Entries are strictly chronological.
	for i := 1; i < len(entries); i++ {
		if entries[i].CreatedAt.Before(entries[i-1].CreatedAt) {
			t.Fatalf("entries out of order at %d", i)
		}
	}
}

// verifyInvariants checks the internal batch/conservation identities.
func verifyInvariants(t *testing.T, l *Ledger, member string) {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	a, ok := l.account[member]
	if !ok {
		return
	}
	for _, b := range a.batches {
		if b.remaining < 0 || b.consumed < 0 || b.expired < 0 {
			t.Errorf("batch %s has negative counter: rem=%d cons=%d exp=%d",
				b.id, b.remaining, b.consumed, b.expired)
		}
		if b.remaining+b.consumed+b.expired != b.total {
			t.Errorf("batch %s breaks conservation: rem=%d cons=%d exp=%d total=%d",
				b.id, b.remaining, b.consumed, b.expired, b.total)
		}
	}
	for _, c := range a.consumptions {
		var partReversed, partAmount int64
		for _, p := range c.parts {
			partReversed += p.reversed
			partAmount += p.amount
			if p.reversed < 0 || p.reversed > p.amount {
				t.Errorf("consumption %s allocation %s reversed=%d out of [0,%d]",
					c.refNo, p.batchID, p.reversed, p.amount)
			}
		}
		if partAmount != c.amount {
			t.Errorf("consumption %s allocations sum %d != amount %d", c.refNo, partAmount, c.amount)
		}
		if partReversed != c.reversed {
			t.Errorf("consumption %s allocation reversals %d != reversed %d", c.refNo, partReversed, c.reversed)
		}
		if c.reversed > c.amount {
			t.Errorf("consumption %s reversed %d > amount %d", c.refNo, c.reversed, c.amount)
		}
	}
}

func TestConcurrentExpiryConsumeReverseInvariants(t *testing.T) {
	base := testTime(0)
	clk := NewFakeClock(base)
	l := NewLedger(clk)

	const batchCount = 30
	for i := 0; i < batchCount; i++ {
		mustIssue(t, l, "m", 10, base.Add(time.Duration(i+1)*time.Hour), fmt.Sprintf("i-%d", i))
	}

	var (
		wg        sync.WaitGroup
		idCounter int64
		refMu     sync.Mutex
		refs      []string
	)

	// Single clock advancer: ~16.7 hours, so roughly half the batches expire.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			clk.Advance(time.Minute)
		}
	}()

	// Consumers spend with unique external numbers.
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				id := atomic.AddInt64(&idCounter, 1)
				ref := fmt.Sprintf("c-%d", id)
				_, err := l.Consume(ConsumeRequest{MemberID: "m", Amount: id%7 + 1, RefNo: ref})
				if err == nil {
					refMu.Lock()
					refs = append(refs, ref)
					refMu.Unlock()
				} else if !errors.Is(err, ErrInsufficientPoints) {
					t.Errorf("unexpected consume error: %v", err)
				}
			}
		}(g)
	}

	// Reversers repeatedly reverse one point of known consumptions.
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				refMu.Lock()
				if len(refs) == 0 {
					refMu.Unlock()
					continue
				}
				ref := refs[(g*100+i)%len(refs)]
				refMu.Unlock()
				_, err := l.Reverse(ReverseRequest{
					MemberID:         "m",
					ConsumptionRefNo: ref,
					Amount:           1,
					RefNo:            fmt.Sprintf("r-%d-%d", g, i),
				})
				if err != nil && !errors.Is(err, ErrConflict) && !errors.Is(err, ErrNotFound) {
					t.Errorf("unexpected reverse error: %v", err)
				}
			}
		}(g)
	}

	// Readers hammer Balance/Batches/Entries.
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				if _, err := l.Balance("m"); err != nil {
					t.Errorf("Balance: %v", err)
				}
				if _, err := l.Batches("m"); err != nil {
					t.Errorf("Batches: %v", err)
				}
				if _, err := l.Entries("m"); err != nil {
					t.Errorf("Entries: %v", err)
				}
			}
		}()
	}

	wg.Wait()
	verifyInvariants(t, l, "m")

	// Available balance must equal the sum of remainders of batches not
	// yet expired (everything is applied up to the final clock time).
	bal, err := l.Balance("m")
	if err != nil {
		t.Fatalf("Balance: %v", err)
	}
	batches, _ := l.Batches("m")
	var avail int64
	for _, b := range batches {
		if b.Remaining < 0 || b.Consumed < 0 || b.Expired < 0 {
			t.Fatalf("negative batch counter: %+v", b)
		}
		if clk.Now().Before(b.ExpiresAt) {
			avail += b.Remaining
		} else if b.Remaining != 0 {
			t.Fatalf("expired batch %s still has remaining %d", b.ID, b.Remaining)
		}
	}
	if bal.Available != avail {
		t.Fatalf("balance available %d != recomputed %d", bal.Available, avail)
	}
	if bal.Issued != batchCount*10 {
		t.Fatalf("issued = %d, want %d", bal.Issued, batchCount*10)
	}
	if bal.Available+bal.Consumed+bal.Expired != bal.Issued {
		t.Fatalf("balance breaks conservation: %+v", bal)
	}
}

func TestInvalidArguments(t *testing.T) {
	clk := NewFakeClock(testTime(0))
	l := NewLedger(clk)

	cases := []struct {
		name string
		call func() error
	}{
		{"issue empty member", func() error { _, e := l.Issue(IssueRequest{Amount: 1, ExpiresAt: testTime(1)}); return e }},
		{"issue zero amount", func() error {
			_, e := l.Issue(IssueRequest{MemberID: "m", Amount: 0, ExpiresAt: testTime(1)})
			return e
		}},
		{"issue negative amount", func() error {
			_, e := l.Issue(IssueRequest{MemberID: "m", Amount: -1, ExpiresAt: testTime(1)})
			return e
		}},
		{"issue zero expiry", func() error { _, e := l.Issue(IssueRequest{MemberID: "m", Amount: 1}); return e }},
		{"issue past expiry", func() error {
			_, e := l.Issue(IssueRequest{MemberID: "m", Amount: 1, ExpiresAt: testTime(0)})
			return e
		}},
		{"consume empty member", func() error { _, e := l.Consume(ConsumeRequest{Amount: 1, RefNo: "x"}); return e }},
		{"consume zero amount", func() error { _, e := l.Consume(ConsumeRequest{MemberID: "m", Amount: 0, RefNo: "x"}); return e }},
		{"consume empty ref", func() error { _, e := l.Consume(ConsumeRequest{MemberID: "m", Amount: 1}); return e }},
		{"reverse empty member", func() error {
			_, e := l.Reverse(ReverseRequest{ConsumptionRefNo: "x", Amount: 1, RefNo: "r"})
			return e
		}},
		{"reverse empty consumption ref", func() error {
			_, e := l.Reverse(ReverseRequest{MemberID: "m", Amount: 1, RefNo: "r"})
			return e
		}},
		{"reverse zero amount", func() error {
			_, e := l.Reverse(ReverseRequest{MemberID: "m", ConsumptionRefNo: "x", Amount: 0, RefNo: "r"})
			return e
		}},
		{"balance empty member", func() error { _, e := l.Balance(""); return e }},
		{"advance empty member", func() error { _, e := l.AdvanceExpiry(""); return e }},
		{"batches empty member", func() error { _, e := l.Batches(""); return e }},
		{"entries empty member", func() error { _, e := l.Entries(""); return e }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("want ErrInvalidArgument, got %v", err)
			}
		})
	}
}
