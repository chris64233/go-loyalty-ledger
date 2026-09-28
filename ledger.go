package goloyaltyledger

import (
	"sort"
	"strconv"
	"sync"
	"time"
)

// EntryType identifies the kind of ledger entry.
type EntryType string

const (
	// EntryIssue records a points issuance into a new batch.
	EntryIssue EntryType = "issue"
	// EntryConsume records a successful consumption across batches.
	EntryConsume EntryType = "consume"
	// EntryReverse records a reversal, restoring (part of) a consumption.
	EntryReverse EntryType = "reverse"
	// EntryExpiry records the expiry of the remaining points of a batch.
	EntryExpiry EntryType = "expiry"
)

// Allocation is one per-batch slice of a consumption (or its reversal).
type Allocation struct {
	BatchID string
	Amount  int64
}

// BatchView is an immutable snapshot of one points batch.
//
// The identity Total = Remaining + Consumed + Expired always holds.
// Consumed counts points covered by consumptions that have not been fully
// reversed (a reversal moves points back from Consumed into Remaining, or
// into Expired if reversal happens after the batch expiry instant).
type BatchView struct {
	ID        string
	IssuedAt  time.Time
	ExpiresAt time.Time
	Total     int64
	Remaining int64
	Consumed  int64
	Expired   int64
}

// Available reports whether the batch can still be spent at time t:
// it has remaining points and the expiry instant has not been reached.
// Expiry is half-open: a batch is available when t.Before(ExpiresAt).
func (b BatchView) Available(t time.Time) bool {
	return b.Remaining > 0 && t.Before(b.ExpiresAt)
}

// Balance is a snapshot of a member's points totals.
type Balance struct {
	// Available is spendable right now: sum of unexpired batch remainders.
	Available int64
	// Consumed is held by consumptions not yet fully reversed.
	Consumed int64
	// Expired is permanently gone (batch expiry or late reversal).
	Expired int64
	// Issued is the grand total ever issued (Available+Consumed+Expired).
	Issued int64
}

// Entry is an immutable ledger entry. Amount is always non-negative;
// interpret it through Type.
//
//	issue    : Amount credited into BatchID
//	consume  : Amount debited; Allocations shows the per-batch split
//	reverse  : Amount restored per Allocations, split into
//	           RestoredAvailable (spendable again) and RestoredExpired
//	           (batch had already expired; traceable, never spendable)
//	expiry   : Amount moved from the batch remainder to expired
type Entry struct {
	ID                string
	MemberID          string
	Type              EntryType
	Amount            int64
	BatchID           string
	RefNo             string
	ConsumptionRefNo  string
	Allocations       []Allocation
	RestoredAvailable int64
	RestoredExpired   int64
	CreatedAt         time.Time
}

// IssueRequest grants points to a member.
type IssueRequest struct {
	MemberID  string
	Amount    int64
	ExpiresAt time.Time
	// RefNo optionally deduplicates repeated submissions; a reused number
	// with different content is rejected with ErrConflict.
	RefNo string
}

// IssueResult is the outcome of an issuance.
type IssueResult struct {
	BatchID   string
	EntryID   string
	Amount    int64
	ExpiresAt time.Time
	IssuedAt  time.Time
}

// ConsumeRequest spends points from a member.
type ConsumeRequest struct {
	MemberID string
	Amount   int64
	// RefNo is the external consumption number. Resubmitting the same
	// (member, RefNo) with the same Amount returns the original result;
	// a different Amount is rejected with ErrConflict.
	RefNo string
}

// ConsumeResult is the outcome of a consumption. Allocations is ordered
// from the earliest-expiring batch to the latest.
type ConsumeResult struct {
	ConsumptionID string
	EntryID       string
	RefNo         string
	Amount        int64
	Allocations   []Allocation
	CreatedAt     time.Time
}

// ReverseRequest reverses (part of) a previous consumption.
type ReverseRequest struct {
	MemberID string
	// ConsumptionRefNo is the external consumption number (ConsumeRequest.RefNo).
	ConsumptionRefNo string
	Amount           int64
	// RefNo optionally deduplicates repeated reversal submissions; a
	// reused number with different content is rejected with ErrConflict.
	RefNo string
}

// ReverseResult is the outcome of a reversal. RestoredAvailable is the
// portion returned to spendable remainders; RestoredExpired is the portion
// that could not be restored because its batches had already expired.
type ReverseResult struct {
	ConsumptionID     string
	EntryID           string
	RefNo             string
	Amount            int64
	RestoredAvailable int64
	RestoredExpired   int64
	Allocations       []Allocation
	CreatedAt         time.Time
}

// ExpiryResult reports how many points one expiry advancement expired.
type ExpiryResult struct {
	Expired int64
	Entries int
}

// ledgerBatch is the mutable internal batch state.
type ledgerBatch struct {
	id        string
	issuedAt  time.Time
	expiresAt time.Time
	total     int64
	remaining int64
	consumed  int64
	expired   int64
}

func (b *ledgerBatch) view() BatchView {
	return BatchView{
		ID:        b.id,
		IssuedAt:  b.issuedAt,
		ExpiresAt: b.expiresAt,
		Total:     b.total,
		Remaining: b.remaining,
		Consumed:  b.consumed,
		Expired:   b.expired,
	}
}

// consumeAllocation is one slice of a stored consumption.
type consumeAllocation struct {
	batchID  string
	amount   int64
	reversed int64
}

// consumption is the stored record of a successful consumption.
type consumption struct {
	id        string
	entryID   string
	memberID  string
	refNo     string
	amount    int64
	reversed  int64
	createdAt time.Time
	parts     []*consumeAllocation
}

// account holds all state belonging to one member.
type account struct {
	memberID     string
	batches      []*ledgerBatch
	batchByID    map[string]*ledgerBatch
	consumptions []*consumption
	consumeByNo  map[string]*consumption
	entries      []*Entry
	issueByNo    map[string]*IssueResult
	revByNo      map[string]*ReverseResult
}

// Ledger is a concurrency-safe in-memory points ledger.
//
// A single mutex serializes every state transition. Each transition first
// applies lazy expiry against the Clock, so expiry never races with
// consumption or reversal: a point is either consumed or expired, never
// both, and a reversal either restores to available or records expired.
type Ledger struct {
	mu      sync.Mutex
	clock   Clock
	seq     uint64
	account map[string]*account
}

// NewLedger creates a Ledger. If clock is nil the system wall clock is used.
func NewLedger(clock Clock) *Ledger {
	if clock == nil {
		clock = SystemClock()
	}
	return &Ledger{clock: clock, account: make(map[string]*account)}
}

// Now returns the ledger's notion of current time.
func (l *Ledger) Now() time.Time { return l.clock.Now() }

func (l *Ledger) nextID(prefix string) string {
	l.seq++
	return prefix + "-" + strconv.FormatUint(l.seq, 10)
}

func (l *Ledger) getAccount(memberID string) *account {
	a := l.account[memberID]
	if a == nil {
		a = &account{
			memberID:    memberID,
			batchByID:   make(map[string]*ledgerBatch),
			consumeByNo: make(map[string]*consumption),
			issueByNo:   make(map[string]*IssueResult),
			revByNo:     make(map[string]*ReverseResult),
		}
		l.account[memberID] = a
	}
	return a
}

// applyExpiry expires the due remainders of every batch of a, appending one
// expiry entry per batch. Callers must hold l.mu.
func (l *Ledger) applyExpiry(a *account, now time.Time) int64 {
	var totalExpired int64
	for _, b := range a.batches {
		if b.remaining > 0 && !now.Before(b.expiresAt) {
			amt := b.remaining
			b.remaining = 0
			b.expired += amt
			totalExpired += amt
			a.entries = append(a.entries, &Entry{
				ID:        l.nextID("E"),
				MemberID:  a.memberID,
				Type:      EntryExpiry,
				Amount:    amt,
				BatchID:   b.id,
				CreatedAt: now,
			})
		}
	}
	return totalExpired
}

// Issue grants points into a new immutable batch.
func (l *Ledger) Issue(req IssueRequest) (*IssueResult, error) {
	if req.MemberID == "" {
		return nil, &argError{"member id is empty"}
	}
	if req.Amount <= 0 {
		return nil, &argError{"amount must be positive"}
	}
	if req.ExpiresAt.IsZero() {
		return nil, &argError{"expires at is zero"}
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.clock.Now()
	if !now.Before(req.ExpiresAt) {
		return nil, &argError{"expires at must be in the future"}
	}

	a := l.getAccount(req.MemberID)

	if req.RefNo != "" {
		if prev, ok := a.issueByNo[req.RefNo]; ok {
			if prev.Amount != req.Amount || !prev.ExpiresAt.Equal(req.ExpiresAt) {
				return nil, &conflictError{"issue ref_no " + req.RefNo + " reused with different content"}
			}
			return cloneIssue(prev), nil
		}
	}

	batch := &ledgerBatch{
		id:        l.nextID("B"),
		issuedAt:  now,
		expiresAt: req.ExpiresAt,
		total:     req.Amount,
		remaining: req.Amount,
	}
	a.batches = append(a.batches, batch)
	a.batchByID[batch.id] = batch

	entry := &Entry{
		ID:        l.nextID("E"),
		MemberID:  req.MemberID,
		Type:      EntryIssue,
		Amount:    req.Amount,
		BatchID:   batch.id,
		RefNo:     req.RefNo,
		CreatedAt: now,
	}
	a.entries = append(a.entries, entry)

	res := &IssueResult{
		BatchID:   batch.id,
		EntryID:   entry.ID,
		Amount:    req.Amount,
		ExpiresAt: req.ExpiresAt,
		IssuedAt:  now,
	}
	if req.RefNo != "" {
		a.issueByNo[req.RefNo] = res
	}
	return cloneIssue(res), nil
}

// Consume spends amount points from spendable batches, earliest-expiring
// first. The whole consumption is atomic: insufficient balance changes
// nothing.
func (l *Ledger) Consume(req ConsumeRequest) (*ConsumeResult, error) {
	if req.MemberID == "" {
		return nil, &argError{"member id is empty"}
	}
	if req.Amount <= 0 {
		return nil, &argError{"amount must be positive"}
	}
	if req.RefNo == "" {
		return nil, &argError{"ref_no is empty"}
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	a := l.getAccount(req.MemberID)
	now := l.clock.Now()
	l.applyExpiry(a, now)

	if prev, ok := a.consumeByNo[req.RefNo]; ok {
		if prev.amount != req.Amount {
			return nil, &conflictError{"consume ref_no " + req.RefNo + " reused with amount " +
				strconv.FormatInt(req.Amount, 10) + ", original " + strconv.FormatInt(prev.amount, 10)}
		}
		return consumeResultOf(prev), nil
	}

	// Order batches by expiry (earliest first), tie-broken by batch
	// sequence so the choice is deterministic.
	order := make([]*ledgerBatch, 0, len(a.batches))
	for _, b := range a.batches {
		if b.remaining > 0 && now.Before(b.expiresAt) {
			order = append(order, b)
		}
	}
	sort.SliceStable(order, func(i, j int) bool {
		if !order[i].expiresAt.Equal(order[j].expiresAt) {
			return order[i].expiresAt.Before(order[j].expiresAt)
		}
		return order[i].id < order[j].id
	})

	// Dry run first: commit only if every point can be covered.
	need := req.Amount
	type take struct {
		b      *ledgerBatch
		amount int64
	}
	takes := make([]take, 0, len(order))
	var available int64
	for _, b := range order {
		available += b.remaining
		if need == 0 {
			continue
		}
		t := b.remaining
		if t > need {
			t = need
		}
		takes = append(takes, take{b: b, amount: t})
		need -= t
	}
	if need > 0 {
		return nil, &insufficientError{requested: req.Amount, available: available}
	}

	c := &consumption{
		id:        l.nextID("C"),
		memberID:  req.MemberID,
		refNo:     req.RefNo,
		amount:    req.Amount,
		createdAt: now,
		parts:     make([]*consumeAllocation, 0, len(takes)),
	}
	allocs := make([]Allocation, 0, len(takes))
	for _, tk := range takes {
		tk.b.remaining -= tk.amount
		tk.b.consumed += tk.amount
		c.parts = append(c.parts, &consumeAllocation{batchID: tk.b.id, amount: tk.amount})
		allocs = append(allocs, Allocation{BatchID: tk.b.id, Amount: tk.amount})
	}

	entry := &Entry{
		ID:          l.nextID("E"),
		MemberID:    req.MemberID,
		Type:        EntryConsume,
		Amount:      req.Amount,
		RefNo:       req.RefNo,
		Allocations: allocs,
		CreatedAt:   now,
	}
	c.entryID = entry.ID
	a.entries = append(a.entries, entry)
	a.consumptions = append(a.consumptions, c)
	a.consumeByNo[req.RefNo] = c

	return consumeResultOf(c), nil
}

// Reverse undoes amount points of the consumption identified by
// ConsumptionRefNo, restoring the original per-batch split. Points whose
// batch has already expired are recorded as expired (traceable) and never
// return to the spendable balance.
//
// Partial reversals unwind the original allocations in their stored order
// (earliest-expiring batch first).
func (l *Ledger) Reverse(req ReverseRequest) (*ReverseResult, error) {
	if req.MemberID == "" {
		return nil, &argError{"member id is empty"}
	}
	if req.ConsumptionRefNo == "" {
		return nil, &argError{"consumption ref_no is empty"}
	}
	if req.Amount <= 0 {
		return nil, &argError{"amount must be positive"}
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	a := l.getAccount(req.MemberID)
	now := l.clock.Now()
	l.applyExpiry(a, now)

	if req.RefNo != "" {
		if prev, ok := a.revByNo[req.RefNo]; ok {
			if prev.Amount != req.Amount {
				return nil, &conflictError{"reverse ref_no " + req.RefNo + " reused with different amount"}
			}
			// The reversal must still refer to the same consumption.
			if c, ok := a.consumeByNo[req.ConsumptionRefNo]; !ok || c.id != prev.ConsumptionID {
				return nil, &conflictError{"reverse ref_no " + req.RefNo + " reused for a different consumption"}
			}
			return cloneReverse(prev), nil
		}
	}

	c, ok := a.consumeByNo[req.ConsumptionRefNo]
	if !ok {
		return nil, &notFoundError{"consumption ref_no " + req.ConsumptionRefNo + " not found"}
	}
	if req.Amount > c.amount-c.reversed {
		return nil, &conflictError{"reverse amount " + strconv.FormatInt(req.Amount, 10) +
			" exceeds reversible " + strconv.FormatInt(c.amount-c.reversed, 10) +
			" of consumption " + req.ConsumptionRefNo}
	}

	// Restore the original per-batch split, in the allocation order of
	// the stored consumption. Dry run before mutating.
	need := req.Amount
	type restore struct {
		part   *consumeAllocation
		batch  *ledgerBatch
		amount int64
	}
	restores := make([]restore, 0, len(c.parts))
	for _, p := range c.parts {
		reversible := p.amount - p.reversed
		if reversible <= 0 || need == 0 {
			continue
		}
		t := reversible
		if t > need {
			t = need
		}
		b := a.batchByID[p.batchID]
		restores = append(restores, restore{part: p, batch: b, amount: t})
		need -= t
	}
	if need > 0 {
		// Defensive: bookkeeping invariant says this cannot happen.
		return nil, &conflictError{"could not locate reversible allocations"}
	}

	var restoredAvailable, restoredExpired int64
	allocs := make([]Allocation, 0, len(restores))
	for _, r := range restores {
		r.part.reversed += r.amount
		r.batch.consumed -= r.amount
		if now.Before(r.batch.expiresAt) {
			// Batch still alive: points become spendable again.
			r.batch.remaining += r.amount
			restoredAvailable += r.amount
		} else {
			// Batch already expired: keep it traceable, never spendable.
			r.batch.expired += r.amount
			restoredExpired += r.amount
		}
		allocs = append(allocs, Allocation{BatchID: r.batch.id, Amount: r.amount})
	}
	c.reversed += req.Amount

	entry := &Entry{
		ID:                l.nextID("E"),
		MemberID:          req.MemberID,
		Type:              EntryReverse,
		Amount:            req.Amount,
		RefNo:             req.RefNo,
		ConsumptionRefNo:  req.ConsumptionRefNo,
		Allocations:       allocs,
		RestoredAvailable: restoredAvailable,
		RestoredExpired:   restoredExpired,
		CreatedAt:         now,
	}
	a.entries = append(a.entries, entry)

	res := &ReverseResult{
		ConsumptionID:     c.id,
		EntryID:           entry.ID,
		RefNo:             req.RefNo,
		Amount:            req.Amount,
		RestoredAvailable: restoredAvailable,
		RestoredExpired:   restoredExpired,
		Allocations:       allocs,
		CreatedAt:         now,
	}
	if req.RefNo != "" {
		a.revByNo[req.RefNo] = res
	}
	return cloneReverse(res), nil
}

// AdvanceExpiry explicitly expires every batch whose expiry instant has
// been reached according to the ledger Clock. Expiry is also applied
// lazily before every other operation, so calling this is optional; it is
// useful to materialize expiry entries at a known point in time.
func (l *Ledger) AdvanceExpiry(memberID string) (*ExpiryResult, error) {
	if memberID == "" {
		return nil, &argError{"member id is empty"}
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	a := l.getAccount(memberID)
	now := l.clock.Now()
	before := len(a.entries)
	expired := l.applyExpiry(a, now)
	return &ExpiryResult{Expired: expired, Entries: len(a.entries) - before}, nil
}

// Balance returns the member's point totals at the current clock time.
// Reading the balance first applies any due expiry.
func (l *Ledger) Balance(memberID string) (*Balance, error) {
	if memberID == "" {
		return nil, &argError{"member id is empty"}
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	a := l.getAccount(memberID)
	now := l.clock.Now()
	l.applyExpiry(a, now)
	return balanceOf(a), nil
}

func balanceOf(a *account) *Balance {
	b := &Balance{}
	for _, batch := range a.batches {
		b.Available += batch.remaining
		b.Consumed += batch.consumed
		b.Expired += batch.expired
		b.Issued += batch.total
	}
	return b
}

// Batches returns all batches of a member, ordered by expiry instant and
// then by issuance sequence. Due expiry is applied first.
func (l *Ledger) Batches(memberID string) ([]BatchView, error) {
	if memberID == "" {
		return nil, &argError{"member id is empty"}
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	a := l.getAccount(memberID)
	now := l.clock.Now()
	l.applyExpiry(a, now)

	views := make([]BatchView, 0, len(a.batches))
	for _, b := range a.batches {
		views = append(views, b.view())
	}
	sort.SliceStable(views, func(i, j int) bool {
		if !views[i].ExpiresAt.Equal(views[j].ExpiresAt) {
			return views[i].ExpiresAt.Before(views[j].ExpiresAt)
		}
		return views[i].ID < views[j].ID
	})
	return views, nil
}

// Entries returns the member's ledger entries in chronological order.
// Due expiry is applied first, so the returned trail is complete.
func (l *Ledger) Entries(memberID string) ([]Entry, error) {
	if memberID == "" {
		return nil, &argError{"member id is empty"}
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	a := l.getAccount(memberID)
	now := l.clock.Now()
	l.applyExpiry(a, now)

	out := make([]Entry, len(a.entries))
	for i, e := range a.entries {
		out[i] = cloneEntry(e)
	}
	return out, nil
}

func consumeResultOf(c *consumption) *ConsumeResult {
	allocs := make([]Allocation, len(c.parts))
	for i, p := range c.parts {
		allocs[i] = Allocation{BatchID: p.batchID, Amount: p.amount}
	}
	return &ConsumeResult{
		ConsumptionID: c.id,
		EntryID:       c.entryID,
		RefNo:         c.refNo,
		Amount:        c.amount,
		Allocations:   allocs,
		CreatedAt:     c.createdAt,
	}
}

func cloneIssue(r *IssueResult) *IssueResult {
	cp := *r
	return &cp
}

func cloneReverse(r *ReverseResult) *ReverseResult {
	cp := *r
	if r.Allocations != nil {
		cp.Allocations = append([]Allocation(nil), r.Allocations...)
	}
	return &cp
}

func cloneEntry(e *Entry) Entry {
	cp := *e
	if e.Allocations != nil {
		cp.Allocations = append([]Allocation(nil), e.Allocations...)
	}
	return cp
}
