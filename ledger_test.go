package goloyaltyledger

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeClock 是测试用的可控时钟。
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(t time.Time) *fakeClock { return &fakeClock{now: t} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func (c *fakeClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func newTestLedger(t *testing.T, c *fakeClock) *Ledger {
	t.Helper()
	l, err := New("member-1", c.Now)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return l
}

func mustGrant(t *testing.T, l *Ledger, amount int64, expireAt time.Time) GrantReceipt {
	t.Helper()
	r, err := l.Grant(amount, expireAt, "")
	if err != nil {
		t.Fatalf("Grant(%d): %v", amount, err)
	}
	return r
}

func mustConsume(t *testing.T, l *Ledger, externalID string, amount int64) ConsumeReceipt {
	t.Helper()
	r, err := l.Consume(externalID, amount, "")
	if err != nil {
		t.Fatalf("Consume(%s, %d): %v", externalID, amount, err)
	}
	return r
}

func TestGrantCreatesImmutableBatch(t *testing.T) {
	c := newFakeClock(t0)
	l := newTestLedger(t, c)

	r := mustGrant(t, l, 100, t0.Add(24*time.Hour))
	if r.Amount != 100 || r.BatchID == "" || r.EntryID == "" {
		t.Fatalf("unexpected receipt: %+v", r)
	}
	b := l.Batch(r.BatchID)
	if b == nil {
		t.Fatal("batch not found")
	}
	if b.Granted != 100 || b.Remaining != 100 || b.Consumed != 0 || b.Expired != 0 {
		t.Fatalf("unexpected batch state: %+v", b)
	}
	if got := l.Available(); got != 100 {
		t.Fatalf("available = %d, want 100", got)
	}

	// 批次发放总量与失效时间不可修改：后续操作只改变 Remaining/Consumed/Expired。
	mustConsume(t, l, "c1", 40)
	b = l.Batch(r.BatchID)
	if b.Granted != 100 || !b.ExpireAt.Equal(t0.Add(24*time.Hour)) {
		t.Fatalf("immutable fields changed: %+v", b)
	}
	if b.Remaining != 60 || b.Consumed != 40 {
		t.Fatalf("unexpected batch after consume: %+v", b)
	}
}

func TestGrantRejectsInvalidArguments(t *testing.T) {
	c := newFakeClock(t0)
	l := newTestLedger(t, c)

	if _, err := l.Grant(0, t0.Add(time.Hour), ""); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("grant 0: %v", err)
	}
	if _, err := l.Grant(-5, t0.Add(time.Hour), ""); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("grant -5: %v", err)
	}
	if _, err := l.Grant(10, time.Time{}, ""); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("grant zero time: %v", err)
	}
	// 失效时间必须晚于当前时刻。
	if _, err := l.Grant(10, t0, ""); !errors.Is(err, ErrAlreadyExpired) {
		t.Fatalf("grant expiring now: %v", err)
	}
	if _, err := l.Grant(10, t0.Add(-time.Hour), ""); !errors.Is(err, ErrAlreadyExpired) {
		t.Fatalf("grant expired: %v", err)
	}
	if _, err := New("", nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty member: %v", err)
	}
}

func TestConsumeFEFOAcrossBatches(t *testing.T) {
	c := newFakeClock(t0)
	l := newTestLedger(t, c)

	// 故意先发放晚失效的批次，验证按失效时间而非发放顺序扣减。
	late := mustGrant(t, l, 50, t0.Add(72*time.Hour))
	early := mustGrant(t, l, 30, t0.Add(24*time.Hour))

	r := mustConsume(t, l, "c1", 60)
	if len(r.Splits) != 2 {
		t.Fatalf("splits = %+v, want 2 splits", r.Splits)
	}
	if r.Splits[0].BatchID != early.BatchID || r.Splits[0].Amount != 30 {
		t.Fatalf("first split should drain earliest-expiring batch: %+v", r.Splits[0])
	}
	if r.Splits[1].BatchID != late.BatchID || r.Splits[1].Amount != 30 {
		t.Fatalf("second split should take remainder from later batch: %+v", r.Splits[1])
	}
	if got := l.Available(); got != 20 {
		t.Fatalf("available = %d, want 20", got)
	}
}

func TestConsumeAtomicFailureLeavesLedgerUntouched(t *testing.T) {
	c := newFakeClock(t0)
	l := newTestLedger(t, c)

	mustGrant(t, l, 40, t0.Add(24*time.Hour))
	mustGrant(t, l, 30, t0.Add(48*time.Hour))

	_, err := l.Consume("c1", 71, "")
	if !errors.Is(err, ErrInsufficientPoints) {
		t.Fatalf("err = %v, want ErrInsufficientPoints", err)
	}
	if got := l.Available(); got != 70 {
		t.Fatalf("available = %d, want 70 (no partial deduction)", got)
	}
	for _, b := range l.Batches() {
		if b.Consumed != 0 {
			t.Fatalf("batch %s consumed = %d, want 0", b.ID, b.Consumed)
		}
	}
	// 失败的消费不应占用幂等键。
	if _, err := l.Consume("c1", 70, ""); err != nil {
		t.Fatalf("consume after failed attempt: %v", err)
	}
	if got := l.Available(); got != 0 {
		t.Fatalf("available = %d, want 0", got)
	}
}

func TestConsumeIdempotentReplay(t *testing.T) {
	c := newFakeClock(t0)
	l := newTestLedger(t, c)

	mustGrant(t, l, 100, t0.Add(24*time.Hour))
	first := mustConsume(t, l, "order-1", 30)

	// 相同内容重复提交：返回首次分摊结果，不重复扣分。
	again, err := l.Consume("order-1", 30, "")
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if again.ConsumeID != first.ConsumeID {
		t.Fatalf("replay returned different consume id: %s vs %s", again.ConsumeID, first.ConsumeID)
	}
	if len(again.Splits) != len(first.Splits) || again.Splits[0] != first.Splits[0] {
		t.Fatalf("replay splits differ: %+v vs %+v", again.Splits, first.Splits)
	}
	if got := l.Available(); got != 70 {
		t.Fatalf("available = %d, want 70", got)
	}

	// 同号不同内容：报错且不扣分。
	if _, err := l.Consume("order-1", 31, ""); !errors.Is(err, ErrDuplicateID) {
		t.Fatalf("conflict err = %v, want ErrDuplicateID", err)
	}
	if got := l.Available(); got != 70 {
		t.Fatalf("available = %d, want 70", got)
	}
}

func TestExpireAdvancesBatches(t *testing.T) {
	c := newFakeClock(t0)
	l := newTestLedger(t, c)

	expiring := mustGrant(t, l, 50, t0.Add(time.Hour))
	staying := mustGrant(t, l, 20, t0.Add(48*time.Hour))

	c.Advance(time.Hour) // 恰好到达失效时刻即视为过期
	r, err := l.Expire()
	if err != nil {
		t.Fatalf("Expire: %v", err)
	}
	if len(r.Batches) != 1 || r.Batches[0].BatchID != expiring.BatchID || r.Batches[0].Amount != 50 {
		t.Fatalf("expire receipt = %+v", r)
	}
	if got := l.Available(); got != 20 {
		t.Fatalf("available = %d, want 20", got)
	}
	b := l.Batch(expiring.BatchID)
	if b.Remaining != 0 || b.Expired != 50 || !b.ExpiredSwept {
		t.Fatalf("expired batch state: %+v", b)
	}
	if b := l.Batch(staying.BatchID); b.Remaining != 20 || b.Expired != 0 {
		t.Fatalf("unaffected batch state: %+v", b)
	}

	// 幂等推进：再次 Expire 不再产生分录。
	r2, err := l.Expire()
	if err != nil {
		t.Fatalf("Expire again: %v", err)
	}
	if r2.EntryID != "" || len(r2.Batches) != 0 {
		t.Fatalf("second expire should be a no-op: %+v", r2)
	}

	// 过期是单向的：时钟回拨也不能复活。
	c.Set(t0)
	if got := l.Available(); got != 20 {
		t.Fatalf("available after clock rewind = %d, want 20", got)
	}
}

func TestConsumeNeverTouchesExpiredPoints(t *testing.T) {
	c := newFakeClock(t0)
	l := newTestLedger(t, c)

	mustGrant(t, l, 50, t0.Add(time.Hour))
	mustGrant(t, l, 10, t0.Add(48*time.Hour))

	c.Advance(2 * time.Hour) // 第一批次已过期，但尚未显式 Expire
	// 消费前内部先推进过期：只能用到未过期的 10。
	r := mustConsume(t, l, "c1", 10)
	if len(r.Splits) != 1 || r.Splits[0].Amount != 10 {
		t.Fatalf("splits = %+v", r.Splits)
	}
	if _, err := l.Consume("c2", 1, ""); !errors.Is(err, ErrInsufficientPoints) {
		t.Fatalf("err = %v, want ErrInsufficientPoints", err)
	}
}

func TestRevokeRestoresPerOriginalSplits(t *testing.T) {
	c := newFakeClock(t0)
	l := newTestLedger(t, c)

	b1 := mustGrant(t, l, 30, t0.Add(24*time.Hour))
	b2 := mustGrant(t, l, 50, t0.Add(72*time.Hour))
	cons := mustConsume(t, l, "c1", 60) // 30 + 30

	// 撤销 40：按原分摊顺序恢复 30 -> b1、10 -> b2。
	r, err := l.Revoke(cons.ConsumeID, 40, "", "")
	if err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if r.Restored != 40 || r.ExpiredAmount != 0 {
		t.Fatalf("receipt = %+v", r)
	}
	if len(r.Splits) != 2 || r.Splits[0].BatchID != b1.BatchID || r.Splits[0].Amount != 30 ||
		r.Splits[1].BatchID != b2.BatchID || r.Splits[1].Amount != 10 {
		t.Fatalf("revoke splits = %+v", r.Splits)
	}
	if got := l.Available(); got != 60 {
		t.Fatalf("available = %d, want 60", got)
	}
	if b := l.Batch(b1.BatchID); b.Remaining != 30 || b.Consumed != 0 {
		t.Fatalf("b1 = %+v", b)
	}
	if b := l.Batch(b2.BatchID); b.Remaining != 30 || b.Consumed != 20 {
		t.Fatalf("b2 = %+v", b)
	}

	// 撤销全部剩余（amount=0）。
	r, err = l.Revoke(cons.ConsumeID, 0, "", "")
	if err != nil {
		t.Fatalf("Revoke rest: %v", err)
	}
	if r.Amount != 20 || r.Restored != 20 {
		t.Fatalf("receipt = %+v", r)
	}
	if got := l.Available(); got != 80 {
		t.Fatalf("available = %d, want 80", got)
	}

	// 已全部撤销：再撤报错。
	if _, err := l.Revoke(cons.ConsumeID, 1, "", ""); !errors.Is(err, ErrRevokeExceedsConsume) {
		t.Fatalf("err = %v, want ErrRevokeExceedsConsume", err)
	}
	if _, err := l.Revoke(cons.ConsumeID, 0, "", ""); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("err = %v, want ErrInvalidArgument", err)
	}
}

func TestRevokeCumulativeCap(t *testing.T) {
	c := newFakeClock(t0)
	l := newTestLedger(t, c)

	mustGrant(t, l, 100, t0.Add(24*time.Hour))
	cons := mustConsume(t, l, "c1", 50)

	if _, err := l.Revoke(cons.ConsumeID, 30, "", ""); err != nil {
		t.Fatalf("revoke 30: %v", err)
	}
	// 累计撤销 30 + 30 > 50，必须拒绝。
	if _, err := l.Revoke(cons.ConsumeID, 30, "", ""); !errors.Is(err, ErrRevokeExceedsConsume) {
		t.Fatalf("err = %v, want ErrRevokeExceedsConsume", err)
	}
	if got := l.Available(); got != 80 {
		t.Fatalf("available = %d, want 80", got)
	}
	if _, err := l.Revoke("consume-999", 1, "", ""); !errors.Is(err, ErrConsumeNotFound) {
		t.Fatalf("err = %v, want ErrConsumeNotFound", err)
	}
}

func TestRevokeAfterExpiryKeepsTraceButNotSpendable(t *testing.T) {
	c := newFakeClock(t0)
	l := newTestLedger(t, c)

	b1 := mustGrant(t, l, 40, t0.Add(time.Hour))
	b2 := mustGrant(t, l, 60, t0.Add(72*time.Hour))
	cons := mustConsume(t, l, "c1", 70) // 40 (b1) + 30 (b2)

	// b1 到期后被推进过期；其被消费的 40 仍占用 Consumed。
	c.Advance(2 * time.Hour)
	if _, err := l.Expire(); err != nil {
		t.Fatalf("Expire: %v", err)
	}

	// 撤销全部 70：b1 的 40 已过期只能留痕，b2 的 30 恢复可用。
	r, err := l.Revoke(cons.ConsumeID, 0, "", "")
	if err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if r.Restored != 30 || r.ExpiredAmount != 40 {
		t.Fatalf("receipt = %+v", r)
	}
	if got := l.Available(); got != 60 {
		t.Fatalf("available = %d, want 60 (expired part must not come back)", got)
	}
	b := l.Batch(b1.BatchID)
	if b.Expired != 40 || b.Consumed != 0 || b.Remaining != 0 {
		t.Fatalf("b1 = %+v, want expired trace only", b)
	}
	if b := l.Batch(b2.BatchID); b.Remaining != 60 || b.Consumed != 0 {
		t.Fatalf("b2 = %+v", b)
	}

	// 留痕可查询：撤销明细中能看到过期部分。
	found := false
	for _, e := range l.Entries() {
		if e.Type == EntryRevoke {
			found = true
			if e.Revoke.ExpiredAmount != 40 || e.Revoke.Restored != 30 {
				t.Fatalf("revoke entry = %+v", e.Revoke)
			}
		}
	}
	if !found {
		t.Fatal("revoke entry not found")
	}
}

func TestRevokeIdempotentReplay(t *testing.T) {
	c := newFakeClock(t0)
	l := newTestLedger(t, c)

	mustGrant(t, l, 100, t0.Add(24*time.Hour))
	cons := mustConsume(t, l, "c1", 50)

	first, err := l.Revoke(cons.ConsumeID, 20, "rv-1", "")
	if err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	again, err := l.Revoke(cons.ConsumeID, 20, "rv-1", "")
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if again.EntryID != first.EntryID {
		t.Fatalf("replay entry id = %s, want %s", again.EntryID, first.EntryID)
	}
	if got := l.Available(); got != 70 {
		t.Fatalf("available = %d, want 70", got)
	}
	// 同键不同内容：报错。
	if _, err := l.Revoke(cons.ConsumeID, 21, "rv-1", ""); !errors.Is(err, ErrDuplicateID) {
		t.Fatalf("err = %v, want ErrDuplicateID", err)
	}
}

func TestExpiryBoundaryExactlyAtExpireAt(t *testing.T) {
	c := newFakeClock(t0)
	l := newTestLedger(t, c)

	mustGrant(t, l, 10, t0.Add(time.Hour))

	c.Set(t0.Add(time.Hour).Add(-time.Nanosecond))
	if got := l.Available(); got != 10 {
		t.Fatalf("just before expiry: available = %d, want 10", got)
	}
	c.Set(t0.Add(time.Hour))
	if got := l.Available(); got != 0 {
		t.Fatalf("at expiry: available = %d, want 0", got)
	}
}

func TestEntriesAndBatchQueries(t *testing.T) {
	c := newFakeClock(t0)
	l := newTestLedger(t, c)

	mustGrant(t, l, 100, t0.Add(24*time.Hour))
	mustConsume(t, l, "c1", 30)
	c.Advance(25 * time.Hour)
	if _, err := l.Expire(); err != nil {
		t.Fatalf("Expire: %v", err)
	}

	entries := l.Entries()
	if len(entries) != 3 {
		t.Fatalf("entries = %d, want 3 (grant/consume/expire)", len(entries))
	}
	var total int64
	for _, e := range entries {
		switch e.Type {
		case EntryGrant:
			total += e.Amount
		case EntryConsume, EntryExpire:
			total -= e.Amount
		}
	}
	if total != 0 {
		t.Fatalf("entry amounts do not net to 0, got %d", total)
	}

	// 查询返回副本：修改返回值不影响账本。
	entries[0].Amount = -999
	if l.Entries()[0].Amount != 100 {
		t.Fatal("entries query must return copies")
	}
	b := l.Batches()[0]
	b.Remaining = -1
	if l.Batches()[0].Remaining != 0 {
		t.Fatal("batches query must return copies")
	}
}

func TestConcurrentConsumeNeverOverspends(t *testing.T) {
	c := newFakeClock(t0)
	l := newTestLedger(t, c)
	mustGrant(t, l, 1000, t0.Add(24*time.Hour))

	const workers = 16
	const perWorker = 20
	var wg sync.WaitGroup
	var mu sync.Mutex
	succeeded := make(map[string]ConsumeReceipt)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				id := "c-" + itoa(int64(w*perWorker+i))
				r, err := l.Consume(id, 10, "")
				if err == nil {
					mu.Lock()
					succeeded[id] = r
					mu.Unlock()
				} else if !errors.Is(err, ErrInsufficientPoints) {
					t.Errorf("unexpected error: %v", err)
				}
			}
		}(w)
	}
	wg.Wait()

	var spent int64
	for _, r := range succeeded {
		spent += r.Amount
	}
	if spent > 1000 {
		t.Fatalf("overspent: %d > 1000", spent)
	}
	if got := l.Available(); got != 1000-spent {
		t.Fatalf("available = %d, want %d", got, 1000-spent)
	}
	if got := l.Available(); got < 0 {
		t.Fatalf("negative balance: %d", got)
	}
}

func TestConcurrentConsumeExpireRevokeConsistent(t *testing.T) {
	c := newFakeClock(t0)
	l := newTestLedger(t, c)
	mustGrant(t, l, 500, t0.Add(time.Hour))
	mustGrant(t, l, 500, t0.Add(48*time.Hour))

	cons := mustConsume(t, l, "c1", 200)

	var wg sync.WaitGroup
	// 并发：消费、撤销、过期推进、查询同时进行。
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = l.Consume("cc-"+itoa(int64(i)), 50, "")
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = l.Revoke(cons.ConsumeID, 100, "", "")
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Advance(2 * time.Hour)
		_, _ = l.Expire()
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			_ = l.Available()
			_ = l.Entries()
			_ = l.Batches()
		}
	}()
	wg.Wait()

	// 不变量：每个批次 Granted == Remaining + Consumed + Expired，且各项非负。
	for _, b := range l.Batches() {
		if b.Remaining < 0 || b.Consumed < 0 || b.Expired < 0 {
			t.Fatalf("negative component in batch %+v", b)
		}
		if b.Granted != b.Remaining+b.Consumed+b.Expired {
			t.Fatalf("invariant violated in batch %+v", b)
		}
	}
	if got := l.Available(); got < 0 {
		t.Fatalf("negative balance: %d", got)
	}
}

func TestGrantWithIDRecordsSourceAndValidWindow(t *testing.T) {
	c := newFakeClock(t0)
	l := newTestLedger(t, c)

	expireAt := t0.Add(48 * time.Hour)
	r, err := l.GrantWithID("grant-1", "开卡礼", 100, expireAt, "")
	if err != nil {
		t.Fatalf("GrantWithID: %v", err)
	}
	b := l.Batch(r.BatchID)
	if b.Source != "开卡礼" {
		t.Fatalf("batch source = %q, want 开卡礼", b.Source)
	}
	// 可用时间段：发放时刻至失效时刻。
	if !b.CreatedAt.Equal(t0) || !b.ExpireAt.Equal(expireAt) {
		t.Fatalf("valid window = %v..%v", b.CreatedAt, b.ExpireAt)
	}
	if b.AvailableAt(t0.Add(47*time.Hour)) != 100 || b.AvailableAt(expireAt) != 0 {
		t.Fatal("availability window boundary wrong")
	}
}

func TestGrantWithIDIdempotentReplay(t *testing.T) {
	c := newFakeClock(t0)
	l := newTestLedger(t, c)

	first, err := l.GrantWithID("grant-1", "开卡礼", 100, t0.Add(24*time.Hour), "")
	if err != nil {
		t.Fatalf("GrantWithID: %v", err)
	}
	// 时钟推进到原批次已过期之后：重放仍须返回首次结果，而不是报已过期。
	c.Advance(48 * time.Hour)
	again, err := l.GrantWithID("grant-1", "开卡礼", 100, t0.Add(24*time.Hour), "")
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if again.EntryID != first.EntryID || again.BatchID != first.BatchID {
		t.Fatalf("replay = %+v, want first %+v", again, first)
	}
	// 不重复入账：分录与余额都只算一次。
	if got := l.RecomputedBalance(); got != 100 {
		t.Fatalf("recomputed balance = %d, want 100", got)
	}
	grants := 0
	for _, e := range l.Entries() {
		if e.Type == EntryGrant {
			grants++
		}
	}
	if grants != 1 {
		t.Fatalf("grant entries = %d, want 1", grants)
	}
}

func TestGrantWithIDConflict(t *testing.T) {
	c := newFakeClock(t0)
	l := newTestLedger(t, c)

	if _, err := l.GrantWithID("grant-1", "开卡礼", 100, t0.Add(24*time.Hour), ""); err != nil {
		t.Fatalf("GrantWithID: %v", err)
	}
	// 金额变化 -> 冲突。
	if _, err := l.GrantWithID("grant-1", "开卡礼", 200, t0.Add(24*time.Hour), ""); !errors.Is(err, ErrDuplicateID) {
		t.Fatalf("amount change err = %v, want ErrDuplicateID", err)
	}
	// 来源变化 -> 冲突。
	if _, err := l.GrantWithID("grant-1", "消费返积分", 100, t0.Add(24*time.Hour), ""); !errors.Is(err, ErrDuplicateID) {
		t.Fatalf("source change err = %v, want ErrDuplicateID", err)
	}
	// 失效时间变化 -> 冲突。
	if _, err := l.GrantWithID("grant-1", "开卡礼", 100, t0.Add(48*time.Hour), ""); !errors.Is(err, ErrDuplicateID) {
		t.Fatalf("expireAt change err = %v, want ErrDuplicateID", err)
	}
	// 冲突不产生任何入账。
	if got := l.RecomputedBalance(); got != 100 {
		t.Fatalf("recomputed balance = %d, want 100", got)
	}
	// 空流水号非法。
	if _, err := l.GrantWithID("", "s", 1, t0.Add(time.Hour), ""); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty id err = %v, want ErrInvalidArgument", err)
	}
}

func TestExpireWithIDRepeatedScanReturnsOriginal(t *testing.T) {
	c := newFakeClock(t0)
	l := newTestLedger(t, c)

	b1 := mustGrant(t, l, 50, t0.Add(time.Hour))
	b2 := mustGrant(t, l, 70, t0.Add(48*time.Hour))

	c.Advance(2 * time.Hour) // 只有 b1 到期
	first, err := l.ExpireWithID("scan-1")
	if err != nil {
		t.Fatalf("ExpireWithID: %v", err)
	}
	if len(first.Batches) != 1 || first.Batches[0].BatchID != b1.BatchID || first.Batches[0].Amount != 50 {
		t.Fatalf("first scan = %+v", first)
	}

	c.Advance(72 * time.Hour) // b2 也到期了
	// 重复扫描同一流水号：返回首次结果，不会把 b2 并入、也不会重复核销 b1。
	again, err := l.ExpireWithID("scan-1")
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if again.EntryID != first.EntryID || len(again.Batches) != 1 || again.Batches[0].Amount != 50 {
		t.Fatalf("replay = %+v, want original %+v", again, first)
	}
	if b := l.Batch(b2.BatchID); b.Remaining != 70 || b.Expired != 0 {
		t.Fatalf("b2 must not be swept by replayed scan: %+v", b)
	}
	if b := l.Batch(b1.BatchID); b.Expired != 50 {
		t.Fatalf("b1 double swept: %+v", b)
	}
	// 新的扫描流水号才推进 b2。
	second, err := l.ExpireWithID("scan-2")
	if err != nil {
		t.Fatalf("ExpireWithID scan-2: %v", err)
	}
	if len(second.Batches) != 1 || second.Batches[0].BatchID != b2.BatchID || second.Batches[0].Amount != 70 {
		t.Fatalf("second scan = %+v", second)
	}
	if got := l.RecomputedBalance(); got != 0 {
		t.Fatalf("recomputed balance = %d, want 0", got)
	}
	// 空扫描号非法。
	if _, err := l.ExpireWithID(""); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty scan id err = %v, want ErrInvalidArgument", err)
	}
}

func TestPartialConsumeThenExpireSplitsBatch(t *testing.T) {
	c := newFakeClock(t0)
	l := newTestLedger(t, c)

	g := mustGrant(t, l, 100, t0.Add(time.Hour))
	cons := mustConsume(t, l, "c1", 30) // 用掉 30，剩 70

	c.Advance(2 * time.Hour)
	r, err := l.Expire()
	if err != nil {
		t.Fatalf("Expire: %v", err)
	}
	// 只核销仍未使用的 70，不能把整笔批次清零。
	if len(r.Batches) != 1 || r.Batches[0].Amount != 70 {
		t.Fatalf("expire receipt = %+v, want only unused 70", r)
	}
	b := l.Batch(g.BatchID)
	if b.Granted != 100 || b.Remaining != 0 || b.Consumed != 30 || b.Expired != 70 {
		t.Fatalf("batch split = %+v, want granted100/consumed30/expired70", b)
	}
	// 已消费部分仍可按原记录撤销路径处理（此处批次已过期，只留痕）。
	rv, err := l.Revoke(cons.ConsumeID, 0, "", "")
	if err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if rv.Restored != 0 || rv.ExpiredAmount != 30 {
		t.Fatalf("revoke = %+v, want all 30 traced as expired", rv)
	}
	b = l.Batch(g.BatchID)
	if b.Consumed != 0 || b.Expired != 100 {
		t.Fatalf("batch after revoke = %+v, want expired 100 total", b)
	}
}

func TestRecomputedBalanceMatchesEntriesAndBatches(t *testing.T) {
	c := newFakeClock(t0)
	l := newTestLedger(t, c)

	mustGrant(t, l, 100, t0.Add(time.Hour))
	if _, err := l.GrantWithID("g2", "消费返积分", 50, t0.Add(72*time.Hour), ""); err != nil {
		t.Fatalf("GrantWithID: %v", err)
	}
	cons := mustConsume(t, l, "c1", 80)
	if _, err := l.Revoke(cons.ConsumeID, 20, "", ""); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	c.Advance(2 * time.Hour)
	if _, err := l.Expire(); err != nil {
		t.Fatalf("Expire: %v", err)
	}

	// 余额必须能由账本重算：重算值 == 各批次 Remaining 之和 == Available。
	var sumRemaining int64
	for _, b := range l.Batches() {
		sumRemaining += b.Remaining
	}
	if got := l.RecomputedBalance(); got != sumRemaining {
		t.Fatalf("recomputed = %d, sum of batch remaining = %d", got, sumRemaining)
	}
	if got, want := l.RecomputedBalance(), l.Available(); got != want {
		t.Fatalf("recomputed = %d, available = %d (post-sweep must match)", got, want)
	}

	// 手工按分录流水独立重算，交叉验证。
	var fold int64
	for _, e := range l.Entries() {
		switch e.Type {
		case EntryGrant:
			fold += e.Amount
		case EntryConsume:
			fold -= e.Amount
		case EntryRevoke:
			fold += e.Revoke.Restored
		case EntryExpire:
			fold -= e.Amount
		}
	}
	if fold != l.RecomputedBalance() {
		t.Fatalf("manual fold = %d, recomputed = %d", fold, l.RecomputedBalance())
	}
}

func TestConsumeExactlyAtExpiryBoundary(t *testing.T) {
	c := newFakeClock(t0)
	l := newTestLedger(t, c)

	mustGrant(t, l, 40, t0.Add(time.Hour))

	// 失效时刻前一纳秒：仍可消费。
	c.Set(t0.Add(time.Hour).Add(-time.Nanosecond))
	mustConsume(t, l, "c1", 10)
	// 到达失效时刻：消费提交前会沿同一路径推进过期，
	// 剩余 30 被核销（只核销未使用部分），消费因余额不足失败。
	c.Set(t0.Add(time.Hour))
	if _, err := l.Consume("c2", 1, ""); !errors.Is(err, ErrInsufficientPoints) {
		t.Fatalf("err = %v, want ErrInsufficientPoints", err)
	}
	var b *Batch
	for _, x := range l.Batches() {
		b = x
	}
	if b.Remaining != 0 || b.Consumed != 10 || b.Expired != 30 {
		t.Fatalf("batch = %+v, want consumed 10 / expired 30 (only the unused part)", b)
	}
	// 已推进过的批次不会被重复扫描核销。
	r, err := l.Expire()
	if err != nil {
		t.Fatalf("Expire: %v", err)
	}
	if r.EntryID != "" || len(r.Batches) != 0 {
		t.Fatalf("re-scan must be a no-op: %+v", r)
	}
}

func TestConcurrentDuplicateConsumeAndScan(t *testing.T) {
	c := newFakeClock(t0)
	l := newTestLedger(t, c)
	mustGrant(t, l, 500, t0.Add(time.Hour))

	const workers = 16
	var wg sync.WaitGroup
	// 同一消费号、同一内容并发提交：只许扣一次，且都拿到同一消费记录；
	// 若扫描先把时钟推进过失效时刻，消费可能因余额不足失败，同样合法。
	ids := make([]string, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r, err := l.Consume("order-dup", 50, "")
			if err != nil {
				if !errors.Is(err, ErrInsufficientPoints) {
					t.Errorf("duplicate consume: %v", err)
				}
				return
			}
			ids[w] = r.ConsumeID
		}(w)
	}
	// 同一扫描号并发提交：只产生一条过期分录。
	scanEntries := make([]string, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c.Advance(2 * time.Hour)
			r, err := l.ExpireWithID("scan-dup")
			if err != nil {
				t.Errorf("duplicate scan: %v", err)
				return
			}
			scanEntries[w] = r.EntryID
		}(w)
	}
	wg.Wait()

	var firstID string
	for _, id := range ids {
		if id == "" {
			continue
		}
		if firstID == "" {
			firstID = id
		} else if id != firstID {
			t.Fatalf("duplicate consume created multiple records: %v", ids)
		}
	}
	var firstScan string
	for _, id := range scanEntries {
		if id == "" {
			continue // 扫描与消费竞速，先到者可能扫到零批次
		}
		if firstScan == "" {
			firstScan = id
		} else if id != firstScan {
			t.Fatalf("duplicate scan created multiple entries: %v", scanEntries)
		}
	}
	// 无论消费与扫描如何交错，最终 500 积分要么被消费要么被过期核销，
	// 余额必须能由账本重算为 0，且批次不变量成立。
	if got := l.RecomputedBalance(); got != 0 {
		t.Fatalf("recomputed balance = %d, want 0", got)
	}
	for _, b := range l.Batches() {
		if b.Granted != b.Remaining+b.Consumed+b.Expired {
			t.Fatalf("invariant violated in batch %+v", b)
		}
	}
}
