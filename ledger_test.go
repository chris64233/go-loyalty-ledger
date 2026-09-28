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
