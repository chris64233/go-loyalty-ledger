// Package goloyaltyledger 实现带有效期批次的会员积分账本。
//
// 核心约束：
//
//   - 积分发放后形成不可修改的批次（总量与失效时间不变），消费按“最早失效优先”
//     （FEFO）跨批次分摊；任一可用量不足则整笔消费失败、账本不变。
//   - 所有写操作在单账本互斥锁内串行提交，配合统一的 Clock 时间源，
//     保证过期推进与消费、撤销并发发生时，每个批次的数量只沿合法方向变化。
//   - 入账与过期扫描均可携带业务流水号：重复提交返回首次结果，
//     内容不一致返回 ErrDuplicateID；过期只核销批次中仍未使用的部分。
//   - 消费支持外部消费号幂等：重复提交相同内容返回首次的分摊结果。
//   - 撤销严格按原消费记录逐批恢复；撤销时对应批次已过期的部分只留痕、
//     不重新变成可消费积分；累计撤销不得超过原消费。
package goloyaltyledger

import (
	"sync"
	"time"
)

// Ledger 是单个会员的积分账本。零值不可用，请使用 New 构造。
type Ledger struct {
	mu       sync.Mutex
	memberID string
	clock    Clock

	seq       uint64
	batches   []*Batch
	batchByID map[string]*Batch
	entries   []*Entry
	entryByID map[string]*Entry

	// consumes 保存全部已提交的消费记录（含每次撤销后的剩余可撤销量）。
	consumes map[string]*consumeRecord
	// consumeByExternal 外部消费号 -> 消费 ID。
	consumeByExternal map[string]string
	// revokeByExternal 撤销自身的幂等键 -> 分录 ID。
	revokeByExternal map[string]string
	// grantByExternal 入账业务流水号 -> 发放分录 ID。
	grantByExternal map[string]string
	// expireByScanID 过期扫描业务流水号 -> 过期分录 ID。
	expireByScanID map[string]string
}

// consumeSplitState 是某批次在一条消费记录上的分摊状态。
type consumeSplitState struct {
	batchID   string
	amount    int64 // 原分摊数量（不变）
	unrevoked int64 // 尚未撤销的数量
}

// consumeRecord 是一条消费的内部记录，撤销必须据此逐批恢复。
type consumeRecord struct {
	id         string
	externalID string
	amount     int64
	remark     string
	splits     []consumeSplitState
}

// New 创建会员 memberID 的账本。clock 为 nil 时使用 time.Now 作为时间源。
func New(memberID string, clock Clock) (*Ledger, error) {
	if memberID == "" {
		return nil, invalidArgumentf("member id must not be empty")
	}
	if clock == nil {
		clock = time.Now
	}
	return &Ledger{
		memberID:          memberID,
		clock:             clock,
		batchByID:         make(map[string]*Batch),
		entryByID:         make(map[string]*Entry),
		consumes:          make(map[string]*consumeRecord),
		consumeByExternal: make(map[string]string),
		revokeByExternal:  make(map[string]string),
		grantByExternal:   make(map[string]string),
		expireByScanID:    make(map[string]string),
	}, nil
}

// MemberID 返回账本所属会员 ID。
func (l *Ledger) MemberID() string { return l.memberID }

// nextID 在持锁状态下生成全账本唯一 ID。
func (l *Ledger) nextID(kind string) string {
	l.seq++
	return kind + "-" + itoa(int64(l.seq))
}

// Grant 发放 amount 积分，形成一个失效时间为 expireAt 的不可修改批次。
// 失效时间必须晚于统一时钟的当前时刻。
func (l *Ledger) Grant(amount int64, expireAt time.Time, remark string) (GrantReceipt, error) {
	return l.grant("", "", amount, expireAt, remark)
}

// GrantWithID 带业务流水号的入账：同一 externalID 重复提交相同内容
// （金额、来源、失效时间、备注一致）时直接返回首次入账结果，不重复入账；
// 内容不一致时返回 ErrDuplicateID。source 记录积分来源并随批次保存。
//
// 账本是单会员维度，“用户”维度天然固定：同一流水号落到不同会员账本
// 不会产生交叉；本方法内的冲突检测覆盖金额、来源与失效时间。
func (l *Ledger) GrantWithID(externalID, source string, amount int64, expireAt time.Time, remark string) (GrantReceipt, error) {
	if externalID == "" {
		return GrantReceipt{}, invalidArgumentf("external grant id must not be empty")
	}
	return l.grant(externalID, source, amount, expireAt, remark)
}

func (l *Ledger) grant(externalID, source string, amount int64, expireAt time.Time, remark string) (GrantReceipt, error) {
	if amount <= 0 {
		return GrantReceipt{}, invalidArgumentf("grant amount must be positive, got %d", amount)
	}
	if expireAt.IsZero() {
		return GrantReceipt{}, invalidArgumentf("expire at must not be zero time")
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// 幂等：同号相同内容返回首次结果（即使批次此后已过期）；
	// 同号不同内容视为调用方错误。该判断必须先于失效时间校验，
	// 否则时钟推进后重放会被误判为 ErrAlreadyExpired。
	if externalID != "" {
		if eid, ok := l.grantByExternal[externalID]; ok {
			e := l.entryByID[eid]
			d := e.Grant
			b := l.batchByID[d.BatchID]
			if d.Amount != amount || d.Source != source || !b.ExpireAt.Equal(expireAt) || e.Remark != remark {
				return GrantReceipt{}, grantConflict(externalID, e, b.ExpireAt, amount, source, expireAt)
			}
			return GrantReceipt{
				EntryID:    e.ID,
				BatchID:    d.BatchID,
				Amount:     d.Amount,
				ExternalID: externalID,
				Source:     d.Source,
			}, nil
		}
	}

	now := l.clock()
	if !now.Before(expireAt) {
		return GrantReceipt{}, ErrAlreadyExpired
	}

	batchID := l.nextID("batch")
	entryID := l.nextID("entry")
	b := &Batch{
		ID:        batchID,
		MemberID:  l.memberID,
		Source:    source,
		Granted:   amount,
		Remaining: amount,
		ExpireAt:  expireAt,
		CreatedAt: now,
	}
	l.batches = append(l.batches, b)
	l.batchByID[batchID] = b

	if externalID != "" {
		l.grantByExternal[externalID] = entryID
	}
	l.appendEntry(&Entry{
		ID:         entryID,
		MemberID:   l.memberID,
		Type:       EntryGrant,
		Amount:     amount,
		OccurredAt: now,
		Remark:     remark,
		Grant:      &GrantDetail{ExternalID: externalID, BatchID: batchID, Amount: amount, Source: source},
	})

	return GrantReceipt{EntryID: entryID, BatchID: batchID, Amount: amount, ExternalID: externalID, Source: source}, nil
}

// Consume 消费 amount 积分，按最早失效优先在批次间分摊，整笔原子成功或失败。
// externalID 为外部消费号（必填），同一消费号重复提交相同内容时，
// 直接返回首次的分摊结果，不重复扣分。
func (l *Ledger) Consume(externalID string, amount int64, remark string) (ConsumeReceipt, error) {
	if externalID == "" {
		return ConsumeReceipt{}, invalidArgumentf("external consume id must not be empty")
	}
	if amount <= 0 {
		return ConsumeReceipt{}, invalidArgumentf("consume amount must be positive, got %d", amount)
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// 幂等：同号相同内容返回首次结果；同号不同内容视为调用方错误。
	if cid, ok := l.consumeByExternal[externalID]; ok {
		rec := l.consumes[cid]
		if rec.amount != amount || rec.remark != remark {
			return ConsumeReceipt{}, idempotencyConflict(externalID, rec.amount, amount)
		}
		return receiptOfConsume(rec), nil
	}

	now := l.clock()

	// 先把已到期批次沿合法方向推进为过期，再做消费：
	// 同一时刻同一批次不可能既被消费又被过期。
	if err := l.sweepExpiredLocked(now); err != nil {
		return ConsumeReceipt{}, err
	}

	// FEFO 选批：失效时间升序，同失效时间按发放先后（批次序号）。
	plan, avail := l.planConsumeLocked(amount, now)
	if avail < amount {
		return ConsumeReceipt{}, insufficientError(amount, avail)
	}

	consumeID := l.nextID("consume")
	entryID := l.nextID("entry")
	splits := make([]consumeSplitState, 0, len(plan))
	detailSplits := make([]ConsumeSplit, 0, len(plan))
	for _, s := range plan {
		b := l.batchByID[s.BatchID]
		b.Remaining -= s.Amount
		b.Consumed += s.Amount
		splits = append(splits, consumeSplitState{batchID: s.BatchID, amount: s.Amount, unrevoked: s.Amount})
		detailSplits = append(detailSplits, s)
	}

	rec := &consumeRecord{
		id:         consumeID,
		externalID: externalID,
		amount:     amount,
		remark:     remark,
		splits:     splits,
	}
	l.consumes[consumeID] = rec
	l.consumeByExternal[externalID] = consumeID

	l.appendEntry(&Entry{
		ID:         entryID,
		MemberID:   l.memberID,
		Type:       EntryConsume,
		Amount:     amount,
		OccurredAt: now,
		Remark:     remark,
		Consume: &ConsumeDetail{
			ExternalID: externalID,
			Splits:     detailSplits,
		},
	})

	return ConsumeReceipt{
		ConsumeID:  consumeID,
		ExternalID: externalID,
		Amount:     amount,
		Splits:     cloneSplits(detailSplits),
	}, nil
}

// Revoke 撤销一次消费。amount 为本次撤销数量：传正数表示部分撤销；
// 传 0 表示撤销该消费剩余尚未撤销的全部数量。
//
// 恢复严格按原消费记录的批次分摊顺序执行；某批次在撤销时刻已过期的，
// 对应数量只计入过期留痕，不重新成为可消费积分。累计撤销不会超过原消费。
// revokeExternalID 为撤销自身的幂等键，可留空。
func (l *Ledger) Revoke(consumeID string, amount int64, revokeExternalID, remark string) (RevokeReceipt, error) {
	if consumeID == "" {
		return RevokeReceipt{}, invalidArgumentf("consume id must not be empty")
	}
	if amount < 0 {
		return RevokeReceipt{}, invalidArgumentf("revoke amount must not be negative, got %d", amount)
	}
	requested := amount // 保留入参原值用于幂等比对（0 表示撤销全部剩余）

	l.mu.Lock()
	defer l.mu.Unlock()

	if revokeExternalID != "" {
		if eid, ok := l.revokeByExternal[revokeExternalID]; ok {
			e := l.entryByID[eid]
			d := e.Revoke
			if d.ConsumeID != consumeID || d.RequestedAmount != requested {
				return RevokeReceipt{}, idempotencyConflict(revokeExternalID, d.RequestedAmount, requested)
			}
			return RevokeReceipt{
				EntryID:       e.ID,
				ConsumeID:     consumeID,
				Amount:        e.Amount,
				Splits:        cloneRevokeSplits(d.Splits),
				Restored:      d.Restored,
				ExpiredAmount: d.ExpiredAmount,
			}, nil
		}
	}

	rec, ok := l.consumes[consumeID]
	if !ok {
		return RevokeReceipt{}, ErrConsumeNotFound
	}

	revocable := rec.unrevokedTotal()
	if amount == 0 {
		amount = revocable
		if amount == 0 {
			return RevokeReceipt{}, invalidArgumentf("consume %s has no remaining amount to revoke", consumeID)
		}
	}
	if amount > revocable {
		return RevokeReceipt{}, ErrRevokeExceedsConsume
	}

	now := l.clock()

	// 先推进到期批次，随后判定每笔恢复落在“可消费”还是“已过期”。
	if err := l.sweepExpiredLocked(now); err != nil {
		return RevokeReceipt{}, err
	}

	// 按原分摊顺序逐批恢复，保证“按照原消费记录恢复各批次”。
	need := amount
	splits := make([]RevokeSplit, 0, len(rec.splits))
	var restoredTotal, expiredTotal int64
	for i := range rec.splits {
		if need == 0 {
			break
		}
		s := &rec.splits[i]
		if s.unrevoked == 0 {
			continue
		}
		take := s.unrevoked
		if take > need {
			take = need
		}
		b := l.batchByID[s.batchID]
		rs := RevokeSplit{BatchID: s.batchID, Amount: take}
		if b.ExpiredSwept || !now.Before(b.ExpireAt) {
			// 批次已失效：释放消费占用并转入过期，余额不恢复。
			b.Consumed -= take
			b.Expired += take
			rs.ExpiredAmount = take
			expiredTotal += take
		} else {
			b.Consumed -= take
			b.Remaining += take
			rs.Restored = take
			restoredTotal += take
		}
		s.unrevoked -= take
		need -= take
		splits = append(splits, rs)
	}

	entryID := l.nextID("entry")
	if revokeExternalID != "" {
		l.revokeByExternal[revokeExternalID] = entryID
	}
	l.appendEntry(&Entry{
		ID:         entryID,
		MemberID:   l.memberID,
		Type:       EntryRevoke,
		Amount:     amount,
		OccurredAt: now,
		Remark:     remark,
		Revoke: &RevokeDetail{
			ConsumeID:       consumeID,
			ExternalID:      revokeExternalID,
			RequestedAmount: requested,
			Splits:          splits,
			Restored:        restoredTotal,
			ExpiredAmount:   expiredTotal,
		},
	})

	return RevokeReceipt{
		EntryID:       entryID,
		ConsumeID:     consumeID,
		Amount:        amount,
		Splits:        cloneRevokeSplits(splits),
		Restored:      restoredTotal,
		ExpiredAmount: expiredTotal,
	}, nil
}

// Expire 按统一时钟推进所有已到期批次：把其剩余可用积分核销为过期。
// 过期是单向的——已被过期推进的批次即使时钟回拨也不会复活。
// 没有批次到期时返回零值收据（不产生分录）。
func (l *Ledger) Expire() (ExpireReceipt, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.expireLocked(l.clock(), "")
}

// ExpireWithID 带业务流水号的过期扫描：同一 scanID 重复扫描直接返回
// 首次扫描的分录结果，不会把后续新到期的批次并入，也不会重复核销。
// 首次扫描没有任何批次到期时不记录该流水号（返回零值收据），
// 之后用同一 scanID 的扫描按首次有效扫描处理。
func (l *Ledger) ExpireWithID(scanID string) (ExpireReceipt, error) {
	if scanID == "" {
		return ExpireReceipt{}, invalidArgumentf("expire scan id must not be empty")
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if eid, ok := l.expireByScanID[scanID]; ok {
		e := l.entryByID[eid]
		return ExpireReceipt{
			EntryID: e.ID,
			ScanID:  scanID,
			Batches: cloneExpiredBatches(e.Expire.Batches),
		}, nil
	}

	r, err := l.expireLocked(l.clock(), scanID)
	if err != nil {
		return ExpireReceipt{}, err
	}
	if r.EntryID != "" {
		l.expireByScanID[scanID] = r.EntryID
	}
	return r, nil
}

func (l *Ledger) expireLocked(now time.Time, scanID string) (ExpireReceipt, error) {
	var batches []ExpiredBatch
	var total int64
	for _, b := range l.batches {
		if b.ExpiredSwept || now.Before(b.ExpireAt) {
			continue
		}
		// 标记是单向的：即使该批次剩余为 0，也不允许时钟回拨后复活。
		b.ExpiredSwept = true
		if b.Remaining == 0 {
			continue
		}
		amount := b.Remaining
		b.Remaining = 0
		b.Expired += amount
		batches = append(batches, ExpiredBatch{BatchID: b.ID, Amount: amount})
		total += amount
	}
	if total == 0 {
		return ExpireReceipt{}, nil
	}
	entryID := l.nextID("entry")
	l.appendEntry(&Entry{
		ID:         entryID,
		MemberID:   l.memberID,
		Type:       EntryExpire,
		Amount:     total,
		OccurredAt: now,
		Expire:     &ExpireDetail{ScanID: scanID, Batches: batches},
	})
	return ExpireReceipt{EntryID: entryID, ScanID: scanID, Batches: cloneExpiredBatches(batches)}, nil
}

// sweepExpiredLocked 在消费、撤销提交前先推进到期批次。
// 它与显式 Expire 走同一条过期核销路径，保证时间边界口径统一。
func (l *Ledger) sweepExpiredLocked(now time.Time) error {
	_, err := l.expireLocked(now, "")
	return err
}

// Available 返回当前时刻（统一时钟）的可用积分总量。
// 该查询不修改账本；是否已产生过期分录请以 Expire 的推进结果为准。
func (l *Ledger) Available() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.availableAtLocked(l.clock())
}

// AvailableAt 返回指定时刻视角下的可用积分总量（纯查询，不修改账本）。
func (l *Ledger) AvailableAt(t time.Time) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.availableAtLocked(t)
}

func (l *Ledger) availableAtLocked(now time.Time) int64 {
	var total int64
	for _, b := range l.batches {
		total += b.AvailableAt(now)
	}
	return total
}

// RecomputedBalance 由账本全部分录重算当前余额：
// 发放入账 − 消费出账 + 撤销中真正恢复可用的部分 − 过期核销。
// 它恒等于各批次 Remaining 之和，可用于对账校验“余额来自账本而非覆盖值”。
//
// 注意与 Available 的区别：Available 按当前时刻把“已到期但尚未推进”的
// 批次也视为不可用；RecomputedBalance 只反映已提交分录。推进过期后两者相等。
func (l *Ledger) RecomputedBalance() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	var total int64
	for _, e := range l.entries {
		switch e.Type {
		case EntryGrant:
			total += e.Amount
		case EntryConsume:
			total -= e.Amount
		case EntryRevoke:
			total += e.Revoke.Restored
		case EntryExpire:
			total -= e.Amount
		}
	}
	return total
}

// Batches 返回全部批次的副本，顺序与发放顺序一致。
func (l *Ledger) Batches() []*Batch {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]*Batch, len(l.batches))
	for i, b := range l.batches {
		out[i] = cloneBatch(b)
	}
	return out
}

// Batch 按 ID 返回批次副本，不存在返回 nil。
func (l *Ledger) Batch(id string) *Batch {
	l.mu.Lock()
	defer l.mu.Unlock()
	if b, ok := l.batchByID[id]; ok {
		return cloneBatch(b)
	}
	return nil
}

// Entries 返回账本全部明细分录的副本，顺序即业务发生顺序。
func (l *Ledger) Entries() []*Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]*Entry, len(l.entries))
	for i, e := range l.entries {
		out[i] = cloneEntry(e)
	}
	return out
}

// Entry 按 ID 返回明细分录副本，不存在返回 nil。
func (l *Ledger) Entry(id string) *Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e, ok := l.entryByID[id]; ok {
		return cloneEntry(e)
	}
	return nil
}

// ConsumeRecord 返回某条消费记录当前视角的收据副本（含首次分摊结果）。
func (l *Ledger) ConsumeRecord(consumeID string) (ConsumeReceipt, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if rec, ok := l.consumes[consumeID]; ok {
		return receiptOfConsume(rec), true
	}
	return ConsumeReceipt{}, false
}

func (l *Ledger) appendEntry(e *Entry) {
	l.entries = append(l.entries, e)
	l.entryByID[e.ID] = e
}

// planConsumeLocked 按 FEFO 在可用批次间分摊 amount。
// 返回分摊计划（只在总量足够时才会被提交）与当前可用总量。
func (l *Ledger) planConsumeLocked(amount int64, now time.Time) ([]ConsumeSplit, int64) {
	// 候选顺序：失效时间升序；同一失效时刻按发放先后（批次在 l.batches 中的位置）。
	cands := make([]candidate, 0, len(l.batches))
	var avail int64
	for i, b := range l.batches {
		a := b.AvailableAt(now)
		if a <= 0 {
			continue
		}
		avail += a
		cands = append(cands, candidate{idx: i, b: b})
	}
	sortCandidates(cands)

	plan := make([]ConsumeSplit, 0, len(cands))
	remaining := amount
	for _, c := range cands {
		if remaining == 0 {
			break
		}
		take := c.b.Remaining
		if take > remaining {
			take = remaining
		}
		plan = append(plan, ConsumeSplit{BatchID: c.b.ID, Amount: take})
		remaining -= take
	}
	return plan, avail
}

func (r *consumeRecord) unrevokedTotal() int64 {
	var n int64
	for _, s := range r.splits {
		n += s.unrevoked
	}
	return n
}

func receiptOfConsume(r *consumeRecord) ConsumeReceipt {
	splits := make([]ConsumeSplit, len(r.splits))
	for i, s := range r.splits {
		splits[i] = ConsumeSplit{BatchID: s.batchID, Amount: s.amount}
	}
	return ConsumeReceipt{
		ConsumeID:  r.id,
		ExternalID: r.externalID,
		Amount:     r.amount,
		Splits:     splits,
	}
}

func insufficientError(requested, available int64) error {
	return &insufficientPointsError{requested: requested, available: available}
}

type insufficientPointsError struct {
	requested int64
	available int64
}

func (e *insufficientPointsError) Error() string {
	return "goloyaltyledger: insufficient available points: requested " +
		itoa(e.requested) + ", available " + itoa(e.available)
}

func (e *insufficientPointsError) Is(target error) bool { return target == ErrInsufficientPoints }

func idempotencyConflict(externalID string, first, again int64) error {
	return &duplicateIDError{externalID: externalID, first: first, again: again}
}

// grantConflict 构造入账流水号冲突错误，列出首次与本次的关键内容。
func grantConflict(externalID string, first *Entry, firstExpireAt time.Time, amount int64, source string, expireAt time.Time) error {
	d := first.Grant
	return &duplicateIDError{
		externalID: externalID,
		first:      d.Amount,
		again:      amount,
		detail: "first source " + d.Source + " expireAt " + firstExpireAt.Format(time.RFC3339) +
			", repeat source " + source + " expireAt " + expireAt.Format(time.RFC3339),
	}
}

type duplicateIDError struct {
	externalID string
	first      int64
	again      int64
	detail     string
}

func (e *duplicateIDError) Error() string {
	msg := "goloyaltyledger: idempotency key reused with different request: " +
		e.externalID + " first amount " + itoa(e.first) + ", repeat amount " + itoa(e.again)
	if e.detail != "" {
		msg += " (" + e.detail + ")"
	}
	return msg
}

func (e *duplicateIDError) Is(target error) bool { return target == ErrDuplicateID }
