package goloyaltyledger

import "time"

// Clock 是全账本唯一的“当前时间”来源。
// 发放、消费、撤销、过期推进中的所有时间边界（批次是否已失效）
// 都只依据 Clock 返回的时刻判断，从而保证各操作交叉发生时口径一致。
type Clock func() time.Time

// EntryType 标识一条账本分录的业务类型。
type EntryType string

const (
	// EntryGrant 积分发放。
	EntryGrant EntryType = "grant"
	// EntryConsume 积分消费。
	EntryConsume EntryType = "consume"
	// EntryRevoke 消费撤销（按原消费分摊恢复批次）。
	EntryRevoke EntryType = "revoke"
	// EntryExpire 过期推进（批次剩余积分被核销为不可消费）。
	EntryExpire EntryType = "expire"
)

// Batch 是一个不可变业务事实下的积分批次：发放时生成，
// 其发放总量与失效时间此后不再改变；其余字段描述当前状态。
type Batch struct {
	// ID 批次 ID。
	ID string
	// MemberID 所属会员。
	MemberID string
	// Source 积分来源（如“开卡礼”“消费返积分”），自发放后不再改变。
	Source string
	// Granted 发放总量，自发放后永不改变。
	Granted int64
	// Remaining 当前仍可用于消费的数量（尚未消费、未撤销恢复、未过期）。
	Remaining int64
	// Consumed 已被消费占用、且尚未被撤销恢复的数量。
	Consumed int64
	// Expired 已被过期推进核销的数量，永远不能重新变为可消费积分。
	Expired int64
	// ExpireAt 失效时刻：now 在该时刻之前批次可用，到达该时刻即失效。
	// 批次的可用时间段为 [CreatedAt, ExpireAt)。
	ExpireAt time.Time
	// CreatedAt 发放时刻（取自统一时钟）。
	CreatedAt time.Time
	// ExpiredSwept 是否已经执行过过期推进；一旦为 true，
	// 即使时钟被回拨，该批次也不会复活。
	ExpiredSwept bool
}

// AvailableAt 返回给定时刻该批次的可用积分数量。
// 已过期（或已推进过）的批次返回 0；该方法不改变任何状态。
func (b *Batch) AvailableAt(now time.Time) int64 {
	if b.ExpiredSwept || !now.Before(b.ExpireAt) {
		return 0
	}
	return b.Remaining
}

// ConsumeSplit 描述一次消费在单个批次上的分摊。
type ConsumeSplit struct {
	BatchID string
	Amount  int64
}

// RevokeSplit 描述一次撤销在单个批次上的恢复分摊。
// 恢复时批次若仍在有效期内，Amount 回到可消费余额；
// 若批次已过期，Amount 计入 Expired，仅保留追溯记录。
type RevokeSplit struct {
	BatchID string
	Amount  int64
	// Restored 其中真正恢复为可消费积分的数量（批次未过期）。
	Restored int64
	// ExpiredAmount 其中因批次已过期而不能恢复、仅留痕的数量。
	ExpiredAmount int64
}

// ExpiredBatch 描述过期推进中单个批次被核销的数量。
type ExpiredBatch struct {
	BatchID string
	Amount  int64
}

// GrantDetail 发放分录明细。
type GrantDetail struct {
	BatchID string
	Amount  int64
	// ExternalID 业务流水号（幂等键），可能为空。
	ExternalID string
	// Source 积分来源。
	Source string
}

// ConsumeDetail 消费分录明细。
type ConsumeDetail struct {
	ExternalID string
	Splits     []ConsumeSplit
}

// RevokeDetail 撤销分录明细。
type RevokeDetail struct {
	ConsumeID  string
	ExternalID string // 撤销自身的幂等键，可能为空
	// RequestedAmount 撤销请求的入参数量；0 表示“撤销全部剩余”，
	// 与实际撤销量 Amount 分开记录，以便幂等重放时还原语义。
	RequestedAmount int64
	Splits          []RevokeSplit
	Restored        int64 // 实际恢复为可用余额的总量
	ExpiredAmount   int64 // 因批次已过期而仅留痕的总量
}

// ExpireDetail 过期分录明细。
type ExpireDetail struct {
	Batches []ExpiredBatch
}

// Entry 是一条不可变的账本流水。所有写操作都会追加一条分录。
type Entry struct {
	// ID 分录 ID，全账本唯一。
	ID string
	// MemberID 所属会员。
	MemberID string
	// Type 业务类型。
	Type EntryType
	// Amount 数量，恒为正数：发放/撤销恢复取入账量，消费/过期取出账量。
	Amount int64
	// OccurredAt 业务发生时刻，取自统一时钟。
	OccurredAt time.Time
	// Remark 可选备注。
	Remark string

	// 以下字段按 Type 仅有一个生效。
	Grant   *GrantDetail
	Consume *ConsumeDetail
	Revoke  *RevokeDetail
	Expire  *ExpireDetail
}

// GrantReceipt 发放结果。
type GrantReceipt struct {
	EntryID string
	BatchID string
	Amount  int64
}

// ConsumeReceipt 消费结果。重复提交相同内容时返回首次的分摊结果。
type ConsumeReceipt struct {
	ConsumeID  string
	ExternalID string
	Amount     int64
	Splits     []ConsumeSplit
}

// RevokeReceipt 撤销结果。
type RevokeReceipt struct {
	EntryID       string
	ConsumeID     string
	Amount        int64
	Splits        []RevokeSplit
	Restored      int64
	ExpiredAmount int64
}

// ExpireReceipt 过期推进结果。没有批次过期时返回零值收据。
type ExpireReceipt struct {
	EntryID string
	Batches []ExpiredBatch
}

// cloneBatch 返回批次的副本，避免调用方通过查询结果修改账本内部状态。
func cloneBatch(b *Batch) *Batch {
	cp := *b
	return &cp
}

func cloneSplits(in []ConsumeSplit) []ConsumeSplit {
	if len(in) == 0 {
		return nil
	}
	out := make([]ConsumeSplit, len(in))
	copy(out, in)
	return out
}

func cloneRevokeSplits(in []RevokeSplit) []RevokeSplit {
	if len(in) == 0 {
		return nil
	}
	out := make([]RevokeSplit, len(in))
	copy(out, in)
	return out
}

func cloneExpiredBatches(in []ExpiredBatch) []ExpiredBatch {
	if len(in) == 0 {
		return nil
	}
	out := make([]ExpiredBatch, len(in))
	copy(out, in)
	return out
}

// cloneExpireReceipt 返回过期推进收据的副本。
func cloneExpireReceipt(r ExpireReceipt) ExpireReceipt {
	r.Batches = cloneExpiredBatches(r.Batches)
	return r
}
