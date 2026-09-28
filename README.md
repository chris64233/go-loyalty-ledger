# go-loyalty-ledger

带有效期批次的会员积分账本（Go 实现）。

## 功能概览

- **积分发放（Grant）**：入账即形成不可修改的批次，记录发放总量、剩余数量与失效时间；
  发放后批次的总量与失效时间不再改变。
- **积分消费（Consume）**：按“最早失效优先”（FEFO）在批次间分摊扣减；一次消费跨多个批次时
  整笔原子成功或失败，不产生部分扣减。外部消费号（`externalID`）提供幂等：
  重复提交相同内容返回首次分摊结果，内容不一致则返回 `ErrDuplicateID`。
- **消费撤销（Revoke）**：严格按原消费记录的批次分摊逐批恢复，而不是简单增加总余额。
  撤销时对应批次已过期的部分只保留可追溯记录（计入批次 `Expired` 与撤销分录明细），
  不重新变成可消费积分；累计撤销不得超过原消费，超过返回 `ErrRevokeExceedsConsume`。
- **过期推进（Expire）**：把已到期批次的剩余可用积分核销为过期（单向，不可复活）。
  消费与撤销在提交前会沿同一条路径先推进到期批次，保证时间边界口径统一。
- **查询**：`Available` / `AvailableAt` 查询可用余额，`Batches` / `Batch` 查询批次状态，
  `Entries` / `Entry` 查询不可变的账本明细分录（发放/消费/撤销/过期四类）。

## 并发与时间语义

- 所有写操作在单账本互斥锁内串行提交，并发消费不会造成负余额或超扣。
- 所有时间边界（批次是否已失效）只依据统一的 `Clock` 时间源判断；
  过期推进与消费、撤销交叉发生时，每个批次的数量只沿合法方向变化：
  同一批次不可能既被消费又被过期，也不会被重复恢复。
- 失效边界：当前时刻**早于** `ExpireAt` 时批次可用，到达 `ExpireAt` 即失效。
- 过期是单向的：已被推进的批次即使时钟回拨也不会复活。

## 快速开始

```go
clock := func() time.Time { return time.Now() }
l, err := goloyaltyledger.New("member-1", clock)
if err != nil { /* ... */ }

// 发放 100 分，24 小时后失效
g, _ := l.Grant(100, time.Now().Add(24*time.Hour), "开卡礼")
_ = g.BatchID

// 消费 30 分；重复提交相同 externalID 返回首次分摊结果
c, _ := l.Consume("order-123", 30, "下单抵扣")
_ = c.Splits // 各批次分摊明细

// 撤销 10 分（按原消费记录逐批恢复）；传 0 表示撤销全部剩余
_, _ = l.Revoke(c.ConsumeID, 10, "", "部分退款")

// 推进过期批次
_, _ = l.Expire()

// 查询
_ = l.Available()
_ = l.Entries()
```

错误通过哨兵错误表达，可用 `errors.Is` 判断：
`ErrInvalidArgument`、`ErrInsufficientPoints`、`ErrAlreadyExpired`、
`ErrDuplicateID`、`ErrConsumeNotFound`、`ErrRevokeExceedsConsume`。

## 测试

```sh
go test ./...        # 单元测试（含过期边界、幂等、撤销上限等）
go test -race ./...  # 并发竞争检测（并发消费/撤销/过期推进）
```
