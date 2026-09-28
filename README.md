# go-loyalty-ledger

带有效期批次的会员积分账本（Go，内存实现，零外部依赖）。

- 积分**发放**后形成不可修改的**批次**，记录总量、剩余量、已消费量、已过期量和失效时刻；
- **消费**按"最早失效优先"（FEFO）跨批次扣减，一次消费整体成功或整体不生效；
- 外部消费号**幂等**：重复提交相同请求返回首次分摊结果，内容不一致返回冲突错误；
- **撤销**严格按照原消费记录逐批次恢复，支持部分撤销；撤销时批次已过期的部分保留可追溯记录，但不会重新变成可消费积分；
- **过期推进**与消费、撤销并发安全：每一分只沿合法方向移动，不会既被消费又被过期，也不会被重复恢复；
- 所有时间边界由统一的 `Clock` 判断。

## 安装与测试

```bash
go vet ./...
go test -race ./...
```

## 快速开始

```go
package main

import (
    "fmt"
    "time"

    lll "github.com/chris64233/go-loyalty-ledger"
)

func main() {
    base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
    clk := lll.NewFakeClock(base) // 生产环境可传 nil 使用系统时钟
    ledger := lll.NewLedger(clk)
    member := "member-001"

    // 发放：每笔发放形成一个独立批次
    ledger.Issue(lll.IssueRequest{
        MemberID: member, Amount: 30,
        ExpiresAt: base.Add(2 * time.Hour), RefNo: "issue-1",
    })
    ledger.Issue(lll.IssueRequest{
        MemberID: member, Amount: 50,
        ExpiresAt: base.Add(5 * time.Hour), RefNo: "issue-2",
    })

    // 消费 40：FEFO，先扣 2 点到期的 30，再扣 5 点到期的 10
    res, _ := ledger.Consume(lll.ConsumeRequest{
        MemberID: member, Amount: 40, RefNo: "order-1001",
    })
    fmt.Println(res.Allocations) // 两批分摊: [{B-1 30} {B-3 10}]

    // 撤销：按原消费的批次分摊精确恢复，而不是只加总余额
    ledger.Reverse(lll.ReverseRequest{
        MemberID: member, ConsumptionRefNo: "order-1001",
        Amount: 40, RefNo: "refund-1001",
    })

    // 过期推进（也会在任何读/写操作前惰性执行）
    clk.Advance(3 * time.Hour)
    bal, _ := ledger.Balance(member)
    fmt.Println(bal.Available, bal.Expired) // 50 30
}
```

## API 总览

| 方法 | 说明 |
| --- | --- |
| `NewLedger(clock Clock) *Ledger` | 创建账本；`clock` 传 `nil` 使用系统墙钟 |
| `Issue(IssueRequest) (*IssueResult, error)` | 发放积分，生成不可变批次（数量必须为正，失效时刻必须晚于当前时间） |
| `Consume(ConsumeRequest) (*ConsumeResult, error)` | 按 FEFO 消费，跨批次原子提交；外部 `RefNo` 幂等 |
| `Reverse(ReverseRequest) (*ReverseResult, error)` | 按原批次分摊撤销，可部分撤销；区分恢复可用/恢复已过期 |
| `AdvanceExpiry(memberID) (*ExpiryResult, error)` | 显式推进过期（通常无需调用，所有操作都会先惰性过期） |
| `Balance(memberID) (*Balance, error)` | 可用 / 已消费 / 已过期 / 累计发放余额 |
| `Batches(memberID) ([]BatchView, error)` | 各批次明细（按失效时刻排序） |
| `Entries(memberID) ([]Entry, error)` | 账本分录流水（发放/消费/撤销/过期） |

`Clock` 只有一个方法 `Now() time.Time`。包内提供：

- `SystemClock()`：`time.Now` 墙钟；
- `NewFakeClock(t)` / `(*FakeClock).SetNow` / `Advance`：测试用可控时钟。

## 核心不变量

对每个批次恒成立：

```
Total = Remaining + Consumed + Expired
```

对每个会员恒成立：

```
Issued = Available + Consumed + Expired
```

### 消费（FEFO 与原子性）

- 只考虑 `Remaining > 0` 且当前时间严格早于 `ExpiresAt` 的批次；
- 按失效时刻升序排序（同一时刻按批次序号确定顺序）；
- **先试算后落账**：可用总量不足时不修改任何批次，也不产生分录，返回
  `ErrInsufficientPoints`（错误信息含 `requested` / `available`）；
- 消费记录保存每个批次的扣减量；成功后这些积分从 `Remaining` 转入 `Consumed`。

### 过期（半开区间）

- 批次在 `now >= ExpiresAt` 的瞬间过期：`t == ExpiresAt` 即不可消费
  （`t.Before(ExpiresAt)` 才算有效）；
- 过期只动**剩余**积分：`Remaining -> Expired`，并产生一条 `expiry` 分录；
- 已被消费的部分不受过期推进影响，由撤销规则处理。

### 撤销（精确还原）

- 撤销必须引用原消费外部号，按原消费保存的批次分摊顺序恢复；
- 部分撤销先恢复最早批次的份额，累计撤销额不允许超过原消费量，否则返回
  `ErrConflict`；
- 对每个被恢复的批次，以撤销时刻的统一时钟判断：
  - 批次仍在有效期内：`Consumed -> Remaining`，计入 `RestoredAvailable`；
  - 批次已过期：`Consumed -> Expired`，计入 `RestoredExpired`，
    **只保留追溯记录，不会复活为可消费积分**；
- 撤销同样支持外部撤销号幂等（重复提交须指向同一消费、同一金额）。

### 幂等键

| 操作 | 键 | 冲突条件 |
| --- | --- | --- |
| 发放 | `(MemberID, RefNo)` | 金额或失效时刻与首次不同 |
| 消费 | `(MemberID, RefNo)` | 金额与首次不同 |
| 撤销 | `(MemberID, RefNo)` | 金额不同，或指向另一个消费 |

冲突时返回包装了 `ErrConflict` 的错误；重复的一致请求原样返回首次结果
（含首次的批次分摊与分录 ID），不会重复扣账或重复恢复。

## 错误处理

所有错误均可通过 `errors.Is` 判断：

| 哨兵错误 | 触发场景 |
| --- | --- |
| `ErrInvalidArgument` | 空会员/单号、数量非正、失效时刻为零或不在未来 |
| `ErrInsufficientPoints` | 可用积分无法原子覆盖本次消费（含请求量与可用量明细） |
| `ErrNotFound` | 撤销引用了不存在的消费单号 |
| `ErrConflict` | 幂等号复用但内容不一致；累计撤销超过原消费 |

## 并发模型

账本内部为"每会员独立账户 + 单把账本互斥锁"。发放、消费、撤销、余额、
批次与分录查询、过期推进都在同一把锁内完成，并且每个写操作进入临界区后
**先按统一时钟做惰性过期**。因此在任意交错下：

- 同一分积分不可能同时被消费与过期；
- 撤销恢复最多执行一次（幂等表 + 每条分摊的 `reversed` 上界）；
- 任何计数器都不会为负，守恒等式始终成立。

## 账本分录

`Entries` 返回按时间排列的不可变分录，类型为 `issue` / `consume` /
`reverse` / `expiry`：

- `consume` 分录的 `Allocations` 记录跨批次扣减明细；
- `reverse` 分录带 `ConsumptionRefNo`、同样的批次分摊，以及
  `RestoredAvailable` / `RestoredExpired` 两个数量；
- `expiry` 分录记录每个批次在失效时刻被核销的剩余量。

## 测试覆盖

`ledger_test.go` 覆盖：发放与余额、FEFO 跨批次分摊、不足额原子失败、
半开过期边界、消费/发放/撤销幂等（含并发重复提交）、并发消费不超扣、
全量/部分撤销按批次还原、撤销时批次已过期只留痕不复活、累计撤销上界、
完整分录流水，以及过期推进/消费/撤销/查询高并发混合下的全部守恒不变量
（`go test -race`）。
