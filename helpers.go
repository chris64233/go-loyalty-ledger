package goloyaltyledger

import (
	"sort"
	"strconv"
)

// itoa 是整数到字符串的薄封装，集中 ID 与错误信息的格式化口径。
func itoa(n int64) string { return strconv.FormatInt(n, 10) }

// candidate 是 FEFO 排序候选项。
type candidate struct {
	idx int
	b   *Batch
}

// sortCandidates 按失效时间升序、同失效时间按发放先后稳定排序。
func sortCandidates(cs []candidate) {
	sort.SliceStable(cs, func(i, j int) bool {
		if !cs[i].b.ExpireAt.Equal(cs[j].b.ExpireAt) {
			return cs[i].b.ExpireAt.Before(cs[j].b.ExpireAt)
		}
		return cs[i].idx < cs[j].idx
	})
}

// cloneEntry 返回明细分录的深拷贝，避免调用方改到账本内部状态。
func cloneEntry(e *Entry) *Entry {
	cp := *e
	if e.Grant != nil {
		g := *e.Grant
		cp.Grant = &g
	}
	if e.Consume != nil {
		c := *e.Consume
		c.Splits = cloneSplits(e.Consume.Splits)
		cp.Consume = &c
	}
	if e.Revoke != nil {
		r := *e.Revoke
		r.Splits = cloneRevokeSplits(e.Revoke.Splits)
		cp.Revoke = &r
	}
	if e.Expire != nil {
		x := *e.Expire
		x.Batches = cloneExpiredBatches(e.Expire.Batches)
		cp.Expire = &x
	}
	return &cp
}
