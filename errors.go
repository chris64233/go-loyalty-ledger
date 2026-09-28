package goloyaltyledger

import (
	"errors"
	"fmt"
)

// 账本可能返回的错误。业务方可以用 errors.Is 判断错误类别。
var (
	// ErrInvalidArgument 表示入参不合法（数量非正、时间为零值等）。
	ErrInvalidArgument = errors.New("goloyaltyledger: invalid argument")
	// ErrInsufficientPoints 表示可用积分不足以完成本次消费。
	ErrInsufficientPoints = errors.New("goloyaltyledger: insufficient available points")
	// ErrAlreadyExpired 表示发放批次时失效时间不晚于当前时间。
	ErrAlreadyExpired = errors.New("goloyaltyledger: expiry must be in the future")
	// ErrDuplicateID 表示幂等键已存在，但重复提交的内容与首次不一致。
	ErrDuplicateID = errors.New("goloyaltyledger: idempotency key reused with different request")
	// ErrConsumeNotFound 表示撤销时找不到对应的消费记录。
	ErrConsumeNotFound = errors.New("goloyaltyledger: consume record not found")
	// ErrRevokeExceedsConsume 表示累计撤销数量将超过原消费中尚未撤销的数量。
	ErrRevokeExceedsConsume = errors.New("goloyaltyledger: cumulative revoke exceeds consume")
)

// invalidArgumentf 包装一个带上下文的 ErrInvalidArgument。
func invalidArgumentf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidArgument, fmt.Sprintf(format, args...))
}
