package goloyaltyledger

import (
	"errors"
	"strconv"
)

// Sentinel errors. Wrapped sentinel-compatible errors are returned by the
// Ledger so callers can use errors.Is.
var (
	// ErrInvalidArgument is returned when an argument is malformed
	// (empty member/reference number, non-positive amount, etc.).
	ErrInvalidArgument = errors.New("loyalty ledger: invalid argument")

	// ErrInsufficientPoints is returned when a consumption cannot be
	// satisfied atomically by the spendable balance.
	ErrInsufficientPoints = errors.New("loyalty ledger: insufficient points")

	// ErrNotFound is returned when a referenced consumption does not exist.
	ErrNotFound = errors.New("loyalty ledger: record not found")

	// ErrConflict is returned when an idempotency key is reused with
	// content different from the original request, or when a reversal
	// would exceed the (still reversible portion of the) consumption.
	ErrConflict = errors.New("loyalty ledger: conflicting request")
)

// argError wraps ErrInvalidArgument with a detail message.
type argError struct{ msg string }

func (e *argError) Error() string { return ErrInvalidArgument.Error() + ": " + e.msg }
func (e *argError) Unwrap() error { return ErrInvalidArgument }

// insufficientError wraps ErrInsufficientPoints, reporting the shortfall.
type insufficientError struct {
	requested int64
	available int64
}

func (e *insufficientError) Error() string {
	return "loyalty ledger: insufficient points: requested=" + strconv.FormatInt(e.requested, 10) +
		", available=" + strconv.FormatInt(e.available, 10)
}
func (e *insufficientError) Unwrap() error { return ErrInsufficientPoints }

// notFoundError wraps ErrNotFound with a detail message.
type notFoundError struct{ msg string }

func (e *notFoundError) Error() string { return ErrNotFound.Error() + ": " + e.msg }
func (e *notFoundError) Unwrap() error { return ErrNotFound }

// conflictError wraps ErrConflict with a detail message.
type conflictError struct{ msg string }

func (e *conflictError) Error() string { return ErrConflict.Error() + ": " + e.msg }
func (e *conflictError) Unwrap() error { return ErrConflict }
