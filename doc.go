// Package goloyaltyledger implements an in-memory loyalty-points ledger
// with expiring batches.
//
// Points are issued into immutable batches, each carrying its own expiry
// instant and its own remaining / consumed / expired counters. Consumption
// spends the batches that expire first (FEFO). A consumption that spans
// several batches is atomic: either all required points are reserved or
// nothing is. Reversal restores the exact per-batch allocations of the
// original consumption rather than adding to a single global balance; the
// portion that has already expired is preserved on the audit trail but can
// never become spendable again.
//
// Every time boundary ("has this batch expired yet?") is evaluated against
// one Clock held by the Ledger, so issuance, consumption, reversal and
// expiry advancement share a single notion of "now". Expiry is applied
// lazily at the start of every state transition under the ledger mutex,
// which makes expiry vs. consume vs. reverse races impossible: any given
// point moves only in legal directions (remaining -> consumed -> back to
// remaining, or remaining/consumed -> expired), never twice.
package goloyaltyledger
