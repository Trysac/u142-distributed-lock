// Package distributedlock is a small, dependency-free Go library: Lease-based locks with monotonic fencing tokens in Go: a Store interface, an in-memory store with an injected clock, a blocking Locker and a fence checker
//
// The public API is every exported identifier of this root package. Functions are pure: they do
// no I/O, keep no global mutable state and report invalid input through returned errors.
package distributedlock
