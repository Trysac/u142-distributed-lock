// Package distributedlock provides lease-based distributed locks with strictly
// monotonic fencing tokens for Go services.
//
// # The Problem: Leases Alone Do Not Provide Mutual Exclusion
//
// Services often coordinate cron jobs, migrations, or queue consumers using locks
// backed by Redis or PostgreSQL. However, a timeout-based lease does not guarantee
// mutual exclusion in an asynchronous distributed system. If a process holding a lock
// experiences an unexpected pause (such as stop-the-world garbage collection, a hypervisor
// suspend, or network partition) and wakes up after its lease has expired, it may proceed
// with writing to shared storage concurrently with a new worker that acquired the lock.
//
// The established solution is a monotonically increasing fencing token. Every time a lock
// is acquired, the store issues a strictly greater token. Downstream protected resources
// (such as databases or storage engines) check this token (using [Fence] or SQL clauses
// such as `WHERE fence_token <= $1`) and reject stale writes from paused workers.
//
// # Architecture
//
//   - [Lease]: Represents an acquired lock with Key, Owner, Token, and ExpiresAt.
//   - [Store]: Interface for lock backends implementing TryAcquire, Renew, and Release.
//   - [MemoryStore]: Single-process, thread-safe implementation of [Store] with deterministic
//     clock injection and key capacity limits for testing and single-node use.
//   - [Locker]: Helper that loops TryAcquire with retry intervals and injected sleep.
//   - [Fence]: Resource-side validator enforcing monotonically increasing tokens per key
//     with an LRU eviction policy (default 10,000 keys).
//
// # Limits and Edge Cases
//
//   - Reentrancy: Not supported. An owner re-acquiring its own held lock receives [ErrHeld].
//   - Clock Movement Backwards: If the system clock moves backwards, active leases look
//     longer, preserving safety at the expense of delayed handoff.
//   - Single-Node Scope: [MemoryStore] is for tests and single-node coordination. Real
//     backends (Redis SET NX PX with Lua token counter, Postgres sequences, etcd leases)
//     implement [Store] and are out of scope.
//   - Scope Exclusions: Redlock, read-write locks, and automatic background renewal goroutines
//     are explicitly out of scope.
package distributedlock
