# Architecture

`distributedlock` provides lease-based distributed locking with monotonic fencing tokens for Go services.

## Problem and Core Concepts

In distributed systems, timeouts alone cannot guarantee safety against process pauses or clock anomalies. Consider this sequence without fencing:

1. Worker A acquires lease for `job` (TTL 10s).
2. Worker A enters a stop-the-world GC pause for 12s.
3. The lease expires; Worker B acquires `job` (TTL 10s).
4. Worker A resumes and writes to the database concurrently with Worker B.

With fencing tokens:
1. Worker A receives `Token 1`.
2. Worker B receives `Token 2`.
3. When Worker A resumes and attempts to write with `Token 1`, the database or `Fence` rejects the write because it has already seen `Token 2` (or requires `fence_token >= 2`).

## Component Breakdown

```
+-------------------------------------------------------+
|                      Application                      |
+-------------------------------------------------------+
       |                                      |
       v Acquire / Renew / Release            v Check(key, token)
+-------------------------+             +---------------+
|      Store / Locker     |             |     Fence     |
+-------------------------+             +---------------+
| MemoryStore             |             | LRU cache     |
| (clock, counter, mutex) |             | (MaxKeys)     |
+-------------------------+             +---------------+
```

### 1. Store Interface

```go
type Store interface {
    TryAcquire(ctx context.Context, key, owner string, ttl time.Duration) (Lease, error)
    Renew(ctx context.Context, l Lease, ttl time.Duration) (Lease, error)
    Release(ctx context.Context, l Lease) error
}
```

- **TryAcquire**: Validates key (1-256 bytes), owner (1-256 bytes), and TTL (> 0). Purges expired leases before checking capacity limits.
- **Renew**: Extends an active lease. Requires matching owner and token. Expired leases return `ErrExpired`.
- **Release**: Releases a held lease. Returns `ErrNotOwner` if owner or token mismatch; returns `nil` if already released or expired with no active holder.

### 2. MemoryStore

- **Counter**: One global `uint64` counter strictly increasing across all keys. Tokens survive lease releases and deletions.
- **Capacity**: Bounded by `maxKeys` (default 10,000). Expired keys are purged before enforcing capacity limits.
- **Clock Injection**: Takes `now func() time.Time` for deterministic testing.

### 3. Locker

Coordinates retry loops around `TryAcquire`:
- Retries when `TryAcquire` returns `ErrHeld`.
- Aborts immediately on non-`ErrHeld` errors.
- Uses configurable `RetryInterval` (default 100ms) and injectable `Sleep` function for testability without real timers.

### 4. Fence

Guards downstream protected resources:
- Zero-value usable (`var fence Fence`).
- Checks `token >= highest_seen`.
- Mutex-guarded doubly linked list LRU cache with capacity `MaxKeys` (default 10,000).

## Out of Scope

- Redlock algorithm.
- Read-write or reentrant locks.
- Automatic background renewal goroutines (callers manage lease lifecycle explicitly).
