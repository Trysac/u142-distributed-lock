# distributed-lock

Lease-based distributed locks with strictly monotonic fencing tokens in Go.

## The Problem

In distributed systems, a process can pause (GC, page faults, etc.) after its lease expires. If it resumes and writes to shared storage, it can corrupt data being concurrently written by a new lock holder. Simple timeouts are insufficient; the known fix is using a fencing token validated at write-time by the protected resource.

## Installation

```bash
go get github.com/Trysac/u142-distributed-lock
```

## Usage Example

```go
store := distributedlock.NewMemoryStore(nil, 0)
lease, err := store.TryAcquire(context.Background(), "migration", "worker-1", 30*time.Second)
if err != nil {
	log.Fatal(err)
}
defer store.Release(context.Background(), lease)

var fence distributedlock.Fence
if err := fence.Check(lease.Key, lease.Token); err != nil {
	log.Fatal("stale token:", err)
}
```

## API Overview

### Types

- `Lease`: Holds `Key`, `Owner` string, `Token uint64`, and `ExpiresAt time.Time`.
- `Store`: Interface with `TryAcquire`, `Renew`, and `Release`.
- `MemoryStore`: In-memory implementation of `Store`. Created via `NewMemoryStore(now func() time.Time, maxKeys int)`. Max keys default 10,000 if 0 passed.
- `Locker`: Coordinates retries. Attributes: `Store` (Store), `RetryInterval` (default 100ms), `Sleep` (func(ctx context.Context, d time.Duration) error).
- `Fence`: Zero-value usable. Attributes: `Check(key string, token uint64) error`. Uses an LRU cache (default 10,000 entries) to guard resources.

### Errors

Errors are standard sentinel values checked via `errors.Is`:
- `ErrHeld`: Lock already held.
- `ErrExpired`: Lease missing or expired.
- `ErrNotOwner`: Mismatched owner/token on renew/release.
- `ErrStaleToken`: Token lower than highest seen.
- `ErrInvalidKey`: Key/owner length invalid (1-256 bytes).
- `ErrInvalidTTL`: TTL <= 0.
- `ErrTooManyKeys`: Capacity limit reached.

## Why not existing alternatives?

Existing lock libraries often lack native support for strict fencing tokens and deterministic testing of the locking state. This utility makes fencing tokens and resource-side checks first-class citizens with a clean `Store` interface that supports real backends (e.g., Redis Lua scripts) and deterministic unit tests.

## Security

See [docs/SECURITY.md](docs/SECURITY.md) for the threat model and defense boundaries.