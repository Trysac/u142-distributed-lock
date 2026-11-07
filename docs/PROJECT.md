# Project Record

## Identity

- **Module**: `github.com/Trysac/u142-distributed-lock`
- **Package**: `distributedlock`
- **Language**: Go 1.23

## Objective

To provide a lease-based locking mechanism in Go that prevents data corruption during process pauses by enforcing strict fencing tokens. The package provides a verifiable `Store` interface, an in-memory testable store, a retry-capable `Locker`, and a resource-side `Fence` checker.

## Scope

- Injected clock support for deterministic testing.
- Strictly monotonic token generation that survives lease release.
- Lease lifecycle management (acquire, renew, release).
- Monotonic token validation via `Fence.Check`.
- Blocking lock acquisition with retry logic.
- Support for key/owner validation and capacity limits.

## Out of Scope

- Redlock algorithm implementations.
- Read-write locks.
- Reentrant locking patterns.
- Automatic background renewal goroutines (lifecycle is caller-managed).

## Factory run u142 (measured from API usage)

| Phase | Model | Batch | Input | Output | Cache write | Cache read | Cost (USD) |
|---|---|---|---|---|---|---|---|
| generation | gemini-3.8-flash | yes | 4080 | 46790 | 0 | 0 | 0.0893 |
| audit | gemini-3.1-flash-lite | no | 26684 | 105 | 0 | 20364 | 0.0073 |
| docs | gemini-3.1-flash-lite | no | 4884 | 1470 | 0 | 0 | 0.0034 |

Total: $0.1000. Prices: Anthropic list prices.
