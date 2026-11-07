# 0002-monotonic-fencing-tokens-for-lease-based-locks

Status: accepted (generated, pending human review)

## Context

Lease timeouts alone cannot guarantee mutual exclusion in asynchronous distributed systems. A process holding a lease may pause unexpectedly (due to GC, hypervisor stalls, or page faults) and resume after the lease has expired. Without a resource-side check, split-brain concurrent writes corrupt shared state.

## Decision

Introduce strictly increasing uint64 fencing tokens across all keys that survive lease release, provide an in-memory Store with an injected clock and capacity bounding, provide a blocking Locker with injected sleep, and provide a Fence checker with LRU eviction for resource-side validation.

## Consequences

Protected resources can reliably reject stale writes using Fence or atomic conditional queries (WHERE fence_token <= $1). MemoryStore is thread-safe for unit testing and single-node coordination. Multi-node coordination requires backing Store implementations (e.g. Redis Lua or PostgreSQL sequences). Reentrancy is unsupported.
