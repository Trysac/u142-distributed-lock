# Technology Stack

- **Go Version**: 1.23
- **Standard Library Components**:
  - `sync`: Mutex synchronization for thread-safe stores and fences.
  - `sync/atomic`: Atomic counters for test concurrency tracking.
  - `time`: Time representation, durations, and timers.
  - `context`: Cancellation and deadline propagation.
  - `container/list`: Doubly-linked list for `Fence` LRU eviction.
  - `errors`: Sentinel error construction and `errors.Is` comparison.
- **Dependencies**: 0 external runtime dependencies.
