# Testing Guide

## Overview

Tests are located in `distributedlock_test`. They focus on deterministic execution and public API correctness without side effects.

## Test Coverage

- **Clock Injection**: `fakeClock` verifies TTL expiration, renewal cutoffs, and backwards clock jumps.
- **Race Conditions**: High-concurrency tests execute with `go test -race` (e.g., 100 goroutines competing for a lock, verifying exactly 1 success and 99 `ErrHeld`).
- **Retry/Locker Logic**: Injected sleep functions allow testing of retry counts and context cancellation handling without wall-clock waits.
- **Boundary Conditions**: Tests cover empty keys, 257-byte keys, zero TTL, and capacity saturation (eviction).
- **LRU Cache**: Explicit assertions that `Fence` evicts the least recently checked entry when full.