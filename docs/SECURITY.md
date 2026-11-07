# Security and Threat Model

## Defended Threats

- **Process Pauses**: Protects against stale writes via monotonically increasing fencing tokens. Protected resources should validate these tokens (e.g., `UPDATE ... WHERE fence_token <= $1`).
- **Resource Exhaustion**: `MemoryStore` and `Fence` have strictly enforced capacity limits (default 10,000). Key/owner lengths are limited to 1-256 bytes to prevent DoS via invalid inputs.
- **Clock Anomalies**: If the clock moves backward, leases appear longer, preserving mutual exclusion. If the clock moves forward, leases expire faster, favoring safety over liveness.

## Not Defended

- **Process Isolation**: This is an in-process coordinator (`MemoryStore`). For multi-node deployments, the user must implement `Store` using a shared backend (e.g., PostgreSQL or Redis) to maintain state consistency across physical nodes.
- **Storage-Side Persistence**: `MemoryStore` is volatile. State loss occurs on process termination.