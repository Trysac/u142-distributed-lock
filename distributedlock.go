package distributedlock

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"time"
)

// Sentinel errors returned by distributedlock operations.
// All errors can be compared using [errors.Is].
var (
	// ErrHeld is returned when a requested lock is already held by another owner
	// or when the current owner attempts to re-acquire without releasing (no reentrancy).
	ErrHeld = errors.New("distributedlock: lock is held")

	// ErrExpired is returned when renewing an expired lease or when a lease does not exist.
	ErrExpired = errors.New("distributedlock: lease is expired")

	// ErrNotOwner is returned when attempting to renew or release a lease with a mismatched owner or token.
	ErrNotOwner = errors.New("distributedlock: not lock owner or token mismatch")

	// ErrStaleToken is returned by [Fence.Check] when a fencing token is strictly lower than the highest seen for the key.
	ErrStaleToken = errors.New("distributedlock: stale fencing token")

	// ErrInvalidKey is returned when a key or owner byte length is outside [1, 256].
	ErrInvalidKey = errors.New("distributedlock: invalid key or owner")

	// ErrInvalidTTL is returned when a lease TTL is less than or equal to zero.
	ErrInvalidTTL = errors.New("distributedlock: invalid ttl")

	// ErrTooManyKeys is returned when attempting to acquire a lock in a store that has reached its maximum key capacity.
	ErrTooManyKeys = errors.New("distributedlock: store max keys exceeded")
)

// Lease represents an acquired distributed lock.
type Lease struct {
	Key       string
	Owner     string
	Token     uint64
	ExpiresAt time.Time
}

// Store defines the interface for distributed lock storage engines.
type Store interface {
	// TryAcquire attempts to acquire a lease for key and owner with the given ttl.
	// Returns ErrHeld if the lock is held, ErrTooManyKeys if the capacity limit is reached,
	// or ErrInvalidKey / ErrInvalidTTL on validation failure.
	TryAcquire(ctx context.Context, key, owner string, ttl time.Duration) (Lease, error)

	// Renew extends an active lease by ttl.
	// Returns ErrExpired if the lease has expired, ErrNotOwner on owner or token mismatch,
	// or validation errors.
	Renew(ctx context.Context, l Lease, ttl time.Duration) (Lease, error)

	// Release releases a held lease.
	// Returns ErrNotOwner if owner or token mismatch, or nil if already released or expired
	// while no one else holds the key.
	Release(ctx context.Context, l Lease) error
}

type memoryEntry struct {
	owner     string
	token     uint64
	expiresAt time.Time
}

// MemoryStore provides a thread-safe, single-process in-memory implementation of [Store].
// It is intended for testing, development, and single-node coordination.
type MemoryStore struct {
	mu        sync.Mutex
	now       func() time.Time
	maxKeys   int
	lastToken uint64
	entries   map[string]memoryEntry
}

// NewMemoryStore constructs a new MemoryStore.
// If now is nil, time.Now is used. If maxKeys is <= 0, a default of 10,000 is used.
func NewMemoryStore(now func() time.Time, maxKeys int) *MemoryStore {
	if now == nil {
		now = time.Now
	}
	if maxKeys <= 0 {
		maxKeys = 10_000
	}
	return &MemoryStore{
		now:     now,
		maxKeys: maxKeys,
		entries: make(map[string]memoryEntry),
	}
}

func validateKeyAndOwner(key, owner string) error {
	if len(key) < 1 || len(key) > 256 || len(owner) < 1 || len(owner) > 256 {
		return ErrInvalidKey
	}
	return nil
}

// TryAcquire attempts to acquire a lease on the in-memory store.
func (s *MemoryStore) TryAcquire(ctx context.Context, key, owner string, ttl time.Duration) (Lease, error) {
	if err := ctx.Err(); err != nil {
		return Lease{}, err
	}
	if err := validateKeyAndOwner(key, owner); err != nil {
		return Lease{}, err
	}
	if ttl <= 0 {
		return Lease{}, ErrInvalidTTL
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	e, exists := s.entries[key]
	if exists && now.Before(e.expiresAt) {
		return Lease{}, ErrHeld
	}

	// Purge expired entries before checking capacity.
	for k, v := range s.entries {
		if !now.Before(v.expiresAt) {
			delete(s.entries, k)
		}
	}

	if _, ok := s.entries[key]; !ok && len(s.entries) >= s.maxKeys {
		return Lease{}, ErrTooManyKeys
	}

	s.lastToken++
	expiresAt := now.Add(ttl).UTC()
	s.entries[key] = memoryEntry{
		owner:     owner,
		token:     s.lastToken,
		expiresAt: expiresAt,
	}

	return Lease{
		Key:       key,
		Owner:     owner,
		Token:     s.lastToken,
		ExpiresAt: expiresAt,
	}, nil
}

// Renew extends an active lease by ttl.
func (s *MemoryStore) Renew(ctx context.Context, l Lease, ttl time.Duration) (Lease, error) {
	if err := ctx.Err(); err != nil {
		return Lease{}, err
	}
	if err := validateKeyAndOwner(l.Key, l.Owner); err != nil {
		return Lease{}, err
	}
	if ttl <= 0 {
		return Lease{}, ErrInvalidTTL
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	e, exists := s.entries[l.Key]
	if !exists {
		return Lease{}, ErrExpired
	}
	if e.owner != l.Owner || e.token != l.Token {
		return Lease{}, ErrNotOwner
	}
	if !now.Before(e.expiresAt) {
		return Lease{}, ErrExpired
	}

	expiresAt := now.Add(ttl).UTC()
	s.entries[l.Key] = memoryEntry{
		owner:     e.owner,
		token:     e.token,
		expiresAt: expiresAt,
	}

	return Lease{
		Key:       l.Key,
		Owner:     e.owner,
		Token:     e.token,
		ExpiresAt: expiresAt,
	}, nil
}

// Release releases an acquired lease.
func (s *MemoryStore) Release(ctx context.Context, l Lease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateKeyAndOwner(l.Key, l.Owner); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	e, exists := s.entries[l.Key]
	if !exists {
		return nil
	}
	if e.owner != l.Owner || e.token != l.Token {
		return ErrNotOwner
	}
	delete(s.entries, l.Key)
	return nil
}

// Locker coordinates blocking lock acquisition with retries.
type Locker struct {
	Store         Store
	RetryInterval time.Duration
	Sleep         func(ctx context.Context, d time.Duration) error
}

func defaultSleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Acquire blocks until the lock is acquired, the context is cancelled, or an error occurs.
func (l *Locker) Acquire(ctx context.Context, key, owner string, ttl time.Duration) (Lease, error) {
	if err := ctx.Err(); err != nil {
		return Lease{}, err
	}
	interval := l.RetryInterval
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	sleep := l.Sleep
	if sleep == nil {
		sleep = defaultSleep
	}

	for {
		lease, err := l.Store.TryAcquire(ctx, key, owner, ttl)
		if err == nil {
			return lease, nil
		}
		if !errors.Is(err, ErrHeld) {
			return Lease{}, err
		}
		if err := sleep(ctx, interval); err != nil {
			return Lease{}, err
		}
	}
}

type fenceItem struct {
	key   string
	token uint64
}

// Fence guards protected resources by checking monotonically increasing tokens per key.
// The zero value is usable. The internal cache retains at most MaxKeys entries (default 10,000),
// evicting the least recently checked key when full.
type Fence struct {
	MaxKeys int
	mu      sync.Mutex
	entries map[string]*list.Element
	lru     *list.List
}

// Check validates that token is greater than or equal to the highest token seen for key.
// If token is strictly lower than the highest seen, ErrStaleToken is returned.
// Accepted tokens update the recorded token and mark the key as most recently checked.
func (f *Fence) Check(key string, token uint64) error {
	if len(key) < 1 || len(key) > 256 {
		return ErrInvalidKey
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.entries == nil {
		f.entries = make(map[string]*list.Element)
		f.lru = list.New()
	}

	maxKeys := f.MaxKeys
	if maxKeys <= 0 {
		maxKeys = 10_000
	}

	if elem, ok := f.entries[key]; ok {
		item := elem.Value.(*fenceItem)
		if token < item.token {
			f.lru.MoveToFront(elem)
			return ErrStaleToken
		}
		item.token = token
		f.lru.MoveToFront(elem)
		return nil
	}

	if f.lru.Len() >= maxKeys {
		oldest := f.lru.Back()
		if oldest != nil {
			oldItem := oldest.Value.(*fenceItem)
			delete(f.entries, oldItem.key)
			f.lru.Remove(oldest)
		}
	}

	elem := f.lru.PushFront(&fenceItem{key: key, token: token})
	f.entries[key] = elem
	return nil
}
