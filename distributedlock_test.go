package distributedlock_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	distributedlock "github.com/Trysac/u142-distributed-lock"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{now: start}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Acceptance 1:
// With an injected clock, TryAcquire(ctx, "job", "a", 10*time.Second) returns a Lease with Token 1;
// TryAcquire by owner "b" returns an error matching ErrHeld; after advancing the clock by 10 s, "b" acquires with Token 2
func TestAcceptance1_ClockAndTokens(t *testing.T) {
	clock := newFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	store := distributedlock.NewMemoryStore(clock.Now, 0)
	ctx := context.Background()

	l1, err := store.TryAcquire(ctx, "job", "a", 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if l1.Token != 1 {
		t.Fatalf("expected Token 1, got %d", l1.Token)
	}

	_, err = store.TryAcquire(ctx, "job", "b", 10*time.Second)
	if !errors.Is(err, distributedlock.ErrHeld) {
		t.Fatalf("expected error matching ErrHeld, got %v", err)
	}

	clock.Advance(10 * time.Second)

	l2, err := store.TryAcquire(ctx, "job", "b", 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if l2.Token != 2 {
		t.Fatalf("expected Token 2, got %d", l2.Token)
	}
}

// Acceptance 2:
// Renew 5 s after acquiring with ttl 10*time.Second returns the same Token and ExpiresAt == now.Add(10*time.Second).UTC();
// Renew 10 s after the last renewal returns ErrExpired
func TestAcceptance2_RenewLifecycle(t *testing.T) {
	clock := newFakeClock(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC))
	store := distributedlock.NewMemoryStore(clock.Now, 0)
	ctx := context.Background()

	lease, err := store.TryAcquire(ctx, "job", "a", 10*time.Second)
	if err != nil {
		t.Fatalf("acquire failed: %v", err)
	}

	clock.Advance(5 * time.Second)

	renewed, err := store.Renew(ctx, lease, 10*time.Second)
	if err != nil {
		t.Fatalf("renew failed: %v", err)
	}
	if renewed.Token != lease.Token {
		t.Fatalf("expected same Token %d, got %d", lease.Token, renewed.Token)
	}
	wantExpires := clock.Now().Add(10 * time.Second).UTC()
	if !renewed.ExpiresAt.Equal(wantExpires) {
		t.Fatalf("expected ExpiresAt == %v, got %v", wantExpires, renewed.ExpiresAt)
	}

	clock.Advance(10 * time.Second)

	_, err = store.Renew(ctx, renewed, 10*time.Second)
	if !errors.Is(err, distributedlock.ErrExpired) {
		t.Fatalf("expected ErrExpired, got %v", err)
	}
}

// Acceptance 3:
// Release with a different owner or token returns ErrNotOwner and the lock stays held;
// Release by the holder frees it, and the next acquirer gets Token 3, never a reused token
func TestAcceptance3_ReleaseAndMonotonicTokens(t *testing.T) {
	clock := newFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	store := distributedlock.NewMemoryStore(clock.Now, 0)
	ctx := context.Background()

	// Token 1
	_, _ = store.TryAcquire(ctx, "other", "x", 10*time.Second)
	// Token 2
	l2, err := store.TryAcquire(ctx, "job", "a", 10*time.Second)
	if err != nil || l2.Token != 2 {
		t.Fatalf("acquire failed: %v, token: %d", err, l2.Token)
	}

	wrongOwnerLease := l2
	wrongOwnerLease.Owner = "intruder"
	if err := store.Release(ctx, wrongOwnerLease); !errors.Is(err, distributedlock.ErrNotOwner) {
		t.Fatalf("expected ErrNotOwner, got %v", err)
	}

	wrongTokenLease := l2
	wrongTokenLease.Token = 999
	if err := store.Release(ctx, wrongTokenLease); !errors.Is(err, distributedlock.ErrNotOwner) {
		t.Fatalf("expected ErrNotOwner, got %v", err)
	}

	// Verify lock stays held
	if _, err := store.TryAcquire(ctx, "job", "b", 10*time.Second); !errors.Is(err, distributedlock.ErrHeld) {
		t.Fatalf("expected lock to stay held, got %v", err)
	}

	// Release by holder
	if err := store.Release(ctx, l2); err != nil {
		t.Fatalf("release by holder failed: %v", err)
	}

	// Next acquirer gets Token 3
	l3, err := store.TryAcquire(ctx, "job", "b", 10*time.Second)
	if err != nil {
		t.Fatalf("acquire after release failed: %v", err)
	}
	if l3.Token != 3 {
		t.Fatalf("expected Token 3, got %d", l3.Token)
	}
}

// Acceptance 4:
// fence.Check("job", 2) returns nil, then fence.Check("job", 1) returns ErrStaleToken, and fence.Check("job", 2) again returns nil
func TestAcceptance4_FenceCheck(t *testing.T) {
	var fence distributedlock.Fence
	if err := fence.Check("job", 2); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if err := fence.Check("job", 1); !errors.Is(err, distributedlock.ErrStaleToken) {
		t.Fatalf("expected ErrStaleToken, got %v", err)
	}
	if err := fence.Check("job", 2); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

// Acceptance 5:
// Locker.Acquire with RetryInterval 50*time.Millisecond and an injected sleep retries until the holder releases;
// a cancelled ctx makes it return an error matching context.Canceled via errors.Is
func TestAcceptance5_LockerRetryAndCancellation(t *testing.T) {
	clock := newFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	store := distributedlock.NewMemoryStore(clock.Now, 0)
	ctx := context.Background()

	holderLease, err := store.TryAcquire(ctx, "job", "holder", 10*time.Second)
	if err != nil {
		t.Fatalf("initial acquire failed: %v", err)
	}

	sleepCount := 0
	injectedSleep := func(sCtx context.Context, d time.Duration) error {
		sleepCount++
		if sleepCount == 2 {
			_ = store.Release(sCtx, holderLease)
		}
		return nil
	}

	locker := distributedlock.Locker{
		Store:         store,
		RetryInterval: 50 * time.Millisecond,
		Sleep:         injectedSleep,
	}

	l, err := locker.Acquire(ctx, "job", "waiter", 10*time.Second)
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	if l.Owner != "waiter" {
		t.Fatalf("expected owner waiter, got %s", l.Owner)
	}
	if sleepCount != 2 {
		t.Fatalf("expected 2 sleep iterations, got %d", sleepCount)
	}

	// Test cancellation
	cancellingSleep := func(sCtx context.Context, d time.Duration) error {
		return sCtx.Err()
	}
	cancellingLocker := distributedlock.Locker{
		Store:         store,
		RetryInterval: 50 * time.Millisecond,
		Sleep:         cancellingSleep,
	}

	cancellingCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = cancellingLocker.Acquire(cancellingCtx, "job", "other", 10*time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

// Acceptance 6:
// 100 goroutines calling TryAcquire on "job" with distinct owners produce exactly 1 success and 99 ErrHeld under go test -race
func TestAcceptance6_RaceConcurrency(t *testing.T) {
	clock := newFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	store := distributedlock.NewMemoryStore(clock.Now, 0)
	ctx := context.Background()

	const count = 100
	var successCount int64
	var heldCount int64

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(count)

	for i := 0; i < count; i++ {
		owner := fmt.Sprintf("owner-%d", i)
		go func(o string) {
			defer wg.Done()
			<-start
			_, err := store.TryAcquire(ctx, "job", o, 10*time.Second)
			if err == nil {
				atomic.AddInt64(&successCount, 1)
			} else if errors.Is(err, distributedlock.ErrHeld) {
				atomic.AddInt64(&heldCount, 1)
			}
		}(owner)
	}

	close(start)
	wg.Wait()

	if successCount != 1 {
		t.Fatalf("expected exactly 1 success, got %d", successCount)
	}
	if heldCount != 99 {
		t.Fatalf("expected exactly 99 ErrHeld, got %d", heldCount)
	}
}

// Acceptance 7:
// Key "" or a 257-byte key returns ErrInvalidKey, ttl 0 returns ErrInvalidTTL,
// and with NewMemoryStore(clock, 2) a third distinct held key returns ErrTooManyKeys unless an expired lease can be purged first
func TestAcceptance7_ValidationAndCapacity(t *testing.T) {
	clock := newFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	ctx := context.Background()

	store := distributedlock.NewMemoryStore(clock.Now, 0)

	if _, err := store.TryAcquire(ctx, "", "owner", 10*time.Second); !errors.Is(err, distributedlock.ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey for empty key, got %v", err)
	}

	longKey := strings.Repeat("k", 257)
	if _, err := store.TryAcquire(ctx, longKey, "owner", 10*time.Second); !errors.Is(err, distributedlock.ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey for 257-byte key, got %v", err)
	}

	if _, err := store.TryAcquire(ctx, "job", "owner", 0); !errors.Is(err, distributedlock.ErrInvalidTTL) {
		t.Fatalf("expected ErrInvalidTTL for ttl 0, got %v", err)
	}

	// Test capacity limit with NewMemoryStore(clock.Now, 2)
	limitedStore := distributedlock.NewMemoryStore(clock.Now, 2)
	if _, err := limitedStore.TryAcquire(ctx, "k1", "owner-a", 10*time.Second); err != nil {
		t.Fatalf("k1 acquire failed: %v", err)
	}
	if _, err := limitedStore.TryAcquire(ctx, "k2", "owner-b", 10*time.Second); err != nil {
		t.Fatalf("k2 acquire failed: %v", err)
	}

	// Third distinct held key must return ErrTooManyKeys
	if _, err := limitedStore.TryAcquire(ctx, "k3", "owner-c", 10*time.Second); !errors.Is(err, distributedlock.ErrTooManyKeys) {
		t.Fatalf("expected ErrTooManyKeys for 3rd key, got %v", err)
	}

	// Advance clock so k1 and k2 expire
	clock.Advance(10 * time.Second)

	// Now k3 can be acquired because expired keys are purged first
	if _, err := limitedStore.TryAcquire(ctx, "k3", "owner-c", 10*time.Second); err != nil {
		t.Fatalf("expected k3 acquire to succeed after purging, got %v", err)
	}
}

func TestMemoryStore_EdgeCases(t *testing.T) {
	ctx := context.Background()

	t.Run("defaults for NewMemoryStore", func(t *testing.T) {
		s := distributedlock.NewMemoryStore(nil, 0)
		l, err := s.TryAcquire(ctx, "k", "o", 1*time.Second)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if l.Token != 1 {
			t.Fatalf("expected token 1, got %d", l.Token)
		}
	})

	t.Run("cancelled context returns error immediately", func(t *testing.T) {
		s := distributedlock.NewMemoryStore(nil, 10)
		cancellingCtx, cancel := context.WithCancel(context.Background())
		cancel()

		if _, err := s.TryAcquire(cancellingCtx, "k", "o", time.Second); !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
		if _, err := s.Renew(cancellingCtx, distributedlock.Lease{Key: "k", Owner: "o"}, time.Second); !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
		if err := s.Release(cancellingCtx, distributedlock.Lease{Key: "k", Owner: "o"}); !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	})

	t.Run("owner validation", func(t *testing.T) {
		s := distributedlock.NewMemoryStore(nil, 10)
		if _, err := s.TryAcquire(ctx, "k", "", time.Second); !errors.Is(err, distributedlock.ErrInvalidKey) {
			t.Fatalf("expected ErrInvalidKey for empty owner, got %v", err)
		}
		if _, err := s.TryAcquire(ctx, "k", strings.Repeat("o", 257), time.Second); !errors.Is(err, distributedlock.ErrInvalidKey) {
			t.Fatalf("expected ErrInvalidKey for 257-byte owner, got %v", err)
		}
		// Valid boundary lengths (1 and 256 bytes)
		if _, err := s.TryAcquire(ctx, "k", strings.Repeat("o", 256), time.Second); err != nil {
			t.Fatalf("expected success for 256-byte owner, got %v", err)
		}
	})

	t.Run("negative ttl", func(t *testing.T) {
		s := distributedlock.NewMemoryStore(nil, 10)
		if _, err := s.TryAcquire(ctx, "k", "o", -5*time.Second); !errors.Is(err, distributedlock.ErrInvalidTTL) {
			t.Fatalf("expected ErrInvalidTTL for negative ttl, got %v", err)
		}
		l, _ := s.TryAcquire(ctx, "k", "o", time.Second)
		if _, err := s.Renew(ctx, l, -1*time.Second); !errors.Is(err, distributedlock.ErrInvalidTTL) {
			t.Fatalf("expected ErrInvalidTTL on renew, got %v", err)
		}
	})

	t.Run("no reentrancy for same owner", func(t *testing.T) {
		s := distributedlock.NewMemoryStore(nil, 10)
		_, err := s.TryAcquire(ctx, "job", "same-owner", 10*time.Second)
		if err != nil {
			t.Fatalf("initial acquire failed: %v", err)
		}
		_, err = s.TryAcquire(ctx, "job", "same-owner", 10*time.Second)
		if !errors.Is(err, distributedlock.ErrHeld) {
			t.Fatalf("expected ErrHeld for re-acquisition by same owner, got %v", err)
		}
	})

	t.Run("renew edge cases", func(t *testing.T) {
		clock := newFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
		s := distributedlock.NewMemoryStore(clock.Now, 10)
		// Renew non-existent key
		if _, err := s.Renew(ctx, distributedlock.Lease{Key: "missing", Owner: "o", Token: 1}, time.Second); !errors.Is(err, distributedlock.ErrExpired) {
			t.Fatalf("expected ErrExpired for missing key, got %v", err)
		}
		// Renew with invalid key
		if _, err := s.Renew(ctx, distributedlock.Lease{Key: "", Owner: "o", Token: 1}, time.Second); !errors.Is(err, distributedlock.ErrInvalidKey) {
			t.Fatalf("expected ErrInvalidKey, got %v", err)
		}
		// Renew with different TTL
		l, _ := s.TryAcquire(ctx, "k", "o", 10*time.Second)
		clock.Advance(2 * time.Second)
		ren, err := s.Renew(ctx, l, 20*time.Second)
		if err != nil {
			t.Fatalf("renew with different ttl failed: %v", err)
		}
		wantExp := clock.Now().Add(20 * time.Second).UTC()
		if !ren.ExpiresAt.Equal(wantExp) {
			t.Fatalf("expected %v, got %v", wantExp, ren.ExpiresAt)
		}
		// Renew with wrong owner on active lease
		wrongOwner := ren
		wrongOwner.Owner = "other"
		if _, err := s.Renew(ctx, wrongOwner, 10*time.Second); !errors.Is(err, distributedlock.ErrNotOwner) {
			t.Fatalf("expected ErrNotOwner, got %v", err)
		}
	})

	t.Run("release edge cases and idempotence", func(t *testing.T) {
		clock := newFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
		s := distributedlock.NewMemoryStore(clock.Now, 10)

		// Release non-existent key returns nil (idempotent)
		if err := s.Release(ctx, distributedlock.Lease{Key: "missing", Owner: "o", Token: 1}); err != nil {
			t.Fatalf("expected nil for nonexistent key release, got %v", err)
		}
		// Release with invalid key returns ErrInvalidKey
		if err := s.Release(ctx, distributedlock.Lease{Key: "", Owner: "o", Token: 1}); !errors.Is(err, distributedlock.ErrInvalidKey) {
			t.Fatalf("expected ErrInvalidKey, got %v", err)
		}

		// Acquire, let expire, then release: returns nil when nobody else holds it
		l, _ := s.TryAcquire(ctx, "k", "o", 10*time.Second)
		clock.Advance(10 * time.Second)
		if err := s.Release(ctx, l); err != nil {
			t.Fatalf("expected nil when releasing expired lease, got %v", err)
		}
		// Releasing a second time is idempotent
		if err := s.Release(ctx, l); err != nil {
			t.Fatalf("expected nil on second release, got %v", err)
		}
	})

	t.Run("clock moving backwards keeps lease valid", func(t *testing.T) {
		clock := newFakeClock(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC))
		s := distributedlock.NewMemoryStore(clock.Now, 10)
		l, err := s.TryAcquire(ctx, "k", "o", 10*time.Second)
		if err != nil {
			t.Fatalf("acquire failed: %v", err)
		}
		// Move clock backwards by 1 hour
		clock.Advance(-1 * time.Hour)
		// TryAcquire by another owner still returns ErrHeld (lease looks longer)
		if _, err := s.TryAcquire(ctx, "k", "other", 10*time.Second); !errors.Is(err, distributedlock.ErrHeld) {
			t.Fatalf("expected ErrHeld when clock moved backwards, got %v", err)
		}
		// Renew still succeeds
		if _, err := s.Renew(ctx, l, 10*time.Second); err != nil {
			t.Fatalf("renew after clock jump failed: %v", err)
		}
	})
}

func TestLocker_EdgeCases(t *testing.T) {
	ctx := context.Background()

	t.Run("context cancelled before acquire", func(t *testing.T) {
		s := distributedlock.NewMemoryStore(nil, 10)
		l := distributedlock.Locker{Store: s}
		cancellingCtx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := l.Acquire(cancellingCtx, "k", "o", time.Second); !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	})

	t.Run("non-ErrHeld error returned immediately", func(t *testing.T) {
		s := distributedlock.NewMemoryStore(nil, 10)
		l := distributedlock.Locker{Store: s}
		if _, err := l.Acquire(ctx, "", "o", time.Second); !errors.Is(err, distributedlock.ErrInvalidKey) {
			t.Fatalf("expected ErrInvalidKey, got %v", err)
		}
	})

	t.Run("default sleep and default retry interval", func(t *testing.T) {
		s := distributedlock.NewMemoryStore(nil, 10)
		// Acquire lock first
		l1, _ := s.TryAcquire(ctx, "k", "o1", 50*time.Millisecond)
		l := distributedlock.Locker{Store: s} // RetryInterval <= 0 and Sleep == nil

		go func() {
			time.Sleep(20 * time.Millisecond)
			_ = s.Release(ctx, l1)
		}()

		lease, err := l.Acquire(ctx, "k", "o2", 50*time.Millisecond)
		if err != nil {
			t.Fatalf("acquire failed: %v", err)
		}
		if lease.Owner != "o2" {
			t.Fatalf("expected owner o2, got %s", lease.Owner)
		}
	})

	t.Run("default sleep cancelled by context", func(t *testing.T) {
		s := distributedlock.NewMemoryStore(nil, 10)
		_, _ = s.TryAcquire(ctx, "k", "o1", 10*time.Second)
		cancellingCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()

		l := distributedlock.Locker{Store: s, RetryInterval: 100 * time.Millisecond}
		_, err := l.Acquire(cancellingCtx, "k", "o2", time.Second)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected DeadlineExceeded, got %v", err)
		}
	})
}

func TestFence_LRUAndLimits(t *testing.T) {
	t.Run("invalid key validation", func(t *testing.T) {
		var fence distributedlock.Fence
		if err := fence.Check("", 1); !errors.Is(err, distributedlock.ErrInvalidKey) {
			t.Fatalf("expected ErrInvalidKey for empty key, got %v", err)
		}
		if err := fence.Check(strings.Repeat("k", 257), 1); !errors.Is(err, distributedlock.ErrInvalidKey) {
			t.Fatalf("expected ErrInvalidKey for 257-byte key, got %v", err)
		}
	})

	t.Run("lru eviction when max keys reached", func(t *testing.T) {
		fence := distributedlock.Fence{MaxKeys: 2}
		if err := fence.Check("k1", 10); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := fence.Check("k2", 20); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Access k1 to make k2 the least recently checked
		if err := fence.Check("k1", 11); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Insert k3 -> should evict k2
		if err := fence.Check("k3", 30); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// k1 should still have token 11
		if err := fence.Check("k1", 5); !errors.Is(err, distributedlock.ErrStaleToken) {
			t.Fatalf("expected ErrStaleToken for k1 token 5, got %v", err)
		}

		// k2 was evicted, so checking it with token 5 treats it as a new key
		if err := fence.Check("k2", 5); err != nil {
			t.Fatalf("expected nil for newly inserted evicted key, got %v", err)
		}
	})

	t.Run("concurrent checks under race detector", func(t *testing.T) {
		var fence distributedlock.Fence
		var wg sync.WaitGroup
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				key := fmt.Sprintf("key-%d", id%5)
				_ = fence.Check(key, uint64(id))
			}(i)
		}
		wg.Wait()
	})
}

func ExampleMemoryStore() {
	store := distributedlock.NewMemoryStore(nil, 0)
	ctx := context.Background()

	lease, err := store.TryAcquire(ctx, "task-lock", "worker-1", 10*time.Second)
	if err != nil {
		fmt.Println("acquire error:", err)
		return
	}
	fmt.Printf("acquired: key=%s token=%d\n", lease.Key, lease.Token)

	ren, err := store.Renew(ctx, lease, 10*time.Second)
	if err != nil {
		fmt.Println("renew error:", err)
		return
	}
	fmt.Printf("renewed: token=%d\n", ren.Token)

	if err := store.Release(ctx, ren); err != nil {
		fmt.Println("release error:", err)
		return
	}
	fmt.Println("released successfully")

	// Output:
	// acquired: key=task-lock token=1
	// renewed: token=1
	// released successfully
}

func ExampleFence() {
	var fence distributedlock.Fence

	err := fence.Check("resource-a", 10)
	fmt.Println("check 10:", err)

	err = fence.Check("resource-a", 5)
	fmt.Println("check 5:", err)

	err = fence.Check("resource-a", 10)
	fmt.Println("check 10 again:", err)

	// Output:
	// check 10: <nil>
	// check 5: distributedlock: stale fencing token
	// check 10 again: <nil>
}
