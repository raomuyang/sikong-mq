// A smooth rate limiter base in the token bucket algorithm

package ratelimiter

import (
	"time"
	"errors"
	"math"
	"sync"
)

type RateLimiter interface {
	SetRate(permitsPerSecond int)

	GetRate() int

	Acquire(permits int) int64

	TryAcquire(permits int, timeout time.Duration) bool

}

type TokenBucketLimiter struct {
	burstInterval  float64
	storedPermits  float64
	maxPermits     float64
	nextFreeTicket int64
	stopwatch      *Stopwatch
	mutex          sync.Mutex
}

func (limiter *TokenBucketLimiter) SetRate(permitsPerSeconds float64) {
	limiter.resync(limiter.stopwatch.ElapsedNanos())
	limiter.doSetRate(permitsPerSeconds, time.Now().UnixNano())

}

func (limiter TokenBucketLimiter) GetRate() float64 {
	return float64(time.Second) / limiter.burstInterval
}

func (limiter *TokenBucketLimiter) Acquire(permits int) (int64, error) {
	if permits < 0 {
		return 0, errors.New("requested permits must be positive")
	}

	availableTime := limiter.reserve(permits)
	sleepTime := availableTime - limiter.stopwatch.ElapsedNanos()
	if sleepTime < 0 {
		sleepTime = 0
	}
	time.Sleep(time.Duration(sleepTime))
	return sleepTime, nil
}

func (limiter *TokenBucketLimiter) TryAcquire(permits int, timeout time.Duration) (bool, error) {
	if permits < 0 {
		return false, errors.New("requested permits must be positive")
	}

	availableTime := limiter.reserve(permits)
	sleepTime := time.Duration(availableTime - limiter.stopwatch.ElapsedNanos())
	if sleepTime > timeout {
		return false, nil
	}
	time.Sleep(sleepTime)
	return true, nil
}

func (limiter *TokenBucketLimiter) doSetRate(permitsPerSeconds float64, now int64) {
	limiter.mutex.Lock()
	defer limiter.mutex.Unlock()

	interval := float64(time.Second) / permitsPerSeconds // micro of interval
	limiter.burstInterval = interval

	oldMax := limiter.maxPermits
	limiter.maxPermits = permitsPerSeconds

	if oldMax == math.MaxFloat64 {
		limiter.storedPermits = limiter.maxPermits
	} else {
		limiter.storedPermits = 0.0
		if oldMax != 0.0 {
			limiter.storedPermits = limiter.storedPermits * limiter.maxPermits / oldMax
		}
	}

}

/**
	将nextFreeTicket同步到当前时间
 */
func (limiter *TokenBucketLimiter) resync(now int64) {
	if now > limiter.nextFreeTicket {
		newPermits := float64(now - limiter.nextFreeTicket) / limiter.burstInterval
		limiter.storedPermits = math.Min(newPermits + limiter.storedPermits, limiter.maxPermits)
		limiter.nextFreeTicket = now
	}
}

func (limiter *TokenBucketLimiter) reserve(permits int) int64 {
	limiter.mutex.Lock()
	defer limiter.mutex.Unlock()

	now := limiter.stopwatch.ElapsedNanos()
	limiter.resync(now)

	requiredPermits := float64(permits)
	wait := limiter.nextFreeTicket
	storedWillSpendPermits := math.Min(limiter.storedPermits, requiredPermits)

	freshPermits := float64(permits) - storedWillSpendPermits

	nextTimeWait := int64(freshPermits * limiter.burstInterval)
	limiter.nextFreeTicket += nextTimeWait
	limiter.storedPermits -= storedWillSpendPermits
	return wait
}

func CreateTokenBucket(rate float64) (*TokenBucketLimiter, error) {
	if rate < 0 {
		return nil, errors.New("rate must be positive")
	}
	stopwatch := CreateStartedStopwatch()
	return CustomTokenBucket(rate, stopwatch), nil
}

func CustomTokenBucket(rate float64, stopwatch *Stopwatch) *TokenBucketLimiter {
	if stopwatch == nil {
		stopwatch = CreateStartedStopwatch()
	}
	limiter := &TokenBucketLimiter{stopwatch: stopwatch, storedPermits: 0.0}
	limiter.SetRate(rate)
	return limiter
}


type Stopwatch struct {
	startNanos   int64
	elapsedNanos int64
	isRun        bool
}

func (stopwatch *Stopwatch) Start() error {
	if stopwatch.isRun {
		return errors.New("this stopwatch is already running")
	}
	stopwatch.isRun = true
	stopwatch.startNanos = time.Now().UnixNano()
	return nil
}

func (stopwatch *Stopwatch) Stop() error {
	if !stopwatch.isRun {
		return errors.New("this stopwatch is already stopped")
	}
	stopwatch.isRun = false
	stopwatch.elapsedNanos += time.Now().UnixNano() - stopwatch.startNanos
	return nil
}

func (stopwatch *Stopwatch) ElapsedNanos() int64 {
	if stopwatch.isRun {
		return time.Now().UnixNano() - stopwatch.startNanos
	}
	return stopwatch.elapsedNanos
}

func (stopwatch *Stopwatch) Reset() {
	stopwatch.elapsedNanos = 0
	stopwatch.isRun = false
}

func CreateStartedStopwatch() *Stopwatch {
	stopwatch := Stopwatch{}
	stopwatch.Start()
	return &stopwatch
}