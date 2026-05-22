package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	processedJobPrefix = "processed_job:"
	rateLimitPrefix    = "rate_limit:"
)

type RedisCache struct {
	client *redis.Client
}

// --------------------------------------------------------------------
// Constructor
// --------------------------------------------------------------------

func NewRedisCache(addr string) (*RedisCache, error) {

	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     "",
		DB:           0,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolTimeout:  4 * time.Second,
	})

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf(
			"failed to connect to redis: %w",
			err,
		)
	}

	return &RedisCache{
		client: rdb,
	}, nil
}

// --------------------------------------------------------------------
// Idempotency Lock
// --------------------------------------------------------------------

// TryClaimJob attempts to atomically claim a job.
//
// Returns:
//   - true  -> this worker successfully claimed the job
//   - false -> another worker already claimed it
//
// This prevents duplicate email sends across concurrent workers.
func (r *RedisCache) TryClaimJob(
	ctx context.Context,
	jobID string,
) (bool, error) {

	key := fmt.Sprintf(
		"%s%s",
		processedJobPrefix,
		jobID,
	)

	// SETNX = SET if Not eXists
	success, err := r.client.SetNX(
		ctx,
		key,
		"1",
		24*time.Hour,
	).Result()

	if err != nil {
		return false, fmt.Errorf(
			"redis SETNX failed: %w",
			err,
		)
	}

	return success, nil
}

// ReleaseJobClaim removes an idempotency key so Kafka can redeliver and retry.
// Call only when processing did not complete (no offset commit).
func (r *RedisCache) ReleaseJobClaim(ctx context.Context, jobID string) error {
	key := fmt.Sprintf("%s%s", processedJobPrefix, jobID)
	if err := r.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("redis DEL failed: %w", err)
	}
	return nil
}

// --------------------------------------------------------------------
// Fixed Window Rate Limiter
// --------------------------------------------------------------------

// AllowRequest enforces a global per-second limit.
func (r *RedisCache) AllowRequest(
	ctx context.Context,
	maxPerSecond int64,
) (bool, error) {

	currentSecond := time.Now().UTC().Unix()

	key := fmt.Sprintf(
		"%s%d",
		rateLimitPrefix,
		currentSecond,
	)

	pipe := r.client.TxPipeline()

	incr := pipe.Incr(ctx, key)

	pipe.Expire(
		ctx,
		key,
		2*time.Second,
	)

	_, err := pipe.Exec(ctx)

	if err != nil {
		return false, fmt.Errorf(
			"redis pipeline failed: %w",
			err,
		)
	}

	if incr.Val() > maxPerSecond {
		return false, nil
	}

	return true, nil
}

// --------------------------------------------------------------------
// Cleanup
// --------------------------------------------------------------------

func (r *RedisCache) Close() error {
	return r.client.Close()
}
