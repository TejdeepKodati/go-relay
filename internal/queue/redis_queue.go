package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tejdeep/gorelay/internal/models"
)

// RedisQueue wraps Redis LIST operations for reliable delivery queuing.
// Enqueue pushes jobs to the tail (RPUSH).
// Dequeue blocks on the head (BLPOP) — zero idle CPU when queue is empty.
// DLQ is a separate list for permanently failed jobs.
type RedisQueue struct {
	rdb       *redis.Client
	queueName string
	dlqName   string
}

func NewRedisQueue(rdb *redis.Client, queueName, dlqName string) *RedisQueue {
	return &RedisQueue{rdb: rdb, queueName: queueName, dlqName: dlqName}
}

// Enqueue serialises a DeliveryJob and pushes it onto the tail of the queue.
func (q *RedisQueue) Enqueue(ctx context.Context, job *models.DeliveryJob) error {
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}
	return q.rdb.RPush(ctx, q.queueName, data).Err()
}

// EnqueueDelayed pushes to a delayed retry key that expires and re-appears.
// We use a Sorted Set where the score is the Unix timestamp to deliver.
func (q *RedisQueue) EnqueueDelayed(ctx context.Context, job *models.DeliveryJob, delay time.Duration) error {
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal delayed job: %w", err)
	}
	score := float64(time.Now().Add(delay).Unix())
	return q.rdb.ZAdd(ctx, q.queueName+":delayed", redis.Z{
		Score:  score,
		Member: data,
	}).Err()
}

// Dequeue blocks up to `timeout` waiting for a job from the queue.
// Returns (nil, nil) if the timeout elapsed without a message.
func (q *RedisQueue) Dequeue(ctx context.Context, timeout time.Duration) (*models.DeliveryJob, error) {
	// BLPOP returns [queueName, value]
	result, err := q.rdb.BLPop(ctx, timeout, q.queueName).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil // timeout — no message
		}
		return nil, fmt.Errorf("blpop: %w", err)
	}
	if len(result) < 2 {
		return nil, nil
	}

	var job models.DeliveryJob
	if err := json.Unmarshal([]byte(result[1]), &job); err != nil {
		return nil, fmt.Errorf("unmarshal job: %w", err)
	}
	return &job, nil
}

// MoveToDelayedReady scans the delayed sorted set and moves jobs whose
// score (delivery timestamp) <= now into the main queue. Call this in a
// background goroutine on a ticker.
func (q *RedisQueue) MoveToDelayedReady(ctx context.Context) (int64, error) {
	now := float64(time.Now().Unix())

	// Atomically pop members with score <= now
	members, err := q.rdb.ZRangeByScoreWithScores(ctx, q.queueName+":delayed", &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%f", now),
	}).Result()
	if err != nil {
		return 0, err
	}

	pipe := q.rdb.Pipeline()
	for _, m := range members {
		pipe.ZRem(ctx, q.queueName+":delayed", m.Member)
		pipe.RPush(ctx, q.queueName, m.Member)
	}
	if len(members) > 0 {
		if _, err := pipe.Exec(ctx); err != nil {
			return 0, err
		}
	}
	return int64(len(members)), nil
}

// EnqueueDLQ pushes a permanently failed job to the dead-letter queue.
func (q *RedisQueue) EnqueueDLQ(ctx context.Context, job *models.DeliveryJob) error {
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}
	return q.rdb.RPush(ctx, q.dlqName, data).Err()
}

// Depth returns the number of jobs waiting in the main queue.
func (q *RedisQueue) Depth(ctx context.Context) (int64, error) {
	return q.rdb.LLen(ctx, q.queueName).Result()
}

// DLQDepth returns the number of permanently failed jobs.
func (q *RedisQueue) DLQDepth(ctx context.Context) (int64, error) {
	return q.rdb.LLen(ctx, q.dlqName).Result()
}
