package worker

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/tejdeep/gorelay/internal/models"
	"github.com/tejdeep/gorelay/internal/queue"
	"github.com/tejdeep/gorelay/internal/repository"
)

// Pool manages N concurrent delivery goroutines and one delayed-job promoter.
// Each worker calls Dequeue (blocking) → attempts HTTP delivery → handles retry/DLQ.
type Pool struct {
	workerCount int
	maxRetries  int
	httpTimeout time.Duration

	q            *queue.RedisQueue
	deliveryRepo *repository.DeliveryRepository
	eventRepo    *repository.EventRepository

	httpClient *http.Client
	wg         sync.WaitGroup
	stopCh     chan struct{}
}

func NewPool(
	workerCount, maxRetries int,
	httpTimeoutSec int,
	q *queue.RedisQueue,
	deliveryRepo *repository.DeliveryRepository,
	eventRepo *repository.EventRepository,
) *Pool {
	timeout := time.Duration(httpTimeoutSec) * time.Second
	return &Pool{
		workerCount:  workerCount,
		maxRetries:   maxRetries,
		httpTimeout:  timeout,
		q:            q,
		deliveryRepo: deliveryRepo,
		eventRepo:    eventRepo,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		stopCh: make(chan struct{}),
	}
}

// Start launches all worker goroutines and the delayed-job promoter.
func (p *Pool) Start(ctx context.Context) {
	log.Printf("🔧 Worker pool starting: %d workers, maxRetries=%d", p.workerCount, p.maxRetries)

	// Launch N delivery workers
	for i := 0; i < p.workerCount; i++ {
		p.wg.Add(1)
		go p.runWorker(ctx, i)
	}

	// Launch delayed-job promoter (runs every 5 seconds)
	p.wg.Add(1)
	go p.runPromoter(ctx)

	log.Printf("✓ Worker pool running")
}

// Stop signals workers to stop and waits for them to drain.
func (p *Pool) Stop() {
	log.Println("Worker pool stopping...")
	close(p.stopCh)
	p.wg.Wait()
	log.Println("Worker pool stopped")
}

// runWorker is the main loop for one delivery worker goroutine.
func (p *Pool) runWorker(ctx context.Context, id int) {
	defer p.wg.Done()
	log.Printf("Worker %d started", id)

	for {
		select {
		case <-p.stopCh:
			log.Printf("Worker %d stopping", id)
			return
		default:
		}

		// Block up to 2s waiting for a job. Short timeout so we can check stopCh.
		job, err := p.q.Dequeue(ctx, 2*time.Second)
		if err != nil {
			log.Printf("Worker %d dequeue error: %v", id, err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if job == nil {
			continue // timeout, loop back and check stopCh
		}

		p.deliver(ctx, job)
	}
}

// deliver attempts one HTTP delivery, then handles success / retry / DLQ.
func (p *Pool) deliver(ctx context.Context, job *models.DeliveryJob) {
	log.Printf("Delivering job %s → %s (attempt %d)", job.DeliveryID, job.EndpointURL, job.AttemptNumber)

	statusCode, body, err := p.httpPost(job)

	if err == nil && statusCode >= 200 && statusCode < 300 {
		// ── SUCCESS ──────────────────────────────────────────────────────
		if dbErr := p.deliveryRepo.MarkSuccess(ctx, job.DeliveryID, statusCode, body); dbErr != nil {
			log.Printf("MarkSuccess DB error: %v", dbErr)
		}
		_ = p.eventRepo.UpdateStatus(ctx, job.EventID, "delivered")
		log.Printf("✓ Delivered %s (HTTP %d)", job.DeliveryID, statusCode)
		return
	}

	// ── FAILURE ──────────────────────────────────────────────────────────
	errMsg := fmt.Sprintf("HTTP %d: %s", statusCode, body)
	if err != nil {
		errMsg = err.Error()
	}

	nextAttempt := job.AttemptNumber + 1
	isDLQ := nextAttempt > p.maxRetries

	var nextRetryAt *time.Time
	var httpStatusPtr *int

	if statusCode > 0 {
		httpStatusPtr = &statusCode
	}

	if isDLQ {
		// ── DEAD LETTER QUEUE ─────────────────────────────────────────────
		log.Printf("✗ DLQ %s after %d attempts: %s", job.DeliveryID, job.AttemptNumber, errMsg)
		_ = p.deliveryRepo.MarkFailed(ctx, job.DeliveryID, errMsg, httpStatusPtr, nil, true)
		_ = p.eventRepo.UpdateStatus(ctx, job.EventID, "failed")
		_ = p.q.EnqueueDLQ(ctx, job)
	} else {
		// ── RETRY WITH EXPONENTIAL BACKOFF ────────────────────────────────
		// Delays: attempt 1→5s, 2→30s, 3→5min, 4→30min (capped at 6h)
		delay := retryDelay(nextAttempt)
		retryAt := time.Now().Add(delay)
		nextRetryAt = &retryAt

		log.Printf("↻ Retry %s in %v (attempt %d/%d)", job.DeliveryID, delay, nextAttempt, p.maxRetries)
		_ = p.deliveryRepo.MarkFailed(ctx, job.DeliveryID, errMsg, httpStatusPtr, nextRetryAt, false)

		// Create next delivery record and enqueue after delay
		nextJob := *job
		nextJob.AttemptNumber = nextAttempt
		_ = p.q.EnqueueDelayed(ctx, &nextJob, delay)
	}
}

// httpPost sends the signed webhook HTTP POST and returns (statusCode, body, error).
func (p *Pool) httpPost(job *models.DeliveryJob) (int, string, error) {
	payload := []byte(job.Payload)

	req, err := http.NewRequest(http.MethodPost, job.EndpointURL, bytes.NewReader(payload))
	if err != nil {
		return 0, "", fmt.Errorf("build request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "GoRelay/1.0")
	req.Header.Set("X-GoRelay-Event", job.EventType)
	req.Header.Set("X-GoRelay-Delivery", job.DeliveryID)
	req.Header.Set("X-GoRelay-Timestamp", fmt.Sprintf("%d", time.Now().Unix()))

	// HMAC-SHA256 signature  ─  verifiable by the endpoint
	if job.EndpointSecret != "" {
		sig := computeHMAC(payload, job.EndpointSecret)
		req.Header.Set("X-GoRelay-Signature", "sha256="+sig)
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return 0, "", fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return resp.StatusCode, string(bodyBytes), nil
}

// runPromoter ticks every 5s and moves delayed jobs whose time has come
// into the main queue.
func (p *Pool) runPromoter(ctx context.Context) {
	defer p.wg.Done()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			n, err := p.q.MoveToDelayedReady(ctx)
			if err != nil {
				log.Printf("Promoter error: %v", err)
			} else if n > 0 {
				log.Printf("Promoter: moved %d delayed jobs to ready queue", n)
			}
		}
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

// retryDelay returns the backoff duration for a given attempt number.
// Formula: min(5s * 6^(attempt-1), 6h)
func retryDelay(attempt int) time.Duration {
	seconds := 5.0 * math.Pow(6, float64(attempt-1))
	maxSeconds := 6.0 * 3600.0
	if seconds > maxSeconds {
		seconds = maxSeconds
	}
	return time.Duration(seconds) * time.Second
}

// computeHMAC returns the hex-encoded HMAC-SHA256 of payload using secret.
func computeHMAC(payload []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)
	return fmt.Sprintf("%x", mac.Sum(nil))
}
