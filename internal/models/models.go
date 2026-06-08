package models

import (
	"encoding/json"
	"time"
)

// ─────────────────────────────────────────────
//  Application (tenant / API key owner)
// ─────────────────────────────────────────────

type Application struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	APIKey    string    `json:"api_key"`    // shown only at creation
	APIKeyHash string   `json:"-"`          // stored in DB
	IsActive  bool      `json:"is_active"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type CreateAppRequest struct {
	Name string `json:"name" binding:"required,min=2"`
}

// ─────────────────────────────────────────────
//  Endpoint (a webhook target URL)
// ─────────────────────────────────────────────

type Endpoint struct {
	ID            string    `json:"id"`
	AppID         string    `json:"app_id"`
	URL           string    `json:"url"`
	Description   string    `json:"description"`
	Secret        string    `json:"secret,omitempty"` // HMAC signing secret (show at creation only)
	SecretHash    string    `json:"-"`
	EnabledEvents []string  `json:"enabled_events"` // ["order.created","payment.failed"] or ["*"]
	IsActive      bool      `json:"is_active"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type CreateEndpointRequest struct {
	URL           string   `json:"url"            binding:"required,url"`
	Description   string   `json:"description"`
	EnabledEvents []string `json:"enabled_events" binding:"required,min=1"`
}

type UpdateEndpointRequest struct {
	URL           string   `json:"url,omitempty"`
	Description   string   `json:"description,omitempty"`
	EnabledEvents []string `json:"enabled_events,omitempty"`
	IsActive      *bool    `json:"is_active,omitempty"`
}

// ─────────────────────────────────────────────
//  Event (inbound payload from API client)
// ─────────────────────────────────────────────

type Event struct {
	ID        string          `json:"id"`
	AppID     string          `json:"app_id"`
	EventType string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload"`
	Status    string          `json:"status"` // pending | delivered | failed
	CreatedAt time.Time       `json:"created_at"`
}

type CreateEventRequest struct {
	EventType string          `json:"event_type" binding:"required"`
	Payload   json.RawMessage `json:"payload"    binding:"required"`
}

// ─────────────────────────────────────────────
//  Delivery (one attempt to push event → endpoint)
// ─────────────────────────────────────────────

type DeliveryStatus string

const (
	DeliveryPending   DeliveryStatus = "pending"
	DeliverySuccess   DeliveryStatus = "success"
	DeliveryFailed    DeliveryStatus = "failed"
	DeliveryDLQ       DeliveryStatus = "dlq" // max retries exceeded
)

type Delivery struct {
	ID             string         `json:"id"`
	EventID        string         `json:"event_id"`
	EndpointID     string         `json:"endpoint_id"`
	AppID          string         `json:"app_id"`
	AttemptNumber  int            `json:"attempt_number"`
	Status         DeliveryStatus `json:"status"`
	HTTPStatusCode *int           `json:"http_status_code,omitempty"`
	ResponseBody   string         `json:"response_body,omitempty"`
	ErrorMessage   string         `json:"error_message,omitempty"`
	NextRetryAt    *time.Time     `json:"next_retry_at,omitempty"`
	DeliveredAt    *time.Time     `json:"delivered_at,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
}

// ─────────────────────────────────────────────
//  Queue job (serialized into Redis)
// ─────────────────────────────────────────────

type DeliveryJob struct {
	DeliveryID    string    `json:"delivery_id"`
	EventID       string    `json:"event_id"`
	EndpointID    string    `json:"endpoint_id"`
	AppID         string    `json:"app_id"`
	EventType     string    `json:"event_type"`
	Payload       string    `json:"payload"` // JSON string
	EndpointURL   string    `json:"endpoint_url"`
	EndpointSecret string   `json:"endpoint_secret"`
	AttemptNumber int       `json:"attempt_number"`
	EnqueuedAt    time.Time `json:"enqueued_at"`
}

// ─────────────────────────────────────────────
//  Generic responses
// ─────────────────────────────────────────────

type MetricsResponse struct {
	TotalEvents      int64            `json:"total_events"`
	TotalDeliveries  int64            `json:"total_deliveries"`
	SuccessRate      float64          `json:"success_rate_percent"`
	PendingQueue     int64            `json:"pending_queue_depth"`
	DLQDepth         int64            `json:"dlq_depth"`
	ByStatus         map[string]int64 `json:"deliveries_by_status"`
	AvgResponseTimeMs float64         `json:"avg_response_time_ms"`
}
