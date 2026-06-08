package handlers

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/tejdeep/gorelay/internal/middleware"
	"github.com/tejdeep/gorelay/internal/models"
	"github.com/tejdeep/gorelay/internal/queue"
	"github.com/tejdeep/gorelay/internal/repository"
)

// ─────────────────────────────────────────────
//  App Handler  (admin — create/manage apps)
// ─────────────────────────────────────────────

type AppHandler struct {
	appRepo *repository.AppRepository
}

func NewAppHandler(appRepo *repository.AppRepository) *AppHandler {
	return &AppHandler{appRepo: appRepo}
}

// POST /admin/apps — create a new application + API key
func (h *AppHandler) Create(c *gin.Context) {
	var req models.CreateAppRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Generate cryptographically random API key (32 bytes = 64 hex chars)
	rawKey, err := generateAPIKey()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not generate API key"})
		return
	}

	app := &models.Application{
		ID:         uuid.New().String(),
		Name:       req.Name,
		APIKey:     rawKey, // returned once, never stored
		APIKeyHash: middleware.HashAPIKey(rawKey),
		IsActive:   true,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}

	if err := h.appRepo.Create(context.Background(), app); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not create application"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"message": "Application created. Save the api_key — it will not be shown again.",
		"app":     app,
	})
}

// ─────────────────────────────────────────────
//  Endpoint Handler
// ─────────────────────────────────────────────

type EndpointHandler struct {
	endpointRepo *repository.EndpointRepository
}

func NewEndpointHandler(endpointRepo *repository.EndpointRepository) *EndpointHandler {
	return &EndpointHandler{endpointRepo: endpointRepo}
}

// POST /api/endpoints
func (h *EndpointHandler) Create(c *gin.Context) {
	var req models.CreateEndpointRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	appID := c.GetString("app_id")

	// Generate a signing secret for HMAC verification
	secret, err := generateAPIKey()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not generate secret"})
		return
	}

	ep := &models.Endpoint{
		ID:            uuid.New().String(),
		AppID:         appID,
		URL:           req.URL,
		Description:   req.Description,
		Secret:        secret,     // returned once
		SecretHash:    secret,     // stored directly (needed for signing, not just verification)
		EnabledEvents: req.EnabledEvents,
		IsActive:      true,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}

	if err := h.endpointRepo.Create(context.Background(), ep); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not create endpoint"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"message":  "Endpoint created. Use the signing_secret to verify incoming webhooks.",
		"endpoint": ep,
	})
}

// GET /api/endpoints
func (h *EndpointHandler) List(c *gin.Context) {
	appID := c.GetString("app_id")
	endpoints, err := h.endpointRepo.ListByApp(context.Background(), appID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not list endpoints"})
		return
	}
	// Zero out secrets in list view
	for _, ep := range endpoints {
		ep.SecretHash = ""
	}
	c.JSON(http.StatusOK, gin.H{"endpoints": endpoints, "count": len(endpoints)})
}

// GET /api/endpoints/:id
func (h *EndpointHandler) Get(c *gin.Context) {
	appID := c.GetString("app_id")
	ep, err := h.endpointRepo.GetByID(context.Background(), c.Param("id"), appID)
	if err != nil {
		if err == repository.ErrNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "endpoint not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "server error"})
		return
	}
	ep.SecretHash = ""
	c.JSON(http.StatusOK, ep)
}

// PATCH /api/endpoints/:id
func (h *EndpointHandler) Update(c *gin.Context) {
	var req models.UpdateEndpointRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	appID := c.GetString("app_id")
	if err := h.endpointRepo.Update(context.Background(), c.Param("id"), appID, &req); err != nil {
		if err == repository.ErrNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "endpoint not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not update"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "endpoint updated"})
}

// DELETE /api/endpoints/:id
func (h *EndpointHandler) Delete(c *gin.Context) {
	appID := c.GetString("app_id")
	if err := h.endpointRepo.Delete(context.Background(), c.Param("id"), appID); err != nil {
		if err == repository.ErrNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "endpoint not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not delete"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "endpoint deleted"})
}

// ─────────────────────────────────────────────
//  Event Handler  (ingest + fan-out)
// ─────────────────────────────────────────────

type EventHandler struct {
	eventRepo    *repository.EventRepository
	endpointRepo *repository.EndpointRepository
	deliveryRepo *repository.DeliveryRepository
	q            *queue.RedisQueue
}

func NewEventHandler(
	eventRepo *repository.EventRepository,
	endpointRepo *repository.EndpointRepository,
	deliveryRepo *repository.DeliveryRepository,
	q *queue.RedisQueue,
) *EventHandler {
	return &EventHandler{eventRepo: eventRepo, endpointRepo: endpointRepo, deliveryRepo: deliveryRepo, q: q}
}

// POST /api/events — ingest a new event and fan out to all matching endpoints.
// This is the core ingest + dispatch path.
func (h *EventHandler) Ingest(c *gin.Context) {
	var req models.CreateEventRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	appID := c.GetString("app_id")
	ctx := context.Background()

	// 1. Persist the event
	event := &models.Event{
		ID:        uuid.New().String(),
		AppID:     appID,
		EventType: req.EventType,
		Payload:   req.Payload,
		Status:    "pending",
		CreatedAt: time.Now(),
	}
	if err := h.eventRepo.Create(ctx, event); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not store event"})
		return
	}

	// 2. Find all active endpoints that subscribe to this event type
	endpoints, err := h.endpointRepo.GetActiveForEvent(ctx, appID, req.EventType)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not find endpoints"})
		return
	}

	if len(endpoints) == 0 {
		_ = h.eventRepo.UpdateStatus(ctx, event.ID, "no_endpoints")
		c.JSON(http.StatusAccepted, gin.H{
			"event_id": event.ID,
			"message":  "event accepted — no active endpoints matched",
			"queued":   0,
		})
		return
	}

	// 3. Create a Delivery record + queue job for EACH matching endpoint (fan-out)
	payloadStr := string(req.Payload)
	queued := 0
	for _, ep := range endpoints {
		delivery := &models.Delivery{
			ID:            uuid.New().String(),
			EventID:       event.ID,
			EndpointID:    ep.ID,
			AppID:         appID,
			AttemptNumber: 1,
			Status:        models.DeliveryPending,
			CreatedAt:     time.Now(),
			UpdatedAt:     time.Now(),
		}
		if err := h.deliveryRepo.Create(ctx, delivery); err != nil {
			continue
		}

		job := &models.DeliveryJob{
			DeliveryID:     delivery.ID,
			EventID:        event.ID,
			EndpointID:     ep.ID,
			AppID:          appID,
			EventType:      req.EventType,
			Payload:        payloadStr,
			EndpointURL:    ep.URL,
			EndpointSecret: ep.SecretHash,
			AttemptNumber:  1,
			EnqueuedAt:     time.Now(),
		}
		if err := h.q.Enqueue(ctx, job); err == nil {
			queued++
		}
	}

	c.JSON(http.StatusAccepted, gin.H{
		"event_id":   event.ID,
		"event_type": req.EventType,
		"queued":     queued,
		"endpoints":  len(endpoints),
		"message":    "event accepted and queued for delivery",
	})
}

// GET /api/events — list events for the authenticated app
func (h *EventHandler) List(c *gin.Context) {
	appID := c.GetString("app_id")
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))
	if limit > 200 {
		limit = 200
	}

	events, total, err := h.eventRepo.ListByApp(context.Background(), appID, limit, offset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not list events"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"events": events, "total": total, "limit": limit, "offset": offset})
}

// GET /api/events/:id/deliveries — delivery attempts for a specific event
func (h *EventHandler) GetDeliveries(c *gin.Context) {
	deliveries, err := h.deliveryRepo.ListByEvent(context.Background(), c.Param("id"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not list deliveries"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"deliveries": deliveries, "count": len(deliveries)})
}

// ─────────────────────────────────────────────
//  Metrics Handler
// ─────────────────────────────────────────────

type MetricsHandler struct {
	deliveryRepo *repository.DeliveryRepository
	eventRepo    *repository.EventRepository
	q            *queue.RedisQueue
}

func NewMetricsHandler(deliveryRepo *repository.DeliveryRepository, eventRepo *repository.EventRepository, q *queue.RedisQueue) *MetricsHandler {
	return &MetricsHandler{deliveryRepo: deliveryRepo, eventRepo: eventRepo, q: q}
}

// GET /api/metrics — system-wide health and throughput stats
func (h *MetricsHandler) Get(c *gin.Context) {
	ctx := context.Background()

	byStatus, err := h.deliveryRepo.GetMetrics(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not fetch metrics"})
		return
	}

	qDepth, _ := h.q.Depth(ctx)
	dlqDepth, _ := h.q.DLQDepth(ctx)

	var total, success int64
	for status, count := range byStatus {
		total += count
		if status == "success" {
			success = count
		}
	}

	var successRate float64
	if total > 0 {
		successRate = float64(success) / float64(total) * 100
	}

	c.JSON(http.StatusOK, models.MetricsResponse{
		TotalDeliveries: total,
		SuccessRate:     successRate,
		PendingQueue:    qDepth,
		DLQDepth:        dlqDepth,
		ByStatus:        byStatus,
	})
}

// GET /health
func HealthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":    "ok",
		"service":   "gorelay",
		"timestamp": time.Now().UTC(),
	})
}

// ── helpers ────────────────────────────────────────────────────────────────

func generateAPIKey() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// Unused import guard
var _ = json.Marshal
