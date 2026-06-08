package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tejdeep/gorelay/internal/models"
)

var ErrNotFound = errors.New("record not found")

// ─────────────────────────────────────────────
//  Application Repository
// ─────────────────────────────────────────────

type AppRepository struct{ db *pgxpool.Pool }

func NewAppRepository(db *pgxpool.Pool) *AppRepository { return &AppRepository{db: db} }

func (r *AppRepository) Create(ctx context.Context, app *models.Application) error {
	_, err := r.db.Exec(ctx,
		`INSERT INTO applications (id, name, api_key_hash, is_active, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		app.ID, app.Name, app.APIKeyHash, app.IsActive, app.CreatedAt, app.UpdatedAt,
	)
	return err
}

func (r *AppRepository) GetByAPIKeyHash(ctx context.Context, hash string) (*models.Application, error) {
	app := &models.Application{}
	err := r.db.QueryRow(ctx,
		`SELECT id, name, api_key_hash, is_active, created_at, updated_at
		 FROM applications WHERE api_key_hash = $1 AND is_active = TRUE`, hash,
	).Scan(&app.ID, &app.Name, &app.APIKeyHash, &app.IsActive, &app.CreatedAt, &app.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return app, err
}

// ─────────────────────────────────────────────
//  Endpoint Repository
// ─────────────────────────────────────────────

type EndpointRepository struct{ db *pgxpool.Pool }

func NewEndpointRepository(db *pgxpool.Pool) *EndpointRepository { return &EndpointRepository{db: db} }

func (r *EndpointRepository) Create(ctx context.Context, e *models.Endpoint) error {
	eventsJSON, _ := json.Marshal(e.EnabledEvents)
	_, err := r.db.Exec(ctx,
		`INSERT INTO endpoints (id, app_id, url, description, secret_hash, enabled_events, is_active, created_at, updated_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		e.ID, e.AppID, e.URL, e.Description, e.SecretHash, string(eventsJSON), e.IsActive, e.CreatedAt, e.UpdatedAt,
	)
	return err
}

func (r *EndpointRepository) ListByApp(ctx context.Context, appID string) ([]*models.Endpoint, error) {
	rows, err := r.db.Query(ctx,
		`SELECT id, app_id, url, description, secret_hash, enabled_events, is_active, created_at, updated_at
		 FROM endpoints WHERE app_id = $1 ORDER BY created_at DESC`, appID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEndpoints(rows)
}

func (r *EndpointRepository) GetActiveForEvent(ctx context.Context, appID, eventType string) ([]*models.Endpoint, error) {
	// Return endpoints that have "*" or the specific event type in their enabled_events
	rows, err := r.db.Query(ctx,
		`SELECT id, app_id, url, description, secret_hash, enabled_events, is_active, created_at, updated_at
		 FROM endpoints
		 WHERE app_id = $1 AND is_active = TRUE
		 AND (enabled_events::jsonb @> '["*"]'::jsonb OR enabled_events::jsonb @> $2::jsonb)`,
		appID, fmt.Sprintf(`[%q]`, eventType),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEndpoints(rows)
}

func (r *EndpointRepository) GetByID(ctx context.Context, id, appID string) (*models.Endpoint, error) {
	e := &models.Endpoint{}
	var eventsJSON string
	err := r.db.QueryRow(ctx,
		`SELECT id, app_id, url, description, secret_hash, enabled_events, is_active, created_at, updated_at
		 FROM endpoints WHERE id = $1 AND app_id = $2`, id, appID,
	).Scan(&e.ID, &e.AppID, &e.URL, &e.Description, &e.SecretHash, &eventsJSON, &e.IsActive, &e.CreatedAt, &e.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	json.Unmarshal([]byte(eventsJSON), &e.EnabledEvents)
	return e, nil
}

func (r *EndpointRepository) Update(ctx context.Context, id, appID string, req *models.UpdateEndpointRequest) error {
	sets := []string{"updated_at = NOW()"}
	args := []interface{}{}
	i := 1

	if req.URL != "" {
		sets = append(sets, fmt.Sprintf("url = $%d", i)); args = append(args, req.URL); i++
	}
	if req.Description != "" {
		sets = append(sets, fmt.Sprintf("description = $%d", i)); args = append(args, req.Description); i++
	}
	if len(req.EnabledEvents) > 0 {
		eventsJSON, _ := json.Marshal(req.EnabledEvents)
		sets = append(sets, fmt.Sprintf("enabled_events = $%d", i)); args = append(args, string(eventsJSON)); i++
	}
	if req.IsActive != nil {
		sets = append(sets, fmt.Sprintf("is_active = $%d", i)); args = append(args, *req.IsActive); i++
	}

	args = append(args, id, appID)
	q := fmt.Sprintf("UPDATE endpoints SET %s WHERE id = $%d AND app_id = $%d", strings.Join(sets, ", "), i, i+1)
	tag, err := r.db.Exec(ctx, q, args...)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *EndpointRepository) Delete(ctx context.Context, id, appID string) error {
	tag, err := r.db.Exec(ctx, `DELETE FROM endpoints WHERE id = $1 AND app_id = $2`, id, appID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// ─────────────────────────────────────────────
//  Event Repository
// ─────────────────────────────────────────────

type EventRepository struct{ db *pgxpool.Pool }

func NewEventRepository(db *pgxpool.Pool) *EventRepository { return &EventRepository{db: db} }

func (r *EventRepository) Create(ctx context.Context, e *models.Event) error {
	_, err := r.db.Exec(ctx,
		`INSERT INTO events (id, app_id, event_type, payload, status, created_at)
		 VALUES ($1,$2,$3,$4,$5,$6)`,
		e.ID, e.AppID, e.EventType, string(e.Payload), e.Status, e.CreatedAt,
	)
	return err
}

func (r *EventRepository) UpdateStatus(ctx context.Context, id, status string) error {
	_, err := r.db.Exec(ctx, `UPDATE events SET status = $1 WHERE id = $2`, status, id)
	return err
}

func (r *EventRepository) ListByApp(ctx context.Context, appID string, limit, offset int) ([]*models.Event, int64, error) {
	var total int64
	r.db.QueryRow(ctx, `SELECT COUNT(*) FROM events WHERE app_id = $1`, appID).Scan(&total)

	rows, err := r.db.Query(ctx,
		`SELECT id, app_id, event_type, payload, status, created_at
		 FROM events WHERE app_id = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3`,
		appID, limit, offset,
	)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var events []*models.Event
	for rows.Next() {
		ev := &models.Event{}
		var payload string
		if err := rows.Scan(&ev.ID, &ev.AppID, &ev.EventType, &payload, &ev.Status, &ev.CreatedAt); err != nil {
			return nil, 0, err
		}
		ev.Payload = json.RawMessage(payload)
		events = append(events, ev)
	}
	return events, total, nil
}

// ─────────────────────────────────────────────
//  Delivery Repository
// ─────────────────────────────────────────────

type DeliveryRepository struct{ db *pgxpool.Pool }

func NewDeliveryRepository(db *pgxpool.Pool) *DeliveryRepository { return &DeliveryRepository{db: db} }

func (r *DeliveryRepository) Create(ctx context.Context, d *models.Delivery) error {
	_, err := r.db.Exec(ctx,
		`INSERT INTO deliveries (id, event_id, endpoint_id, app_id, attempt_number, status, next_retry_at, created_at, updated_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		d.ID, d.EventID, d.EndpointID, d.AppID, d.AttemptNumber,
		string(d.Status), d.NextRetryAt, d.CreatedAt, d.UpdatedAt,
	)
	return err
}

func (r *DeliveryRepository) MarkSuccess(ctx context.Context, id string, httpStatus int, body string) error {
	now := time.Now()
	_, err := r.db.Exec(ctx,
		`UPDATE deliveries SET status='success', http_status_code=$1, response_body=$2,
		 delivered_at=$3, updated_at=$3 WHERE id=$4`,
		httpStatus, truncate(body, 2000), now, id,
	)
	return err
}

func (r *DeliveryRepository) MarkFailed(ctx context.Context, id, errMsg string, httpStatus *int, nextRetry *time.Time, isDLQ bool) error {
	status := models.DeliveryFailed
	if isDLQ {
		status = models.DeliveryDLQ
	}
	_, err := r.db.Exec(ctx,
		`UPDATE deliveries SET status=$1, error_message=$2, http_status_code=$3,
		 next_retry_at=$4, updated_at=NOW() WHERE id=$5`,
		string(status), truncate(errMsg, 1000), httpStatus, nextRetry, id,
	)
	return err
}

func (r *DeliveryRepository) ListByEvent(ctx context.Context, eventID string) ([]*models.Delivery, error) {
	rows, err := r.db.Query(ctx,
		`SELECT id, event_id, endpoint_id, app_id, attempt_number, status,
		 http_status_code, response_body, error_message, next_retry_at, delivered_at, created_at, updated_at
		 FROM deliveries WHERE event_id = $1 ORDER BY attempt_number ASC`, eventID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanDeliveries(rows)
}

func (r *DeliveryRepository) GetMetrics(ctx context.Context) (map[string]int64, error) {
	rows, err := r.db.Query(ctx,
		`SELECT status, COUNT(*) FROM deliveries GROUP BY status`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	m := map[string]int64{}
	for rows.Next() {
		var status string
		var count int64
		rows.Scan(&status, &count)
		m[status] = count
	}
	return m, nil
}

// ─── helpers ────────────────────────────────────────────────────────────────

func scanEndpoints(rows pgx.Rows) ([]*models.Endpoint, error) {
	var result []*models.Endpoint
	for rows.Next() {
		e := &models.Endpoint{}
		var eventsJSON string
		if err := rows.Scan(&e.ID, &e.AppID, &e.URL, &e.Description, &e.SecretHash, &eventsJSON, &e.IsActive, &e.CreatedAt, &e.UpdatedAt); err != nil {
			return nil, err
		}
		json.Unmarshal([]byte(eventsJSON), &e.EnabledEvents)
		result = append(result, e)
	}
	return result, nil
}

func scanDeliveries(rows pgx.Rows) ([]*models.Delivery, error) {
	var result []*models.Delivery
	for rows.Next() {
		d := &models.Delivery{}
		var status string
		if err := rows.Scan(&d.ID, &d.EventID, &d.EndpointID, &d.AppID,
			&d.AttemptNumber, &status, &d.HTTPStatusCode,
			&d.ResponseBody, &d.ErrorMessage, &d.NextRetryAt, &d.DeliveredAt,
			&d.CreatedAt, &d.UpdatedAt,
		); err != nil {
			return nil, err
		}
		d.Status = models.DeliveryStatus(status)
		result = append(result, d)
	}
	return result, nil
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max]
}
