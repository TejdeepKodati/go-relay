package middleware

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/tejdeep/gorelay/internal/repository"
)

// APIKeyMiddleware authenticates requests using an API key passed in
// the X-API-Key header. The key is never stored plaintext — only its
// SHA-256 hash is compared against the database.
type APIKeyMiddleware struct {
	appRepo *repository.AppRepository
}

func NewAPIKeyMiddleware(appRepo *repository.AppRepository) *APIKeyMiddleware {
	return &APIKeyMiddleware{appRepo: appRepo}
}

func (m *APIKeyMiddleware) Authenticate() gin.HandlerFunc {
	return func(c *gin.Context) {
		raw := strings.TrimSpace(c.GetHeader("X-API-Key"))
		if raw == "" {
			// Also accept Bearer token for convenience
			auth := c.GetHeader("Authorization")
			if strings.HasPrefix(strings.ToLower(auth), "bearer ") {
				raw = strings.TrimSpace(auth[7:])
			}
		}

		if raw == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "missing X-API-Key header",
			})
			return
		}

		hash := HashAPIKey(raw)
		app, err := m.appRepo.GetByAPIKeyHash(context.Background(), hash)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "invalid or inactive API key",
			})
			return
		}

		// Inject app context for downstream handlers
		c.Set("app_id", app.ID)
		c.Set("app_name", app.Name)
		c.Next()
	}
}

// HashAPIKey produces a deterministic hex SHA-256 of the raw key.
// This is intentionally fast (not bcrypt) because it runs on every request —
// API keys are long random strings so speed here is acceptable.
func HashAPIKey(raw string) string {
	h := sha256.Sum256([]byte(raw))
	return fmt.Sprintf("%x", h)
}
