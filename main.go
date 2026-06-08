package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"github.com/tejdeep/gorelay/internal/config"
	"github.com/tejdeep/gorelay/internal/db"
	"github.com/tejdeep/gorelay/internal/handlers"
	"github.com/tejdeep/gorelay/internal/middleware"
	"github.com/tejdeep/gorelay/internal/queue"
	"github.com/tejdeep/gorelay/internal/repository"
	"github.com/tejdeep/gorelay/internal/worker"
)

func main() {
	_ = godotenv.Load()
	cfg := config.Load()

	// ── Infrastructure ─────────────────────────────────────────────────────
	pgPool, err := db.NewPostgresDB(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("PostgreSQL: %v", err)
	}
	defer pgPool.Close()
	log.Println("✓ PostgreSQL connected")

	rdb := db.NewRedisClient(cfg.RedisURL)
	defer rdb.Close()
	log.Println("✓ Redis connected")

	// ── Repositories ──────────────────────────────────────────────────────
	appRepo      := repository.NewAppRepository(pgPool)
	endpointRepo := repository.NewEndpointRepository(pgPool)
	eventRepo    := repository.NewEventRepository(pgPool)
	deliveryRepo := repository.NewDeliveryRepository(pgPool)

	// ── Queue ──────────────────────────────────────────────────────────────
	q := queue.NewRedisQueue(rdb, cfg.QueueName, cfg.DLQName)

	// ── Worker Pool ────────────────────────────────────────────────────────
	pool := worker.NewPool(
		cfg.WorkerCount,
		cfg.MaxRetries,
		cfg.HTTPTimeout,
		q,
		deliveryRepo,
		eventRepo,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool.Start(ctx)

	// ── Handlers ───────────────────────────────────────────────────────────
	appH      := handlers.NewAppHandler(appRepo)
	endpointH := handlers.NewEndpointHandler(endpointRepo)
	eventH    := handlers.NewEventHandler(eventRepo, endpointRepo, deliveryRepo, q)
	metricsH  := handlers.NewMetricsHandler(deliveryRepo, eventRepo, q)
	authMW    := middleware.NewAPIKeyMiddleware(appRepo)

	// ── Router ─────────────────────────────────────────────────────────────
	if cfg.Env == "production" {
		gin.SetMode(gin.ReleaseMode)
	}
	r := gin.Default()

	r.Use(cors.New(cors.Config{
		AllowOrigins: []string{"*"},
		AllowMethods: []string{"GET", "POST", "PATCH", "DELETE", "OPTIONS"},
		AllowHeaders: []string{"Origin", "Content-Type", "X-API-Key", "Authorization"},
		MaxAge:       12 * time.Hour,
	}))

	// Public
	r.GET("/health", handlers.HealthCheck)

	// Admin (protected by admin secret — simple header check in production)
	admin := r.Group("/admin")
	admin.POST("/apps", appH.Create)

	// Authenticated API (X-API-Key required)
	api := r.Group("/api")
	api.Use(authMW.Authenticate())
	{
		// Endpoints
		ep := api.Group("/endpoints")
		{
			ep.POST("", endpointH.Create)
			ep.GET("", endpointH.List)
			ep.GET("/:id", endpointH.Get)
			ep.PATCH("/:id", endpointH.Update)
			ep.DELETE("/:id", endpointH.Delete)
		}

		// Events
		ev := api.Group("/events")
		{
			ev.POST("", eventH.Ingest)              // core ingest path
			ev.GET("", eventH.List)
			ev.GET("/:id/deliveries", eventH.GetDeliveries)
		}

		// Metrics
		api.GET("/metrics", metricsH.Get)
	}

	// ── Server ─────────────────────────────────────────────────────────────
	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		log.Printf("🚀 GoRelay API on :%s | workers=%d maxRetries=%d",
			cfg.Port, cfg.WorkerCount, cfg.MaxRetries)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server: %v", err)
		}
	}()

	// ── Graceful Shutdown ──────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutdown signal received...")
	cancel() // signal context cancellation to worker pool

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutCancel()

	if err := srv.Shutdown(shutCtx); err != nil {
		log.Printf("HTTP shutdown error: %v", err)
	}

	pool.Stop()
	log.Println("GoRelay stopped cleanly ✓")
}
