package config

import "os"

type Config struct {
	Env         string
	Port        string
	DatabaseURL string
	RedisURL    string
	WorkerCount int    // How many concurrent delivery goroutines
	MaxRetries  int    // Delivery attempts before DLQ
	QueueName   string // Redis list key for delivery jobs
	DLQName     string // Dead-letter queue key
	HTTPTimeout int    // Seconds for outbound webhook HTTP calls
}

func Load() *Config {
	return &Config{
		Env:         getEnv("ENV", "development"),
		Port:        getEnv("PORT", "8090"),
		DatabaseURL: getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/gorelay?sslmode=disable"),
		RedisURL:    getEnv("REDIS_URL", "redis://localhost:6379"),
		WorkerCount: getEnvInt("WORKER_COUNT", 10),
		MaxRetries:  getEnvInt("MAX_RETRIES", 3),
		QueueName:   getEnv("QUEUE_NAME", "gorelay:delivery_queue"),
		DLQName:     getEnv("DLQ_NAME", "gorelay:dead_letter_queue"),
		HTTPTimeout: getEnvInt("HTTP_TIMEOUT_SEC", 30),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		var n int
		if _, err := parseIntHelper(v, &n); err == nil {
			return n
		}
	}
	return fallback
}

func parseIntHelper(s string, out *int) (int, error) {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, nil
		}
		n = n*10 + int(c-'0')
	}
	*out = n
	return n, nil
}
