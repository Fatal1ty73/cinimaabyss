package main

import (
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type config struct {
	port                   string
	monolithURL            *url.URL
	moviesServiceURL       *url.URL
	eventsServiceURL       *url.URL
	gradualMigration       bool
	moviesMigrationPercent int
}

func main() {
	cfg := mustLoadConfig()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", proxyHealthHandler)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		target := routeTarget(cfg, r)
		if target == nil {
			writeError(w, http.StatusNotFound, "route is not defined in proxy")
			return
		}

		log.Printf("proxying %s %s to %s", r.Method, r.URL.RequestURI(), target)
		newReverseProxy(target).ServeHTTP(w, r)
	})

	addr := ":" + cfg.port
	log.Printf("starting proxy service on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func mustLoadConfig() config {
	return config{
		port:                   getEnv("PORT", "8000"),
		monolithURL:            mustParseURL(getEnv("MONOLITH_URL", "http://localhost:8080")),
		moviesServiceURL:       mustParseURL(getEnv("MOVIES_SERVICE_URL", "http://localhost:8081")),
		eventsServiceURL:       mustParseURL(getEnv("EVENTS_SERVICE_URL", "http://localhost:8082")),
		gradualMigration:       getEnvAsBool("GRADUAL_MIGRATION", true),
		moviesMigrationPercent: clampPercent(getEnvAsInt("MOVIES_MIGRATION_PERCENT", 50)),
	}
}

func proxyHealthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("Strangler Fig Proxy is healthy"))
}

func routeTarget(cfg config, r *http.Request) *url.URL {
	path := r.URL.Path

	switch {
	case path == "/health":
		return nil
	case path == "/api/movies/health":
		return cfg.moviesServiceURL
	case strings.HasPrefix(path, "/api/events"):
		return cfg.eventsServiceURL
	case path == "/api/movies":
		return moviesTarget(cfg)
	case path == "/api/users", path == "/api/payments", path == "/api/subscriptions":
		return cfg.monolithURL
	default:
		return nil
	}
}

func moviesTarget(cfg config) *url.URL {
	if !cfg.gradualMigration {
		return cfg.moviesServiceURL
	}

	if rand.Intn(100) < cfg.moviesMigrationPercent {
		return cfg.moviesServiceURL
	}

	return cfg.monolithURL
}

func newReverseProxy(target *url.URL) *httputil.ReverseProxy {
	proxy := httputil.NewSingleHostReverseProxy(target)
	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)
		req.Host = target.Host
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("proxy error for %s: %v", r.URL.Path, err)
		writeError(w, http.StatusBadGateway, "upstream service is unavailable")
	}
	return proxy
}

func mustParseURL(raw string) *url.URL {
	parsed, err := url.Parse(raw)
	if err != nil {
		log.Fatalf("invalid url %q: %v", raw, err)
	}
	return parsed
}

func getEnv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func getEnvAsBool(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	parsed, err := strconv.ParseBool(value)
	if err != nil {
		log.Printf("invalid bool for %s=%q, using %t", key, value, fallback)
		return fallback
	}
	return parsed
}

func getEnvAsInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		log.Printf("invalid int for %s=%q, using %d", key, value, fallback)
		return fallback
	}
	return parsed
}

func clampPercent(value int) int {
	if value < 0 {
		return 0
	}
	if value > 100 {
		return 100
	}
	return value
}

func writeError(w http.ResponseWriter, status int, message string) {
	http.Error(w, message, status)
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
