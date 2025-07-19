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

// backend targets (parsed once at startup)
var (
	monolithURL     *url.URL
	moviesSvcURL    *url.URL
	eventsSvcURL    *url.URL
	monolithProxy   *httputil.ReverseProxy
	moviesSvcProxy  *httputil.ReverseProxy
	eventsSvcProxy  *httputil.ReverseProxy
	gradual         bool
	migrationShare  int // 0-100
)

func main() {
	loadConfig()
	rand.Seed(time.Now().UnixNano())

	http.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":true}`))
	})

	// маршрутизация всех API-запросов
	http.HandleFunc("/api/", dispatch)

	port := getenv("PORT", "8000")
	log.Printf("Proxy-service started on :%s (gradual=%v, share=%d%%)", port, gradual, migrationShare)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

// dispatch выбирает целевой бэкенд и проксирует запрос
func dispatch(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	switch {
	case strings.HasPrefix(path, "/api/movies/health"):
		moviesSvcProxy.ServeHTTP(w, r)
	case strings.HasPrefix(path, "/api/movies"):
		routeMovies(w, r)
	case strings.HasPrefix(path, "/api/events"):
		eventsSvcProxy.ServeHTTP(w, r)
	default:
		// всё остальное — к монолиту
		monolithProxy.ServeHTTP(w, r)
	}
}

// routeMovies распределяет трафик между монолитом и movies-service
func routeMovies(w http.ResponseWriter, r *http.Request) {
	if !gradual {
		moviesSvcProxy.ServeHTTP(w, r)
		return
	}
	if rand.Intn(100) < migrationShare {
		moviesSvcProxy.ServeHTTP(w, r)
	} else {
		monolithProxy.ServeHTTP(w, r)
	}
}

// ------------ helpers ------------

func loadConfig() {

	monolithURL = mustParseURL(getenv("MONOLITH_URL", "MONOLITH_URL=http://cinemaabyss-monolith:8080"))
	moviesSvcURL = mustParseURL(getenv("MOVIES_SERVICE_URL", "MOVIES_SERVICE_URL=http://cinemaabyss-movies-service:8081"))
	eventsSvcURL = mustParseURL(getenv("EVENTS_SERVICE_URL", "http://cinemaabyss-events-service:8082"))

	monolithProxy = newProxy(monolithURL)
	moviesSvcProxy = newProxy(moviesSvcURL)
	eventsSvcProxy = newProxy(eventsSvcURL)

	gradual = strings.ToLower(getenv("GRADUAL_MIGRATION", "false")) == "true"

	if p, err := strconv.Atoi(getenv("MOVIES_MIGRATION_PERCENT", "50")); err == nil && p >= 0 && p <= 100 {
		migrationShare = p
	} else {
		log.Printf("Invalid MOVIES_MIGRATION_PERCENT, defaulting to 50")
		migrationShare = 50
	}
}

func newProxy(target *url.URL) *httputil.ReverseProxy {
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, e error) {
		log.Printf("proxy error (%s): %v", target, e)
		http.Error(w, "upstream unavailable", http.StatusBadGateway)
	}
	return proxy
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func mustParseURL(raw string) *url.URL {
	u, err := url.Parse(raw)
	if err != nil {
		log.Fatalf("invalid URL %q: %v", raw, err)
	}
	return u
}
