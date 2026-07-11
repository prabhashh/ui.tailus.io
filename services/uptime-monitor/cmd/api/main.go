// Command api is a minimal REST surface for managing monitors and
// notification channels. It intentionally does no authentication/multi-
// tenancy logic here — put this behind your existing auth/reverse proxy
// (see docs/OPERATIONS.md) and pass the authenticated owner_id through,
// e.g. as a trusted header your proxy sets after verifying a session.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/tailus/uptime-monitor/internal/config"
	"github.com/tailus/uptime-monitor/internal/metrics"
	"github.com/tailus/uptime-monitor/internal/models"
	"github.com/tailus/uptime-monitor/internal/storage/postgres"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := config.Load("api")
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := postgres.New(ctx, cfg.PostgresDSN, cfg.PostgresMaxConns, cfg.PostgresMinConns)
	if err != nil {
		slog.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	h := &handler{store: store}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", h.healthz)
	mux.Handle("GET /metrics", metrics.Handler())
	mux.HandleFunc("POST /monitors", h.createMonitor)
	mux.HandleFunc("GET /monitors", h.listMonitors)
	mux.HandleFunc("GET /monitors/{id}", h.getMonitor)
	mux.HandleFunc("DELETE /monitors/{id}", h.deleteMonitor)
	mux.HandleFunc("POST /monitors/{id}/enable", h.setEnabled(true))
	mux.HandleFunc("POST /monitors/{id}/disable", h.setEnabled(false))

	addr := os.Getenv("UPTIME_API_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	slog.Info("api listening", "addr", addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("api server exited with error", "error", err)
		os.Exit(1)
	}
}

type handler struct {
	store *postgres.Store
}

func (h *handler) healthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// ownerID trusts an upstream-verified header. Replace with real auth
// middleware before exposing this publicly.
func ownerID(r *http.Request) (uuid.UUID, error) {
	return uuid.Parse(r.Header.Get("X-Owner-Id"))
}

func (h *handler) createMonitor(w http.ResponseWriter, r *http.Request) {
	owner, err := ownerID(r)
	if err != nil {
		http.Error(w, "missing or invalid X-Owner-Id", http.StatusUnauthorized)
		return
	}

	var m models.Monitor
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	m.OwnerID = owner
	applyMonitorDefaults(&m)

	if err := validateMonitor(m); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	created, err := h.store.CreateMonitor(r.Context(), m)
	if err != nil {
		slog.Error("create monitor failed", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, created)
}

func (h *handler) listMonitors(w http.ResponseWriter, r *http.Request) {
	owner, err := ownerID(r)
	if err != nil {
		http.Error(w, "missing or invalid X-Owner-Id", http.StatusUnauthorized)
		return
	}
	monitors, err := h.store.ListMonitorsByOwner(r.Context(), owner)
	if err != nil {
		slog.Error("list monitors failed", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, monitors)
}

func (h *handler) getMonitor(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}
	m, err := h.store.GetMonitor(r.Context(), id)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, m)
}

func (h *handler) deleteMonitor(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}
	if err := h.store.DeleteMonitor(r.Context(), id); err != nil {
		slog.Error("delete monitor failed", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) setEnabled(enabled bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := uuid.Parse(r.PathValue("id"))
		if err != nil {
			http.Error(w, "invalid id", http.StatusBadRequest)
			return
		}
		if err := h.store.SetMonitorEnabled(r.Context(), id, enabled); err != nil {
			slog.Error("set monitor enabled failed", "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func applyMonitorDefaults(m *models.Monitor) {
	if m.CheckType == "" {
		m.CheckType = models.CheckTypeHTTP
	}
	if m.Method == "" {
		m.Method = http.MethodGet
	}
	if m.ExpectedStatusMin == 0 && m.ExpectedStatusMax == 0 {
		m.ExpectedStatusMin, m.ExpectedStatusMax = 200, 299
	}
	if m.IntervalSeconds == 0 {
		m.IntervalSeconds = 30
	}
	if m.BaseTimeoutMS == 0 {
		m.BaseTimeoutMS = 5000
	}
	if m.MinTimeoutMS == 0 {
		m.MinTimeoutMS = 2000
	}
	if m.MaxTimeoutMS == 0 {
		m.MaxTimeoutMS = 30000
	}
	if len(m.Regions) == 0 {
		m.Regions = []string{"default"}
	}
	if m.FailureThreshold == 0 {
		m.FailureThreshold = 3
	}
	m.Enabled = true
}

func validateMonitor(m models.Monitor) error {
	if m.Name == "" {
		return errRequired("name")
	}
	if m.URL == "" {
		return errRequired("url")
	}
	if m.IntervalSeconds < 5 {
		return errInvalid("interval_seconds", "must be >= 5")
	}
	return nil
}

type validationError struct{ msg string }

func (e validationError) Error() string { return e.msg }
func errRequired(field string) error    { return validationError{field + " is required"} }
func errInvalid(field, reason string) error {
	return validationError{field + " is invalid: " + reason}
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
