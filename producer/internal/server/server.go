// Package server exposes the producer HTTP API.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"producer/internal/event"
	"producer/internal/publisher"
)

const maxBodyBytes = 1 << 20 // 1 MiB

// Publisher is what the handlers need from internal/publisher.
type Publisher interface {
	Publish(ctx context.Context, e event.Event) (publisher.Result, error)
	PublishBatch(ctx context.Context, events []event.Event) ([]publisher.Result, error)
}

type Server struct {
	pub Publisher
}

func New(pub Publisher) http.Handler {
	s := &Server{pub: pub}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("POST /events", s.postEvent)
	mux.HandleFunc("POST /events/batch", s.postBatch)
	return mux
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func badRequest(w http.ResponseWriter, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeError(w, http.StatusRequestEntityTooLarge, errors.New("request body too large"))
		return
	}
	writeError(w, http.StatusBadRequest, err)
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) postEvent(w http.ResponseWriter, r *http.Request) {
	e, err := event.Parse(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		badRequest(w, err)
		return
	}
	result, err := s.pub.Publish(r.Context(), e)
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) postBatch(w http.ResponseWriter, r *http.Request) {
	events, err := event.ParseBatch(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		badRequest(w, err)
		return
	}
	if len(events) == 0 || len(events) > publisher.MaxBatchSize {
		writeError(w, http.StatusBadRequest, publisher.ErrBatchSize)
		return
	}
	results, err := s.pub.PublishBatch(r.Context(), events)
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	failed := 0
	for _, result := range results {
		if result.Error != "" {
			failed++
		}
	}
	status := http.StatusOK
	if failed > 0 {
		status = http.StatusMultiStatus
	}
	writeJSON(w, status, map[string]any{"count": len(results), "failed": failed, "results": results})
}
