// Package server exposes the causal engine over a real HTTP API and
// serves the operator page.
package server

import (
	_ "embed"
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"vacuum-interlock/internal/causality"
)

//go:embed static/index.html
var indexHTML []byte

// Server wires the engine to HTTP handlers.
type Server struct {
	eng *causality.Engine
	mux *http.ServeMux
}

// New builds a Server with all routes registered.
func New(eng *causality.Engine) *Server {
	s := &Server{eng: eng, mux: http.NewServeMux()}
	s.mux.HandleFunc("GET /healthz", s.health)
	s.mux.HandleFunc("GET /{$}", s.index)
	s.mux.HandleFunc("POST /api/rounds", s.createRound)
	s.mux.HandleFunc("GET /api/rounds", s.listRounds)
	s.mux.HandleFunc("GET /api/rounds/{id}", s.getRound)
	s.mux.HandleFunc("POST /api/rounds/{id}/events", s.submitEvent)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) index(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(indexHTML)
}

type createRoundRequest struct {
	ID       string            `json:"id"`
	Consoles []string          `json:"consoles"`
	Valves   map[string]string `json:"valves"`
}

func (s *Server) createRound(w http.ResponseWriter, r *http.Request) {
	var req createRoundRequest
	if err := decodeJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	view, err := s.eng.CreateRound(req.ID, req.Consoles, req.Valves)
	if err != nil {
		writeEngineErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, view)
}

func (s *Server) listRounds(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"rounds": s.eng.ListRounds()})
}

func (s *Server) getRound(w http.ResponseWriter, r *http.Request) {
	view, err := s.eng.View(r.PathValue("id"))
	if err != nil {
		writeEngineErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

// submitResponse carries the event verdict plus a fresh round snapshot so
// the page (and the verifier) can observe the causal state directly.
type submitResponse struct {
	EventID    string               `json:"event_id"`
	Status     causality.Status     `json:"status"`
	Reason     string               `json:"reason,omitempty"`
	Consumed   bool                 `json:"consumed"`
	ValveAfter string               `json:"valve_after,omitempty"`
	Round      *causality.RoundView `json:"round,omitempty"`
}

func (s *Server) submitEvent(w http.ResponseWriter, r *http.Request) {
	roundID := r.PathValue("id")
	var e causality.Event
	if err := decodeJSON(r, &e); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	rec, err := s.eng.Submit(roundID, e)
	if err != nil {
		writeEngineErr(w, err)
		return
	}
	resp := submitResponse{
		EventID:    rec.Event.EventID,
		Status:     rec.Status,
		Reason:     rec.Reason,
		Consumed:   rec.Consumed,
		ValveAfter: rec.ValveAfter,
	}
	if view, verr := s.eng.View(roundID); verr == nil {
		resp.Round = &view
	}
	code := http.StatusOK
	if rec.Status == causality.StatusWaiting {
		code = http.StatusAccepted
	}
	writeJSON(w, code, resp)
}

func writeEngineErr(w http.ResponseWriter, err error) {
	var ve *causality.ValidationError
	var ce *causality.ConflictError
	switch {
	case errors.Is(err, causality.ErrRoundNotFound):
		writeErr(w, http.StatusNotFound, err.Error())
	case errors.As(err, &ve):
		writeErr(w, http.StatusBadRequest, ve.Reason)
	case errors.As(err, &ce):
		writeErr(w, http.StatusConflict, ce.Reason)
	default:
		log.Printf("internal error: %v", err)
		writeErr(w, http.StatusInternalServerError, "internal error")
	}
}

func writeErr(w http.ResponseWriter, code int, reason string) {
	writeJSON(w, code, map[string]string{"status": "rejected", "reason": reason})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func decodeJSON(r *http.Request, v any) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(v)
}
