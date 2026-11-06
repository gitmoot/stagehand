// Package trigger is the HTTP front door for event-triggered runs:
// POST /v1/pipelines/{name}/events, authenticated with HMAC-SHA256.
//
// Signature: hex(HMAC-SHA256(secret, timestamp + "\n" + pipeline + "\n" +
// event_id + "\n" + body)), sent as `X-Stagehand-Signature: sha256=<hex>`
// with `X-Stagehand-Timestamp: <unix seconds>` (within MaxSkew of now) and
// an optional `X-Stagehand-Event-Id` for idempotency.
package trigger

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gitmoot/stagehand/internal/engine"
	"github.com/gitmoot/stagehand/internal/store"
)

// MaxSkew is how far a request's timestamp may be from now.
const MaxSkew = 5 * time.Minute

// Headers.
const (
	HeaderTimestamp = "X-Stagehand-Timestamp"
	HeaderSignature = "X-Stagehand-Signature"
	HeaderEventID   = "X-Stagehand-Event-Id"
)

// Sign returns the signature header value for a request.
func Sign(secret []byte, timestamp int64, pipeline, eventID string, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	fmt.Fprintf(mac, "%d\n%s\n%s\n", timestamp, pipeline, eventID)
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// Accepter takes an authenticated event.
type Accepter interface {
	Trigger(ctx context.Context, req engine.EventRequest) (engine.EventResult, error)
}

// Handler serves the trigger endpoint.
type Handler struct {
	Engine Accepter
	// Secret returns the current HMAC key (re-read so it can be rotated).
	Secret   func() ([]byte, error)
	MaxBytes int64
	Now      func() time.Time
	Log      *slog.Logger
}

// Response is the JSON answer.
type Response struct {
	Accepted  bool             `json:"accepted"`
	Duplicate bool             `json:"duplicate,omitempty"`
	Event     *engine.EventRef `json:"event,omitempty"`
	State     string           `json:"state,omitempty"`
	RunID     int64            `json:"run_id,omitempty"`
	Error     string           `json:"error,omitempty"`
}

func (h Handler) now() time.Time {
	if h.Now != nil {
		return h.Now()
	}
	return time.Now()
}

// Routes registers the endpoint on mux.
func (h Handler) Routes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/pipelines/{name}/events", h.serve)
}

func (h Handler) reply(w http.ResponseWriter, status int, r Response) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(r)
}

func (h Handler) fail(w http.ResponseWriter, r *http.Request, status int, msg string) {
	if h.Log != nil {
		h.Log.Warn("trigger refused", "status", status, "pipeline", r.PathValue("name"), "remote", r.RemoteAddr, "reason", msg)
	}
	h.reply(w, status, Response{Error: msg})
}

func (h Handler) serve(w http.ResponseWriter, r *http.Request) {
	pipeline := r.PathValue("name")
	eventID := r.Header.Get(HeaderEventID)
	if eventID != "" && !engine.ValidEventKey(eventID) {
		h.fail(w, r, http.StatusBadRequest, engine.ErrBadEventKey.Error())
		return
	}
	ts, err := strconv.ParseInt(r.Header.Get(HeaderTimestamp), 10, 64)
	if err != nil {
		h.fail(w, r, http.StatusUnauthorized, "missing or malformed "+HeaderTimestamp)
		return
	}
	if d := h.now().Sub(time.Unix(ts, 0)); d > MaxSkew || d < -MaxSkew {
		h.fail(w, r, http.StatusUnauthorized, "timestamp outside the allowed window")
		return
	}
	sig := r.Header.Get(HeaderSignature)
	if !strings.HasPrefix(sig, "sha256=") {
		h.fail(w, r, http.StatusUnauthorized, "missing or malformed "+HeaderSignature)
		return
	}
	// The body must be read to verify it; it is bounded first.
	limit := h.MaxBytes
	body, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		h.fail(w, r, http.StatusBadRequest, "read body: "+err.Error())
		return
	}
	if int64(len(body)) > limit {
		h.fail(w, r, http.StatusRequestEntityTooLarge, fmt.Sprintf("payload larger than %d bytes", limit))
		return
	}
	secret, err := h.Secret()
	if err != nil {
		if h.Log != nil {
			h.Log.Error("trigger secret unavailable", "err", err)
		}
		h.reply(w, http.StatusServiceUnavailable, Response{Error: "trigger secret unavailable"})
		return
	}
	want := Sign(secret, ts, pipeline, eventID, body)
	if !hmac.Equal([]byte(sig), []byte(want)) {
		h.fail(w, r, http.StatusUnauthorized, "bad signature")
		return
	}

	res, err := h.Engine.Trigger(r.Context(), engine.EventRequest{
		Pipeline: pipeline, Key: eventID, ContentType: r.Header.Get("Content-Type"),
		Source: engine.SourceHTTP, Payload: bytes.NewReader(body), MaxBytes: limit,
	})
	switch {
	case errors.Is(err, store.ErrNotFound):
		h.fail(w, r, http.StatusNotFound, "no such pipeline")
	case errors.Is(err, engine.ErrEventsDisabled):
		h.fail(w, r, http.StatusNotFound, err.Error())
	case errors.Is(err, engine.ErrPipelineDisabled):
		h.fail(w, r, http.StatusConflict, err.Error())
	case errors.Is(err, store.ErrQueueFull):
		w.Header().Set("Retry-After", "60")
		h.fail(w, r, http.StatusTooManyRequests, err.Error())
	case errors.Is(err, engine.ErrPayloadTooLarge):
		h.fail(w, r, http.StatusRequestEntityTooLarge, err.Error())
	case errors.Is(err, engine.ErrBadEventKey):
		h.fail(w, r, http.StatusBadRequest, err.Error())
	case err != nil:
		if h.Log != nil {
			h.Log.Error("trigger", "pipeline", pipeline, "err", err)
		}
		h.reply(w, http.StatusInternalServerError, Response{Error: "internal error"})
	default:
		resp := Response{Accepted: true, Duplicate: res.Duplicate, Event: engine.EventRefOf(res.Event), State: res.Event.State, RunID: res.Event.RunID}
		resp.Event.PayloadPath = "" // never reveal host paths to a caller
		status := http.StatusAccepted
		if res.Duplicate {
			status = http.StatusOK
		}
		h.reply(w, status, resp)
	}
}
