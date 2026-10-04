// Package api exposes the lease-lock service over HTTP.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/hashicorp/raft"

	"github.com/huangjie666777-ux/raft-lease-coordinator-149/internal/fsm"
	"github.com/huangjie666777-ux/raft-lease-coordinator-149/internal/node"
)

const (
	minTTLSec     = 1
	maxTTLSec     = 300
	commitTimeout = 5 * time.Second
)

// Server serves the lock HTTP API on top of a Raft node.
type Server struct {
	node *node.Node
	mux  *http.ServeMux
	srv  *http.Server
}

func NewServer(n *node.Node, httpAddr string) *Server {
	s := &Server{node: n, mux: http.NewServeMux()}
	s.mux.HandleFunc("POST /acquire", s.handleAcquire)
	s.mux.HandleFunc("POST /renew", s.handleRenew)
	s.mux.HandleFunc("POST /release", s.handleRelease)
	s.mux.HandleFunc("GET /query", s.handleQuery)
	s.mux.HandleFunc("POST /barrier/create", s.handleBarrierCreate)
	s.mux.HandleFunc("POST /barrier/arrive", s.handleBarrierArrive)
	s.mux.HandleFunc("POST /barrier/advance", s.handleBarrierAdvance)
	s.mux.HandleFunc("GET /barrier", s.handleBarrierQuery)
	s.mux.HandleFunc("POST /checkpoint/create", s.handleCheckpointCreate)
	s.mux.HandleFunc("POST /checkpoint/submit", s.handleCheckpointSubmit)
	s.mux.HandleFunc("POST /checkpoint/publish", s.handleCheckpointPublish)
	s.mux.HandleFunc("GET /checkpoint", s.handleCheckpointQuery)
	s.srv = &http.Server{Addr: httpAddr, Handler: s.mux}
	return s
}

// ListenAndServe starts the HTTP server.
func (s *Server) ListenAndServe() error {
	return s.srv.ListenAndServe()
}

// Close shuts the HTTP server down.
func (s *Server) Close() error {
	return s.srv.Close()
}

type lockRequest struct {
	Resource string `json:"resource"`
	Holder   string `json:"holder"`
	Token    uint64 `json:"token"`
	TTL      int64  `json:"ttl"`
}

type lockResponse struct {
	OK     bool   `json:"ok"`
	Holder string `json:"holder,omitempty"`
	Token  uint64 `json:"token,omitempty"`
	Expiry int64  `json:"expiry,omitempty"`
	Leader string `json:"leader,omitempty"`
	Error  string `json:"error,omitempty"`

	Barrier *fsm.BarrierView `json:"barrier,omitempty"`

	Checkpoint *fsm.CheckpointView `json:"checkpoint,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// requireLeader rejects the request on followers, pointing at the known
// leader HTTP address. Followers never succeed locally.
func (s *Server) requireLeader(w http.ResponseWriter) bool {
	if s.node.IsLeader() {
		return true
	}
	writeJSON(w, http.StatusTemporaryRedirect, lockResponse{
		OK:     false,
		Leader: s.node.LeaderHTTPAddr(),
		Error:  "not leader",
	})
	return false
}

func (s *Server) propose(w http.ResponseWriter, cmd fsm.Command) {
	res, err := s.node.Propose(cmd, commitTimeout)
	if err != nil {
		status := http.StatusServiceUnavailable
		msg := "result unconfirmed: " + err.Error()
		if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
			status = http.StatusTemporaryRedirect
			writeJSON(w, status, lockResponse{OK: false, Leader: s.node.LeaderHTTPAddr(), Error: msg})
			return
		}
		writeJSON(w, status, lockResponse{OK: false, Error: msg})
		return
	}
	if !res.OK {
		writeJSON(w, http.StatusConflict, lockResponse{
			OK: false, Holder: res.Holder, Token: res.Token, Expiry: res.Expiry, Error: res.Err,
			Barrier: res.Barrier, Checkpoint: res.Checkpoint,
		})
		return
	}
	writeJSON(w, http.StatusOK, lockResponse{
		OK: true, Holder: res.Holder, Token: res.Token, Expiry: res.Expiry,
		Barrier: res.Barrier, Checkpoint: res.Checkpoint,
	})
}

func validResourceHolder(w http.ResponseWriter, req lockRequest) bool {
	if req.Resource == "" || req.Holder == "" {
		writeJSON(w, http.StatusBadRequest, lockResponse{Error: "resource and holder are required"})
		return false
	}
	return true
}

func validTTL(w http.ResponseWriter, ttl int64) bool {
	if ttl < minTTLSec || ttl > maxTTLSec {
		writeJSON(w, http.StatusBadRequest, lockResponse{Error: "ttl must be between 1 and 300 seconds"})
		return false
	}
	return true
}

func decode(w http.ResponseWriter, r *http.Request) (lockRequest, bool) {
	var req lockRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, lockResponse{Error: "invalid JSON: " + err.Error()})
		return req, false
	}
	return req, true
}

func (s *Server) handleAcquire(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w) {
		return
	}
	req, ok := decode(w, r)
	if !ok || !validResourceHolder(w, req) || !validTTL(w, req.TTL) {
		return
	}
	// The leader stamps the decision time into the command; replicas replay
	// it without reading their own clocks.
	s.propose(w, fsm.Command{
		Type: fsm.CmdAcquire, Resource: req.Resource, Holder: req.Holder,
		TTL: req.TTL, Now: time.Now(),
	})
}

func (s *Server) handleRenew(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w) {
		return
	}
	req, ok := decode(w, r)
	if !ok || !validResourceHolder(w, req) || !validTTL(w, req.TTL) {
		return
	}
	s.propose(w, fsm.Command{
		Type: fsm.CmdRenew, Resource: req.Resource, Holder: req.Holder,
		Token: req.Token, TTL: req.TTL, Now: time.Now(),
	})
}

func (s *Server) handleRelease(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w) {
		return
	}
	req, ok := decode(w, r)
	if !ok || !validResourceHolder(w, req) {
		return
	}
	s.propose(w, fsm.Command{
		Type: fsm.CmdRelease, Resource: req.Resource, Holder: req.Holder,
		Token: req.Token, Now: time.Now(),
	})
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w) {
		return
	}
	resource := r.URL.Query().Get("resource")
	if resource == "" {
		writeJSON(w, http.StatusBadRequest, lockResponse{Error: "resource is required"})
		return
	}
	// Reads are decided from the committed log like writes: the query is
	// itself replicated, so followers never answer from local state and
	// the decision time travels with the command.
	s.propose(w, fsm.Command{Type: fsm.CmdQueryLease, Resource: resource, Now: time.Now()})
}

type barrierCreateRequest struct {
	Name         string   `json:"name"`
	Participants []string `json:"participants"`
}

type barrierArriveRequest struct {
	Name        string `json:"name"`
	Round       uint64 `json:"round"`
	Participant string `json:"participant"`
	Resource    string `json:"resource"`
	Holder      string `json:"holder"`
	Token       uint64 `json:"token"`
}

type barrierAdvanceRequest struct {
	Name  string `json:"name"`
	Round uint64 `json:"round"`
}

func (s *Server) handleBarrierCreate(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w) {
		return
	}
	var req barrierCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, lockResponse{Error: "invalid JSON: " + err.Error()})
		return
	}
	if req.Name == "" {
		writeJSON(w, http.StatusBadRequest, lockResponse{Error: "name is required"})
		return
	}
	s.propose(w, fsm.Command{
		Type: fsm.CmdBarrierCreate, Barrier: req.Name,
		Participants: req.Participants, Now: time.Now(),
	})
}

func (s *Server) handleBarrierArrive(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w) {
		return
	}
	var req barrierArriveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, lockResponse{Error: "invalid JSON: " + err.Error()})
		return
	}
	if req.Name == "" || req.Participant == "" || req.Resource == "" || req.Holder == "" {
		writeJSON(w, http.StatusBadRequest, lockResponse{Error: "name, participant, resource and holder are required"})
		return
	}
	s.propose(w, fsm.Command{
		Type: fsm.CmdBarrierArrive, Barrier: req.Name, Round: req.Round,
		Participant: req.Participant, Resource: req.Resource,
		Holder: req.Holder, Token: req.Token, Now: time.Now(),
	})
}

func (s *Server) handleBarrierAdvance(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w) {
		return
	}
	var req barrierAdvanceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, lockResponse{Error: "invalid JSON: " + err.Error()})
		return
	}
	if req.Name == "" {
		writeJSON(w, http.StatusBadRequest, lockResponse{Error: "name is required"})
		return
	}
	s.propose(w, fsm.Command{
		Type: fsm.CmdBarrierAdvance, Barrier: req.Name, Round: req.Round, Now: time.Now(),
	})
}

func (s *Server) handleBarrierQuery(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w) {
		return
	}
	name := r.URL.Query().Get("name")
	if name == "" {
		writeJSON(w, http.StatusBadRequest, lockResponse{Error: "name is required"})
		return
	}
	s.propose(w, fsm.Command{Type: fsm.CmdBarrierQuery, Barrier: name, Now: time.Now()})
}

type checkpointCreateRequest struct {
	Task       string `json:"task"`
	Generation uint64 `json:"generation"`
	Barrier    string `json:"barrier"`
}

type checkpointSubmitRequest struct {
	Task        string `json:"task"`
	Generation  uint64 `json:"generation"`
	Participant string `json:"participant"`
	Resource    string `json:"resource"`
	Holder      string `json:"holder"`
	Token       uint64 `json:"token"`
	URI         string `json:"uri"`
	SHA256      string `json:"sha256"`
	Bytes       uint64 `json:"bytes"`
}

type checkpointPublishRequest struct {
	Task       string `json:"task"`
	Generation uint64 `json:"generation"`
}

func validTaskGeneration(w http.ResponseWriter, task string, gen uint64) bool {
	if task == "" || gen < 1 {
		writeJSON(w, http.StatusBadRequest, lockResponse{Error: "task and a positive integer generation are required"})
		return false
	}
	return true
}

func (s *Server) handleCheckpointCreate(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w) {
		return
	}
	var req checkpointCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, lockResponse{Error: "invalid JSON: " + err.Error()})
		return
	}
	if !validTaskGeneration(w, req.Task, req.Generation) {
		return
	}
	if req.Barrier == "" {
		writeJSON(w, http.StatusBadRequest, lockResponse{Error: "barrier is required"})
		return
	}
	s.propose(w, fsm.Command{
		Type: fsm.CmdCheckpointCreate, Task: req.Task, Generation: req.Generation,
		Barrier: req.Barrier, Now: time.Now(),
	})
}

func (s *Server) handleCheckpointSubmit(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w) {
		return
	}
	var req checkpointSubmitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, lockResponse{Error: "invalid JSON: " + err.Error()})
		return
	}
	if !validTaskGeneration(w, req.Task, req.Generation) {
		return
	}
	if req.Participant == "" || req.Resource == "" || req.Holder == "" {
		writeJSON(w, http.StatusBadRequest, lockResponse{Error: "participant, resource and holder are required"})
		return
	}
	if req.URI == "" {
		writeJSON(w, http.StatusBadRequest, lockResponse{Error: "uri is required"})
		return
	}
	s.propose(w, fsm.Command{
		Type: fsm.CmdCheckpointSubmit, Task: req.Task, Generation: req.Generation,
		Participant: req.Participant, Resource: req.Resource,
		Holder: req.Holder, Token: req.Token,
		URI: req.URI, SHA256: req.SHA256, Bytes: req.Bytes, Now: time.Now(),
	})
}

func (s *Server) handleCheckpointPublish(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w) {
		return
	}
	var req checkpointPublishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, lockResponse{Error: "invalid JSON: " + err.Error()})
		return
	}
	if !validTaskGeneration(w, req.Task, req.Generation) {
		return
	}
	s.propose(w, fsm.Command{
		Type: fsm.CmdCheckpointPublish, Task: req.Task, Generation: req.Generation, Now: time.Now(),
	})
}

func (s *Server) handleCheckpointQuery(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w) {
		return
	}
	task := r.URL.Query().Get("task")
	if task == "" {
		writeJSON(w, http.StatusBadRequest, lockResponse{Error: "task is required"})
		return
	}
	var gen uint64
	if raw := r.URL.Query().Get("generation"); raw != "" {
		if _, err := fmt.Sscanf(raw, "%d", &gen); err != nil || gen < 1 {
			writeJSON(w, http.StatusBadRequest, lockResponse{Error: "generation must be a positive integer"})
			return
		}
	}
	// Like every read, the query is replicated and decided from the
	// committed log; followers never answer from local state.
	s.propose(w, fsm.Command{Type: fsm.CmdCheckpointQuery, Task: task, Generation: gen, Now: time.Now()})
}
