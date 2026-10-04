// Package fsm implements the lease-lock state machine replicated by Raft.
//
// All decisions are made purely from the committed log: the leader stamps
// each command with its decision time (Now), and replicas replay commands
// without reading their own clocks.
package fsm

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

// Command types carried in Raft log entries.
const (
	CmdAcquire = "acquire"
	CmdRenew   = "renew"
	CmdRelease = "release"
)

// Command is the unit of replication. Now is the decision time written by
// the leader before proposing; replicas must use it verbatim.
type Command struct {
	Type     string    `json:"type"`
	Resource string    `json:"resource"`
	Holder   string    `json:"holder"`
	Token    uint64    `json:"token"`
	TTL      int64     `json:"ttl"` // seconds
	Now      time.Time `json:"now"`
}

// Lease is an active lock on a resource.
type Lease struct {
	Holder string    `json:"holder"`
	Token  uint64    `json:"token"`
	Expiry time.Time `json:"expiry"`
}

// Result is the deterministic outcome of applying a Command.
type Result struct {
	OK     bool   `json:"ok"`
	Holder string `json:"holder,omitempty"`
	Token  uint64 `json:"token,omitempty"`
	Expiry int64  `json:"expiry,omitempty"` // unix seconds
	Err    string `json:"err,omitempty"`
}

// FSM is the replicated lease-lock state machine.
type FSM struct {
	mu sync.RWMutex
	// leases holds currently valid (possibly not-yet-expired) leases.
	leases map[string]*Lease
	// tokenBounds is the per-resource historical token upper bound.
	// It is monotonic and never removed, even when a lock is released.
	tokenBounds map[string]uint64
}

func New() *FSM {
	return &FSM{
		leases:      make(map[string]*Lease),
		tokenBounds: make(map[string]uint64),
	}
}

func (f *FSM) liveLease(resource string, now time.Time) *Lease {
	l, ok := f.leases[resource]
	if !ok {
		return nil
	}
	if !now.Before(l.Expiry) {
		return nil // expired
	}
	return l
}

// Apply executes a committed command in log order.
func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return Result{Err: "corrupt command: " + err.Error()}
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch cmd.Type {
	case CmdAcquire:
		if l := f.liveLease(cmd.Resource, cmd.Now); l != nil {
			return Result{
				OK:     false,
				Holder: l.Holder,
				Token:  l.Token,
				Expiry: l.Expiry.Unix(),
				Err:    "resource is locked",
			}
		}
		token := f.tokenBounds[cmd.Resource] + 1
		f.tokenBounds[cmd.Resource] = token
		expiry := cmd.Now.Add(time.Duration(cmd.TTL) * time.Second)
		f.leases[cmd.Resource] = &Lease{Holder: cmd.Holder, Token: token, Expiry: expiry}
		return Result{OK: true, Holder: cmd.Holder, Token: token, Expiry: expiry.Unix()}

	case CmdRenew, CmdRelease:
		l := f.liveLease(cmd.Resource, cmd.Now)
		if l == nil {
			return Result{OK: false, Err: "no valid lease"}
		}
		if l.Holder != cmd.Holder || l.Token != cmd.Token {
			return Result{OK: false, Err: "holder or token mismatch"}
		}
		if cmd.Type == CmdRenew {
			// Recompute the deadline from the decision moment.
			l.Expiry = cmd.Now.Add(time.Duration(cmd.TTL) * time.Second)
			return Result{OK: true, Holder: l.Holder, Token: l.Token, Expiry: l.Expiry.Unix()}
		}
		// Release: drop the lease but keep the token upper bound.
		delete(f.leases, cmd.Resource)
		return Result{OK: true}

	default:
		return Result{Err: fmt.Sprintf("unknown command type %q", cmd.Type)}
	}
}

// Query returns the live lease for a resource as of now, or nil.
func (f *FSM) Query(resource string, now time.Time) *Lease {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if l := f.liveLease(resource, now); l != nil {
		cp := *l
		return &cp
	}
	return nil
}

// TokenBound returns the historical token upper bound for a resource.
func (f *FSM) TokenBound(resource string) uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.tokenBounds[resource]
}

// snapshot is the persisted form of the FSM.
type snapshot struct {
	Leases      map[string]*Lease `json:"leases"`
	TokenBounds map[string]uint64 `json:"token_bounds"`
}

// Snapshot captures leases plus token upper bounds.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	s := snapshot{
		Leases:      make(map[string]*Lease, len(f.leases)),
		TokenBounds: make(map[string]uint64, len(f.tokenBounds)),
	}
	for k, v := range f.leases {
		cp := *v
		s.Leases[k] = &cp
	}
	for k, v := range f.tokenBounds {
		s.TokenBounds[k] = v
	}
	return &fsmSnapshot{state: s}, nil
}

// Restore replaces the FSM state from a snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var s snapshot
	if err := json.NewDecoder(rc).Decode(&s); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leases = s.Leases
	if f.leases == nil {
		f.leases = make(map[string]*Lease)
	}
	f.tokenBounds = s.TokenBounds
	if f.tokenBounds == nil {
		f.tokenBounds = make(map[string]uint64)
	}
	return nil
}

type fsmSnapshot struct {
	state snapshot
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := json.NewEncoder(sink).Encode(s.state)
	if err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
