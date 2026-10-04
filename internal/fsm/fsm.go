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
	"sort"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

// Command types carried in Raft log entries.
const (
	CmdAcquire        = "acquire"
	CmdRenew          = "renew"
	CmdRelease        = "release"
	CmdQueryLease     = "query_lease"
	CmdBarrierCreate  = "barrier_create"
	CmdBarrierArrive  = "barrier_arrive"
	CmdBarrierAdvance = "barrier_advance"
	CmdBarrierQuery   = "barrier_query"

	CmdCheckpointCreate  = "checkpoint_create"
	CmdCheckpointSubmit  = "checkpoint_submit"
	CmdCheckpointPublish = "checkpoint_publish"
	CmdCheckpointQuery   = "checkpoint_query"
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

	// Barrier fields (used by barrier commands only).
	Barrier      string   `json:"barrier,omitempty"`
	Round        uint64   `json:"round,omitempty"`
	Participants []string `json:"participants,omitempty"`
	Participant  string   `json:"participant,omitempty"`

	// Checkpoint fields (used by checkpoint commands only).
	Task       string `json:"task,omitempty"`
	Generation uint64 `json:"generation,omitempty"`
	URI        string `json:"uri,omitempty"`
	SHA256     string `json:"sha256,omitempty"`
	Bytes      uint64 `json:"bytes"`
}

// Barrier lifecycle states.
const (
	BarrierWaiting   = "waiting"
	BarrierCompleted = "completed"
	BarrierFailed    = "failed"
)

// BarrierRegistration records one participant's arrival for a round.
type BarrierRegistration struct {
	Participant string `json:"participant"`
	Resource    string `json:"resource"`
	Holder      string `json:"holder"`
	Token       uint64 `json:"token"`
}

// Barrier is a named, recoverable phase barrier with fixed membership.
type Barrier struct {
	Name         string                         `json:"name"`
	Participants []string                       `json:"participants"` // sorted, fixed
	Round        uint64                         `json:"round"`
	Status       string                         `json:"status"`
	Arrivals     map[string]BarrierRegistration `json:"arrivals"`
	FailReason   string                         `json:"fail_reason,omitempty"`

	// BoundTask/BoundGeneration name the checkpoint this round is bound
	// to; empty when unbound. Cleared on advance.
	BoundTask       string `json:"bound_task,omitempty"`
	BoundGeneration uint64 `json:"bound_generation,omitempty"`
}

// BarrierView is the externally visible, copy-safe form of a Barrier.
type BarrierView struct {
	Name         string   `json:"name"`
	Round        uint64   `json:"round"`
	Status       string   `json:"status"`
	Participants []string `json:"participants"`
	Arrived      []string `json:"arrived"`
	FailReason   string   `json:"fail_reason,omitempty"`
}

func (b *Barrier) view() *BarrierView {
	arrived := make([]string, 0, len(b.Arrivals))
	for p := range b.Arrivals {
		arrived = append(arrived, p)
	}
	sort.Strings(arrived)
	return &BarrierView{
		Name:         b.Name,
		Round:        b.Round,
		Status:       b.Status,
		Participants: append([]string(nil), b.Participants...),
		Arrived:      arrived,
		FailReason:   b.FailReason,
	}
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

	Barrier *BarrierView `json:"barrier,omitempty"`

	Checkpoint *CheckpointView `json:"checkpoint,omitempty"`
}

// FSM is the replicated lease-lock state machine.
type FSM struct {
	mu sync.RWMutex
	// leases holds currently valid (possibly not-yet-expired) leases.
	leases map[string]*Lease
	// tokenBounds is the per-resource historical token upper bound.
	// It is monotonic and never removed, even when a lock is released.
	tokenBounds map[string]uint64
	// barriers holds named phase barriers, keyed by name.
	barriers map[string]*Barrier
	// checkpoints holds checkpoint descriptors, keyed by task then generation.
	checkpoints map[string]map[uint64]*Checkpoint
	// latestPublished is the latest published generation per task.
	latestPublished map[string]uint64
}

func New() *FSM {
	return &FSM{
		leases:          make(map[string]*Lease),
		tokenBounds:     make(map[string]uint64),
		barriers:        make(map[string]*Barrier),
		checkpoints:     make(map[string]map[uint64]*Checkpoint),
		latestPublished: make(map[string]uint64),
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

	case CmdQueryLease:
		// Read-only, but decided from the committed log like every other
		// command: the leader's Now is replayed identically on replicas.
		if l := f.liveLease(cmd.Resource, cmd.Now); l != nil {
			return Result{OK: true, Holder: l.Holder, Token: l.Token, Expiry: l.Expiry.Unix()}
		}
		return Result{OK: false}

	case CmdBarrierCreate:
		return f.applyBarrierCreate(cmd)
	case CmdBarrierArrive:
		return f.applyBarrierArrive(cmd)
	case CmdBarrierAdvance:
		return f.applyBarrierAdvance(cmd)
	case CmdBarrierQuery:
		f.checkBarrierLiveness(cmd.Now)
		b := f.barriers[cmd.Barrier]
		if b == nil {
			return Result{OK: false, Err: "barrier not found"}
		}
		return Result{OK: true, Barrier: b.view()}

	case CmdCheckpointCreate:
		return f.applyCheckpointCreate(cmd)
	case CmdCheckpointSubmit:
		return f.applyCheckpointSubmit(cmd)
	case CmdCheckpointPublish:
		return f.applyCheckpointPublish(cmd)
	case CmdCheckpointQuery:
		return f.applyCheckpointQuery(cmd)

	default:
		return Result{Err: fmt.Sprintf("unknown command type %q", cmd.Type)}
	}
}

// checkBarrierLiveness fails any waiting barrier whose recorded arrival
// leases are no longer valid (released, expired, or re-acquired with a new
// token) as of the command's decision time. It runs on every barrier
// command, so a waiting barrier's invalidation is decided no later than
// the next barrier operation.
func (f *FSM) checkBarrierLiveness(now time.Time) {
	// Iterate in sorted order so every replica records the identical
	// failure when several barriers or arrivals go invalid at once;
	// raw map order would diverge across replicas.
	names := make([]string, 0, len(f.barriers))
	for name := range f.barriers {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		b := f.barriers[name]
		if b.Status != BarrierWaiting {
			continue
		}
		parts := make([]string, 0, len(b.Arrivals))
		for p := range b.Arrivals {
			parts = append(parts, p)
		}
		sort.Strings(parts)
		for _, p := range parts {
			reg := b.Arrivals[p]
			if !f.registrationLeaseValid(reg, now) {
				b.Status = BarrierFailed
				b.FailReason = fmt.Sprintf("lease for participant %q on resource %q is no longer valid", reg.Participant, reg.Resource)
				break
			}
		}
	}
}

// registrationLeaseValid reports whether the lease recorded in reg is
// still held by the same holder with the same token at now.
func (f *FSM) registrationLeaseValid(reg BarrierRegistration, now time.Time) bool {
	l := f.liveLease(reg.Resource, now)
	return l != nil && l.Holder == reg.Holder && l.Token == reg.Token
}

func sameParticipants(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (f *FSM) applyBarrierCreate(cmd Command) Result {
	f.checkBarrierLiveness(cmd.Now)
	if b := f.barriers[cmd.Barrier]; b != nil {
		// Same name and same configuration: idempotent success.
		incoming := append([]string(nil), cmd.Participants...)
		sort.Strings(incoming)
		if sameParticipants(b.Participants, incoming) {
			return Result{OK: true, Barrier: b.view()}
		}
		return Result{OK: false, Err: "barrier exists with a different configuration", Barrier: b.view()}
	}
	if len(cmd.Participants) < 2 || len(cmd.Participants) > 16 {
		return Result{OK: false, Err: "participants must contain 2 to 16 distinct IDs"}
	}
	parts := append([]string(nil), cmd.Participants...)
	sort.Strings(parts)
	seen := make(map[string]bool, len(parts))
	for _, p := range parts {
		if p == "" || seen[p] {
			return Result{OK: false, Err: "participants must be distinct non-empty IDs"}
		}
		seen[p] = true
	}
	b := &Barrier{
		Name:         cmd.Barrier,
		Participants: parts,
		Round:        1,
		Status:       BarrierWaiting,
		Arrivals:     make(map[string]BarrierRegistration),
	}
	f.barriers[b.Name] = b
	return Result{OK: true, Barrier: b.view()}
}

func (f *FSM) applyBarrierArrive(cmd Command) Result {
	f.checkBarrierLiveness(cmd.Now)
	b := f.barriers[cmd.Barrier]
	if b == nil {
		return Result{OK: false, Err: "barrier not found"}
	}
	if cmd.Round != b.Round {
		return Result{OK: false, Err: fmt.Sprintf("stale round %d, current round is %d", cmd.Round, b.Round), Barrier: b.view()}
	}
	if b.Status != BarrierWaiting {
		return Result{OK: false, Err: "round already " + b.Status + ", arrivals are closed", Barrier: b.view()}
	}
	if b.BoundTask != "" {
		// A checkpoint-bound round only accepts arrivals carrying shard
		// metadata via checkpoint_submit, so publish can never be
		// short-circuited by shard-less arrivals.
		return Result{OK: false, Err: "round is bound to a checkpoint; use checkpoint submit", Barrier: b.view()}
	}
	member := false
	for _, p := range b.Participants {
		if p == cmd.Participant {
			member = true
			break
		}
	}
	if !member {
		return Result{OK: false, Err: "unknown participant", Barrier: b.view()}
	}
	reg := BarrierRegistration{
		Participant: cmd.Participant,
		Resource:    cmd.Resource,
		Holder:      cmd.Holder,
		Token:       cmd.Token,
	}
	if prev, ok := b.Arrivals[cmd.Participant]; ok {
		// Identical retransmission: idempotent, never double-counted.
		if prev == reg {
			return Result{OK: true, Barrier: b.view()}
		}
		return Result{OK: false, Err: "conflicting registration for participant", Barrier: b.view()}
	}
	for _, other := range b.Arrivals {
		if other.Resource == cmd.Resource {
			return Result{OK: false, Err: "resource already registered by another participant this round", Barrier: b.view()}
		}
	}
	if !f.registrationLeaseValid(reg, cmd.Now) {
		return Result{OK: false, Err: "no matching valid lease for resource/holder/token", Barrier: b.view()}
	}
	b.Arrivals[cmd.Participant] = reg
	if len(b.Arrivals) == len(b.Participants) {
		// All arrived: re-validate every registered lease in this same
		// commit before completing.
		for _, r := range sortedArrivals(b) {
			if !f.registrationLeaseValid(r, cmd.Now) {
				b.Status = BarrierFailed
				b.FailReason = fmt.Sprintf("lease for participant %q on resource %q invalid at completion", r.Participant, r.Resource)
				return Result{OK: false, Err: b.FailReason, Barrier: b.view()}
			}
		}
		b.Status = BarrierCompleted
	}
	return Result{OK: true, Barrier: b.view()}
}

func (f *FSM) applyBarrierAdvance(cmd Command) Result {
	f.checkBarrierLiveness(cmd.Now)
	b := f.barriers[cmd.Barrier]
	if b == nil {
		return Result{OK: false, Err: "barrier not found"}
	}
	if cmd.Round != b.Round {
		// A concurrent advance already moved the round; only one wins.
		return Result{OK: false, Err: fmt.Sprintf("expected round %d, current round is %d", cmd.Round, b.Round), Barrier: b.view()}
	}
	if b.Status == BarrierWaiting {
		return Result{OK: false, Err: "round still waiting for arrivals", Barrier: b.view()}
	}
	// Terminal state only: open the next round and clear arrival records.
	b.Round++
	b.Status = BarrierWaiting
	b.Arrivals = make(map[string]BarrierRegistration)
	b.FailReason = ""
	b.BoundTask = ""
	b.BoundGeneration = 0
	return Result{OK: true, Barrier: b.view()}
}

// sortedArrivals returns arrival registrations ordered by participant ID,
// keeping completion checks deterministic across replicas.
func sortedArrivals(b *Barrier) []BarrierRegistration {
	parts := make([]string, 0, len(b.Arrivals))
	for p := range b.Arrivals {
		parts = append(parts, p)
	}
	sort.Strings(parts)
	regs := make([]BarrierRegistration, 0, len(parts))
	for _, p := range parts {
		regs = append(regs, b.Arrivals[p])
	}
	return regs
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
	// Barriers is absent in older snapshots; Restore treats it as empty.
	Barriers map[string]*Barrier `json:"barriers,omitempty"`
	// Checkpoints and LatestPublished are absent in older snapshots;
	// Restore treats them as empty.
	Checkpoints     map[string]map[uint64]*Checkpoint `json:"checkpoints,omitempty"`
	LatestPublished map[string]uint64                 `json:"latest_published,omitempty"`
}

// Snapshot captures leases, token upper bounds, barrier and checkpoint state.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	s := snapshot{
		Leases:          make(map[string]*Lease, len(f.leases)),
		TokenBounds:     make(map[string]uint64, len(f.tokenBounds)),
		Barriers:        make(map[string]*Barrier, len(f.barriers)),
		Checkpoints:     make(map[string]map[uint64]*Checkpoint, len(f.checkpoints)),
		LatestPublished: make(map[string]uint64, len(f.latestPublished)),
	}
	for k, v := range f.leases {
		cp := *v
		s.Leases[k] = &cp
	}
	for k, v := range f.tokenBounds {
		s.TokenBounds[k] = v
	}
	for k, v := range f.barriers {
		cp := *v
		cp.Participants = append([]string(nil), v.Participants...)
		cp.Arrivals = make(map[string]BarrierRegistration, len(v.Arrivals))
		for p, reg := range v.Arrivals {
			cp.Arrivals[p] = reg
		}
		s.Barriers[k] = &cp
	}
	for task, gens := range f.checkpoints {
		cpGens := make(map[uint64]*Checkpoint, len(gens))
		for gen, cp := range gens {
			cpGens[gen] = cp.deepCopy()
		}
		s.Checkpoints[task] = cpGens
	}
	for task, gen := range f.latestPublished {
		s.LatestPublished[task] = gen
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
	f.barriers = s.Barriers
	if f.barriers == nil {
		// Old snapshots carry no barrier state.
		f.barriers = make(map[string]*Barrier)
	}
	for _, b := range f.barriers {
		if b.Arrivals == nil {
			b.Arrivals = make(map[string]BarrierRegistration)
		}
	}
	f.checkpoints = s.Checkpoints
	if f.checkpoints == nil {
		// Old snapshots carry no checkpoint state.
		f.checkpoints = make(map[string]map[uint64]*Checkpoint)
	}
	for task, gens := range f.checkpoints {
		if gens == nil {
			gens = make(map[uint64]*Checkpoint)
			f.checkpoints[task] = gens
		}
		for _, cp := range gens {
			if cp.Shards == nil {
				cp.Shards = make(map[string]ShardMeta)
			}
		}
	}
	f.latestPublished = s.LatestPublished
	if f.latestPublished == nil {
		f.latestPublished = make(map[string]uint64)
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
