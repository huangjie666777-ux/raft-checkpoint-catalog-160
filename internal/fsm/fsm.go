// Package fsm implements the lease-lock state machine replicated by Raft.
//
// All decisions are made purely from the committed log: the leader stamps
// each command with its decision time (Now), and replicas replay commands
// without reading their own clocks.
package fsm

import (
	"encoding/hex"
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
	CmdAcquire            = "acquire"
	CmdRenew              = "renew"
	CmdRelease            = "release"
	CmdQueryLease         = "query_lease"
	CmdBarrierCreate      = "barrier_create"
	CmdBarrierArrive      = "barrier_arrive"
	CmdBarrierAdvance     = "barrier_advance"
	CmdBarrierQuery       = "barrier_query"
	CmdCheckpointCreate   = "checkpoint_create"
	CmdCheckpointRegister = "checkpoint_register"
	CmdCheckpointPublish  = "checkpoint_publish"
	CmdCheckpointQuery    = "checkpoint_query"
	CmdCheckpointLatest   = "checkpoint_latest"
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

	Task       string `json:"task,omitempty"`
	Generation uint64 `json:"generation,omitempty"`
	URI        string `json:"uri,omitempty"`
	SHA256     string `json:"sha256,omitempty"`
	Size       uint64 `json:"size,omitempty"`
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

// CheckpointShard is participant-supplied shard metadata. The FSM never
// reads URI; it stores only metadata.
type CheckpointShard struct {
	Participant string `json:"participant"`
	Resource    string `json:"resource"`
	Holder      string `json:"holder"`
	Token       uint64 `json:"token"`
	URI         string `json:"uri"`
	SHA256      string `json:"sha256"`
	Size        uint64 `json:"size"`
}

type ShardView struct {
	Participant string `json:"participant"`
	URI         string `json:"uri"`
	SHA256      string `json:"sha256"`
	Size        uint64 `json:"size"`
}

type ManifestView struct {
	Task       string      `json:"task"`
	Generation uint64      `json:"generation"`
	Barrier    string      `json:"barrier"`
	Round      uint64      `json:"round"`
	Shards     []ShardView `json:"shards"`
}

type Checkpoint struct {
	Task       string                     `json:"task"`
	Generation uint64                     `json:"generation"`
	Barrier    string                     `json:"barrier"`
	Round      uint64                     `json:"round"`
	Candidates map[string]CheckpointShard `json:"candidates"`
	Published  *ManifestView              `json:"published,omitempty"`
}

type CheckpointView struct {
	Task        string        `json:"task"`
	Generation  uint64        `json:"generation"`
	Barrier     string        `json:"barrier"`
	Round       uint64        `json:"round"`
	Published   bool          `json:"published"`
	Manifest    *ManifestView `json:"manifest,omitempty"`
	BarrierView *BarrierView  `json:"barrier_view,omitempty"`
}

func (c *Checkpoint) view() *CheckpointView {
	return &CheckpointView{
		Task:        c.Task,
		Generation:  c.Generation,
		Barrier:     c.Barrier,
		Round:       c.Round,
		Published:   c.Published != nil,
		Manifest:    c.Published,
		BarrierView: nil,
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

	Barrier    *BarrierView    `json:"barrier,omitempty"`
	Checkpoint *CheckpointView `json:"checkpoint,omitempty"`
	Manifest   *ManifestView   `json:"manifest,omitempty"`
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
	// checkpoints is keyed by task and generation.
	checkpoints map[string]map[uint64]*Checkpoint
	// checkpointByRound enforces one checkpoint for each barrier round.
	checkpointByRound map[string]map[uint64]*Checkpoint
	latestGeneration  map[string]uint64
	latestManifest    map[string]*ManifestView
}

func New() *FSM {
	return &FSM{
		leases:            make(map[string]*Lease),
		tokenBounds:       make(map[string]uint64),
		barriers:          make(map[string]*Barrier),
		checkpoints:       make(map[string]map[uint64]*Checkpoint),
		checkpointByRound: make(map[string]map[uint64]*Checkpoint),
		latestGeneration:  make(map[string]uint64),
		latestManifest:    make(map[string]*ManifestView),
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
	case CmdCheckpointRegister:
		return f.applyCheckpointRegister(cmd)
	case CmdCheckpointPublish:
		return f.applyCheckpointPublish(cmd)
	case CmdCheckpointQuery:
		return f.applyCheckpointQuery(cmd)
	case CmdCheckpointLatest:
		return f.applyCheckpointLatest(cmd)

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
		participants := make([]string, 0, len(b.Arrivals))
		for participant := range b.Arrivals {
			participants = append(participants, participant)
		}
		sort.Strings(participants)
		for _, participant := range participants {
			reg := b.Arrivals[participant]
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
	if cp := f.checkpointByRound[b.Name][b.Round]; cp != nil {
		return Result{OK: false, Err: "barrier round is bound to a checkpoint; use checkpoint registration", Barrier: b.view(), Checkpoint: cp.view()}
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
		for _, r := range b.Arrivals {
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
	return Result{OK: true, Barrier: b.view()}
}

func validCheckpointSHA256(sum string) bool {
	if len(sum) != 64 {
		return false
	}
	if _, err := hex.DecodeString(sum); err != nil {
		return false
	}
	for _, ch := range sum {
		if !(ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'f') {
			return false
		}
	}
	return true
}

func checkpointResult(cp *Checkpoint, b *Barrier) Result {
	view := cp.view()
	if b != nil {
		view.BarrierView = b.view()
	}
	return Result{OK: true, Barrier: view.BarrierView, Checkpoint: view, Manifest: cp.Published}
}

func (f *FSM) applyCheckpointCreate(cmd Command) Result {
	f.checkBarrierLiveness(cmd.Now)
	if cmd.Task == "" || cmd.Generation == 0 {
		return Result{OK: false, Err: "task and positive generation are required"}
	}
	b := f.barriers[cmd.Barrier]
	if b == nil {
		return Result{OK: false, Err: "barrier not found"}
	}
	byTask := f.checkpoints[cmd.Task]
	if byTask != nil {
		if cp := byTask[cmd.Generation]; cp != nil {
			if cp.Barrier == cmd.Barrier && cp.Round == b.Round {
				return checkpointResult(cp, b)
			}
			return Result{OK: false, Err: "checkpoint exists with a different barrier binding", Checkpoint: cp.view(), Barrier: b.view()}
		}
	}
	if b.Status != BarrierWaiting {
		return Result{OK: false, Err: "barrier round is not waiting for bindings", Barrier: b.view()}
	}
	roundBindings := f.checkpointByRound[cmd.Barrier]
	if existing := roundBindings[b.Round]; existing != nil {
		return Result{OK: false, Err: fmt.Sprintf("barrier round is already bound to task %q generation %d", existing.Task, existing.Generation), Checkpoint: existing.view()}
	}
	cp := &Checkpoint{
		Task:       cmd.Task,
		Generation: cmd.Generation,
		Barrier:    cmd.Barrier,
		Round:      b.Round,
		Candidates: make(map[string]CheckpointShard),
	}
	if f.checkpoints[cmd.Task] == nil {
		f.checkpoints[cmd.Task] = make(map[uint64]*Checkpoint)
	}
	f.checkpoints[cmd.Task][cmd.Generation] = cp
	if f.checkpointByRound[cmd.Barrier] == nil {
		f.checkpointByRound[cmd.Barrier] = make(map[uint64]*Checkpoint)
	}
	f.checkpointByRound[cmd.Barrier][b.Round] = cp
	return checkpointResult(cp, b)
}

func (f *FSM) applyCheckpointRegister(cmd Command) Result {
	f.checkBarrierLiveness(cmd.Now)
	if cmd.Task == "" || cmd.Generation == 0 || cmd.Participant == "" ||
		cmd.Resource == "" || cmd.Holder == "" || cmd.URI == "" {
		return Result{OK: false, Err: "task, generation, participant, resource, holder and uri are required"}
	}
	if !validCheckpointSHA256(cmd.SHA256) {
		return Result{OK: false, Err: "sha256 must be 64 lowercase hexadecimal characters"}
	}
	byTask := f.checkpoints[cmd.Task]
	if byTask == nil || byTask[cmd.Generation] == nil {
		return Result{OK: false, Err: "checkpoint not found"}
	}
	cp := byTask[cmd.Generation]
	if cp.Published != nil {
		return Result{OK: false, Err: "checkpoint is already published", Checkpoint: cp.view(), Manifest: cp.Published}
	}
	b := f.barriers[cp.Barrier]
	if b == nil {
		return Result{OK: false, Err: "bound barrier no longer exists", Checkpoint: cp.view()}
	}
	if b.Round != cp.Round {
		return Result{OK: false, Err: "bound barrier round has advanced", Barrier: b.view(), Checkpoint: cp.view()}
	}
	if b.Status != BarrierWaiting && b.Status != BarrierCompleted {
		return Result{OK: false, Err: "bound barrier round is " + b.Status, Barrier: b.view(), Checkpoint: cp.view()}
	}
	member := false
	for _, p := range b.Participants {
		if p == cmd.Participant {
			member = true
			break
		}
	}
	if !member {
		return Result{OK: false, Err: "unknown participant", Barrier: b.view(), Checkpoint: cp.view()}
	}
	shard := CheckpointShard{
		Participant: cmd.Participant,
		Resource:    cmd.Resource,
		Holder:      cmd.Holder,
		Token:       cmd.Token,
		URI:         cmd.URI,
		SHA256:      cmd.SHA256,
		Size:        cmd.Size,
	}
	if prev, exists := cp.Candidates[cmd.Participant]; exists {
		if prev == shard {
			return checkpointResult(cp, b)
		}
		return Result{OK: false, Err: "conflicting shard metadata for participant", Barrier: b.view(), Checkpoint: cp.view()}
	}
	reg := BarrierRegistration{Participant: cmd.Participant, Resource: cmd.Resource, Holder: cmd.Holder, Token: cmd.Token}
	if arrived, exists := b.Arrivals[cmd.Participant]; exists {
		if arrived != reg {
			return Result{OK: false, Err: "conflicting barrier registration for participant", Barrier: b.view(), Checkpoint: cp.view()}
		}
	} else {
		for _, other := range b.Arrivals {
			if other.Resource == cmd.Resource {
				return Result{OK: false, Err: "resource already registered by another participant this round", Barrier: b.view(), Checkpoint: cp.view()}
			}
		}
		if !f.registrationLeaseValid(reg, cmd.Now) {
			return Result{OK: false, Err: "no matching valid lease for resource/holder/token", Barrier: b.view(), Checkpoint: cp.view()}
		}
		b.Arrivals[cmd.Participant] = reg
	}
	cp.Candidates[cmd.Participant] = shard
	if len(b.Arrivals) == len(b.Participants) {
		for _, p := range b.Participants {
			r := b.Arrivals[p]
			if !f.registrationLeaseValid(r, cmd.Now) {
				b.Status = BarrierFailed
				b.FailReason = fmt.Sprintf("lease for participant %q on resource %q invalid at completion", r.Participant, r.Resource)
				return Result{OK: false, Err: b.FailReason, Barrier: b.view(), Checkpoint: cp.view()}
			}
		}
		b.Status = BarrierCompleted
	}
	return checkpointResult(cp, b)
}

func (f *FSM) manifestFromCheckpoint(cp *Checkpoint) (*ManifestView, bool) {
	b := f.barriers[cp.Barrier]
	if b == nil || len(cp.Candidates) != len(b.Participants) {
		return nil, false
	}
	shards := make([]ShardView, 0, len(cp.Candidates))
	for _, shard := range cp.Candidates {
		shards = append(shards, ShardView{Participant: shard.Participant, URI: shard.URI, SHA256: shard.SHA256, Size: shard.Size})
	}
	sort.Slice(shards, func(i, j int) bool { return shards[i].Participant < shards[j].Participant })
	return &ManifestView{Task: cp.Task, Generation: cp.Generation, Barrier: cp.Barrier, Round: cp.Round, Shards: shards}, true
}

func (f *FSM) applyCheckpointPublish(cmd Command) Result {
	byTask := f.checkpoints[cmd.Task]
	if byTask == nil || byTask[cmd.Generation] == nil {
		return Result{OK: false, Err: "checkpoint not found"}
	}
	cp := byTask[cmd.Generation]
	if cp.Published != nil {
		return Result{OK: true, Checkpoint: cp.view(), Manifest: cp.Published}
	}
	if latest := f.latestGeneration[cmd.Task]; cmd.Generation <= latest {
		return Result{OK: false, Err: fmt.Sprintf("generation %d is not greater than latest published generation %d", cmd.Generation, latest), Manifest: f.latestManifest[cmd.Task]}
	}
	b := f.barriers[cp.Barrier]
	if b == nil {
		return Result{OK: false, Err: "bound barrier no longer exists", Checkpoint: cp.view()}
	}
	if b.Round != cp.Round {
		return Result{OK: false, Err: "bound barrier round has advanced", Barrier: b.view(), Checkpoint: cp.view()}
	}
	if b.Status != BarrierCompleted {
		return Result{OK: false, Err: "bound barrier round is not complete", Barrier: b.view(), Checkpoint: cp.view()}
	}
	if len(cp.Candidates) != len(b.Participants) {
		return Result{OK: false, Err: "not every participant has shard metadata", Barrier: b.view(), Checkpoint: cp.view()}
	}
	for _, p := range b.Participants {
		arrival, arrived := b.Arrivals[p]
		shard, hasShard := cp.Candidates[p]
		if !arrived || !hasShard || arrival.Resource != shard.Resource || arrival.Holder != shard.Holder || arrival.Token != shard.Token {
			return Result{OK: false, Err: "shard metadata does not cover every barrier arrival", Barrier: b.view(), Checkpoint: cp.view()}
		}
	}
	manifest, ok := f.manifestFromCheckpoint(cp)
	if !ok {
		return Result{OK: false, Err: "not every participant has shard metadata"}
	}
	cp.Published = manifest
	f.latestGeneration[cmd.Task] = cp.Generation
	f.latestManifest[cmd.Task] = manifest
	return Result{OK: true, Checkpoint: cp.view(), Manifest: manifest}
}

func (f *FSM) applyCheckpointQuery(cmd Command) Result {
	byTask := f.checkpoints[cmd.Task]
	if byTask == nil || byTask[cmd.Generation] == nil {
		return Result{OK: false, Err: "checkpoint not found"}
	}
	cp := byTask[cmd.Generation]
	return Result{OK: true, Checkpoint: cp.view(), Manifest: cp.Published}
}

func (f *FSM) applyCheckpointLatest(cmd Command) Result {
	manifest := f.latestManifest[cmd.Task]
	if manifest == nil {
		return Result{OK: false, Err: "no published checkpoint for task"}
	}
	return Result{OK: true, Manifest: manifest}
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
	Barriers         map[string]*Barrier               `json:"barriers,omitempty"`
	Checkpoints      map[string]map[uint64]*Checkpoint `json:"checkpoints,omitempty"`
	LatestGeneration map[string]uint64                 `json:"latest_generation,omitempty"`
	LatestManifest   map[string]*ManifestView          `json:"latest_manifest,omitempty"`
}

// Snapshot captures leases, token upper bounds and barrier state.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	s := snapshot{
		Leases:           make(map[string]*Lease, len(f.leases)),
		TokenBounds:      make(map[string]uint64, len(f.tokenBounds)),
		Barriers:         make(map[string]*Barrier, len(f.barriers)),
		Checkpoints:      make(map[string]map[uint64]*Checkpoint, len(f.checkpoints)),
		LatestGeneration: make(map[string]uint64, len(f.latestGeneration)),
		LatestManifest:   make(map[string]*ManifestView, len(f.latestManifest)),
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
	for task, generations := range f.checkpoints {
		copied := make(map[uint64]*Checkpoint, len(generations))
		for generation, checkpoint := range generations {
			cp := *checkpoint
			cp.Candidates = make(map[string]CheckpointShard, len(checkpoint.Candidates))
			for participant, shard := range checkpoint.Candidates {
				cp.Candidates[participant] = shard
			}
			if checkpoint.Published != nil {
				manifest := *checkpoint.Published
				manifest.Shards = append([]ShardView(nil), checkpoint.Published.Shards...)
				cp.Published = &manifest
			}
			copied[generation] = &cp
		}
		s.Checkpoints[task] = copied
	}
	for task, generation := range f.latestGeneration {
		s.LatestGeneration[task] = generation
	}
	for task, manifest := range f.latestManifest {
		cp := *manifest
		cp.Shards = append([]ShardView(nil), manifest.Shards...)
		s.LatestManifest[task] = &cp
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
		f.checkpoints = make(map[string]map[uint64]*Checkpoint)
	}
	f.latestGeneration = s.LatestGeneration
	if f.latestGeneration == nil {
		f.latestGeneration = make(map[string]uint64)
	}
	f.latestManifest = s.LatestManifest
	if f.latestManifest == nil {
		f.latestManifest = make(map[string]*ManifestView)
	}
	f.checkpointByRound = make(map[string]map[uint64]*Checkpoint)
	for task, generations := range f.checkpoints {
		for generation, cp := range generations {
			if cp == nil {
				delete(generations, generation)
				continue
			}
			if cp.Candidates == nil {
				cp.Candidates = make(map[string]CheckpointShard)
			}
			if f.checkpointByRound[cp.Barrier] == nil {
				f.checkpointByRound[cp.Barrier] = make(map[uint64]*Checkpoint)
			}
			f.checkpointByRound[cp.Barrier][cp.Round] = cp
		}
		if len(generations) == 0 {
			delete(f.checkpoints, task)
		}
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
