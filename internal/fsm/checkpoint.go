// Checkpoint directory: replicated metadata registry that lets restarted
// compute tasks recover the full shard manifest of their latest published
// generation. Only metadata is stored; shard payloads are never touched.
package fsm

import (
	"fmt"
	"sort"
)

// ShardMeta is the registered metadata of one participant's shard.
type ShardMeta struct {
	URI    string `json:"uri"`
	SHA256 string `json:"sha256"`
	Bytes  uint64 `json:"bytes"`
}

// ShardEntry is one manifest line, pairing a participant with its shard.
type ShardEntry struct {
	Participant string `json:"participant"`
	URI         string `json:"uri"`
	SHA256      string `json:"sha256"`
	Bytes       uint64 `json:"bytes"`
}

// Checkpoint binds a task generation to one barrier round and accumulates
// candidate shard metadata until published.
type Checkpoint struct {
	Task       string               `json:"task"`
	Generation uint64               `json:"generation"`
	Barrier    string               `json:"barrier"`
	Round      uint64               `json:"round"`
	Shards     map[string]ShardMeta `json:"shards"`
	Published  bool                 `json:"published"`
	Manifest   []ShardEntry         `json:"manifest,omitempty"`
}

func (c *Checkpoint) deepCopy() *Checkpoint {
	cp := *c
	cp.Shards = make(map[string]ShardMeta, len(c.Shards))
	for p, m := range c.Shards {
		cp.Shards[p] = m
	}
	cp.Manifest = append([]ShardEntry(nil), c.Manifest...)
	return &cp
}

// CheckpointView is the externally visible, copy-safe form of a Checkpoint.
type CheckpointView struct {
	Task       string               `json:"task"`
	Generation uint64               `json:"generation"`
	Barrier    string               `json:"barrier"`
	Round      uint64               `json:"round"`
	Published  bool                 `json:"published"`
	Shards     map[string]ShardMeta `json:"shards"`
	Manifest   []ShardEntry         `json:"manifest,omitempty"`
	// LatestGeneration is the task's latest published generation.
	LatestGeneration uint64 `json:"latest_generation"`
}

func (f *FSM) checkpointView(c *Checkpoint) *CheckpointView {
	shards := make(map[string]ShardMeta, len(c.Shards))
	for p, m := range c.Shards {
		shards[p] = m
	}
	return &CheckpointView{
		Task:             c.Task,
		Generation:       c.Generation,
		Barrier:          c.Barrier,
		Round:            c.Round,
		Published:        c.Published,
		Shards:           shards,
		Manifest:         append([]ShardEntry(nil), c.Manifest...),
		LatestGeneration: f.latestPublished[c.Task],
	}
}

func (f *FSM) findCheckpoint(task string, gen uint64) *Checkpoint {
	gens := f.checkpoints[task]
	if gens == nil {
		return nil
	}
	return gens[gen]
}

// validSHA256 reports whether s is exactly 64 lowercase hex characters.
func validSHA256(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// applyCheckpointCreate binds a task generation to the barrier's current
// waiting round. Same task+generation+binding is idempotent; any deviation
// conflicts. A barrier round binds at most one checkpoint.
func (f *FSM) applyCheckpointCreate(cmd Command) Result {
	f.checkBarrierLiveness(cmd.Now)
	if cmd.Task == "" || cmd.Generation < 1 {
		return Result{OK: false, Err: "task and a positive generation are required"}
	}
	b := f.barriers[cmd.Barrier]
	if b == nil {
		return Result{OK: false, Err: "barrier not found"}
	}
	if cp := f.findCheckpoint(cmd.Task, cmd.Generation); cp != nil {
		if cp.Barrier == cmd.Barrier {
			// Identical re-issue: idempotent.
			return Result{OK: true, Checkpoint: f.checkpointView(cp)}
		}
		return Result{OK: false, Err: "checkpoint generation already bound to a different barrier", Checkpoint: f.checkpointView(cp)}
	}
	if b.Status != BarrierWaiting {
		return Result{OK: false, Err: "barrier round is " + b.Status + ", cannot bind", Barrier: b.view()}
	}
	if b.BoundTask != "" {
		return Result{OK: false, Err: fmt.Sprintf("barrier round %d already bound to task %q generation %d", b.Round, b.BoundTask, b.BoundGeneration), Barrier: b.view()}
	}
	cp := &Checkpoint{
		Task:       cmd.Task,
		Generation: cmd.Generation,
		Barrier:    cmd.Barrier,
		Round:      b.Round,
		Shards:     make(map[string]ShardMeta),
	}
	gens := f.checkpoints[cmd.Task]
	if gens == nil {
		gens = make(map[uint64]*Checkpoint)
		f.checkpoints[cmd.Task] = gens
	}
	gens[cmd.Generation] = cp
	b.BoundTask = cmd.Task
	b.BoundGeneration = cmd.Generation
	return Result{OK: true, Checkpoint: f.checkpointView(cp)}
}

// applyCheckpointSubmit registers one participant's shard metadata and its
// barrier arrival in the same commit: both succeed or neither does.
func (f *FSM) applyCheckpointSubmit(cmd Command) Result {
	f.checkBarrierLiveness(cmd.Now)
	if cmd.URI == "" {
		return Result{OK: false, Err: "shard uri is required"}
	}
	if !validSHA256(cmd.SHA256) {
		return Result{OK: false, Err: "sha256 must be 64 lowercase hex characters"}
	}
	cp := f.findCheckpoint(cmd.Task, cmd.Generation)
	if cp == nil {
		return Result{OK: false, Err: "checkpoint not found"}
	}
	b := f.barriers[cp.Barrier]
	if b == nil || b.Round != cp.Round {
		return Result{OK: false, Err: "bound barrier round no longer exists", Checkpoint: f.checkpointView(cp)}
	}
	if b.Status != BarrierWaiting {
		return Result{OK: false, Err: "round already " + b.Status + ", submissions are closed", Checkpoint: f.checkpointView(cp)}
	}
	member := false
	for _, p := range b.Participants {
		if p == cmd.Participant {
			member = true
			break
		}
	}
	if !member {
		return Result{OK: false, Err: "unknown participant", Checkpoint: f.checkpointView(cp)}
	}
	reg := BarrierRegistration{
		Participant: cmd.Participant,
		Resource:    cmd.Resource,
		Holder:      cmd.Holder,
		Token:       cmd.Token,
	}
	shard := ShardMeta{URI: cmd.URI, SHA256: cmd.SHA256, Bytes: cmd.Bytes}
	if prev, ok := b.Arrivals[cmd.Participant]; ok {
		// Identical retransmission (same registration and same shard):
		// idempotent. Any deviation conflicts.
		if prev == reg && cp.Shards[cmd.Participant] == shard {
			return Result{OK: true, Checkpoint: f.checkpointView(cp)}
		}
		return Result{OK: false, Err: "conflicting submission for participant", Checkpoint: f.checkpointView(cp)}
	}
	for _, other := range b.Arrivals {
		if other.Resource == cmd.Resource {
			return Result{OK: false, Err: "resource already registered by another participant this round", Checkpoint: f.checkpointView(cp)}
		}
	}
	if !f.registrationLeaseValid(reg, cmd.Now) {
		return Result{OK: false, Err: "no matching valid lease for resource/holder/token", Checkpoint: f.checkpointView(cp)}
	}
	// All checks passed: record arrival and shard metadata atomically.
	b.Arrivals[cmd.Participant] = reg
	cp.Shards[cmd.Participant] = shard
	if len(b.Arrivals) == len(b.Participants) {
		// All arrived: re-validate every registered lease in this same
		// commit before completing.
		for _, r := range sortedArrivals(b) {
			if !f.registrationLeaseValid(r, cmd.Now) {
				b.Status = BarrierFailed
				b.FailReason = fmt.Sprintf("lease for participant %q on resource %q invalid at completion", r.Participant, r.Resource)
				return Result{OK: false, Err: b.FailReason, Checkpoint: f.checkpointView(cp)}
			}
		}
		b.Status = BarrierCompleted
	}
	return Result{OK: true, Checkpoint: f.checkpointView(cp)}
}

// applyCheckpointPublish freezes the manifest of a fully submitted,
// completed round and atomically advances the task's latest generation.
func (f *FSM) applyCheckpointPublish(cmd Command) Result {
	f.checkBarrierLiveness(cmd.Now)
	cp := f.findCheckpoint(cmd.Task, cmd.Generation)
	if cp == nil {
		return Result{OK: false, Err: "checkpoint not found"}
	}
	if cp.Published {
		// Retry of an already committed publish: return the frozen manifest.
		return Result{OK: true, Checkpoint: f.checkpointView(cp)}
	}
	b := f.barriers[cp.Barrier]
	if b == nil || b.Round != cp.Round {
		return Result{OK: false, Err: "bound round already advanced without publishing", Checkpoint: f.checkpointView(cp)}
	}
	switch b.Status {
	case BarrierWaiting:
		return Result{OK: false, Err: "round still waiting for arrivals", Checkpoint: f.checkpointView(cp)}
	case BarrierFailed:
		return Result{OK: false, Err: "round failed: " + b.FailReason, Checkpoint: f.checkpointView(cp)}
	}
	// Completed: every member must have registered a shard.
	for _, p := range b.Participants {
		if _, ok := cp.Shards[p]; !ok {
			return Result{OK: false, Err: fmt.Sprintf("participant %q has no shard metadata", p), Checkpoint: f.checkpointView(cp)}
		}
	}
	if latest := f.latestPublished[cmd.Task]; cmd.Generation <= latest {
		return Result{OK: false, Err: fmt.Sprintf("generation %d does not exceed latest published %d", cmd.Generation, latest), Checkpoint: f.checkpointView(cp)}
	}
	manifest := make([]ShardEntry, 0, len(b.Participants))
	for _, p := range b.Participants { // participants are stored sorted
		m := cp.Shards[p]
		manifest = append(manifest, ShardEntry{Participant: p, URI: m.URI, SHA256: m.SHA256, Bytes: m.Bytes})
	}
	sort.Slice(manifest, func(i, j int) bool { return manifest[i].Participant < manifest[j].Participant })
	cp.Manifest = manifest
	cp.Published = true
	f.latestPublished[cmd.Task] = cmd.Generation
	return Result{OK: true, Checkpoint: f.checkpointView(cp)}
}

// applyCheckpointQuery returns one checkpoint, or the task's latest
// published manifest when no generation is given.
func (f *FSM) applyCheckpointQuery(cmd Command) Result {
	f.checkBarrierLiveness(cmd.Now)
	gen := cmd.Generation
	if gen == 0 {
		gen = f.latestPublished[cmd.Task]
		if gen == 0 {
			return Result{OK: false, Err: "task has no published checkpoint"}
		}
	}
	cp := f.findCheckpoint(cmd.Task, gen)
	if cp == nil {
		return Result{OK: false, Err: "checkpoint not found"}
	}
	return Result{OK: true, Checkpoint: f.checkpointView(cp)}
}
