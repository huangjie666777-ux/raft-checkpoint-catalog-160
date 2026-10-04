package fsm

import (
	"bytes"
	"io"
	"testing"
	"time"
)

const (
	shaP1 = "1111111111111111111111111111111111111111111111111111111111111111"
	shaP2 = "2222222222222222222222222222222222222222222222222222222222222222"
)

func checkpointCreateCmd(t *testing.T, f *FSM, task string, generation uint64, barrier string) Result {
	t.Helper()
	return apply(t, f, Command{Type: CmdCheckpointCreate, Task: task, Generation: generation, Barrier: barrier, Now: base})
}

func checkpointRegisterCmd(t *testing.T, f *FSM, task string, generation uint64, part string, now time.Time) Result {
	t.Helper()
	token := uint64(1)
	sha := shaP1
	if part == "p2" {
		sha = shaP2
	}
	return apply(t, f, Command{
		Type: CmdCheckpointRegister, Task: task, Generation: generation, Participant: part,
		Resource: "r-" + part, Holder: "h-" + part, Token: token,
		URI: "s3://bucket/" + task + "/" + part, SHA256: sha, Size: 10, Now: now,
	})
}

func setupCheckpointBarrier(t *testing.T, f *FSM) {
	t.Helper()
	createBarrier(t, f, "phase", "p1", "p2")
	apply(t, f, Command{Type: CmdAcquire, Resource: "r-p1", Holder: "h-p1", TTL: 60, Now: base})
	apply(t, f, Command{Type: CmdAcquire, Resource: "r-p2", Holder: "h-p2", TTL: 60, Now: base})
}

func TestCheckpointCreateIdempotenceAndRoundBinding(t *testing.T) {
	f := New()
	setupCheckpointBarrier(t, f)

	r1 := checkpointCreateCmd(t, f, "job", 1, "phase")
	if !r1.OK || r1.Checkpoint.Round != 1 || r1.Checkpoint.Published {
		t.Fatalf("create checkpoint: %+v", r1)
	}
	if r2 := checkpointCreateCmd(t, f, "job", 1, "phase"); !r2.OK {
		t.Fatalf("same binding must be idempotent: %+v", r2)
	}
	if r := checkpointCreateCmd(t, f, "job", 1, "other"); r.OK {
		t.Fatalf("conflicting binding must fail: %+v", r)
	}
	createBarrier(t, f, "other", "p1", "p2")
	if r := checkpointCreateCmd(t, f, "other-job", 1, "phase"); r.OK {
		t.Fatalf("one barrier round binds at most one checkpoint: %+v", r)
	}
}

func TestCheckpointRegisterAndPublishManifest(t *testing.T) {
	f := New()
	setupCheckpointBarrier(t, f)
	checkpointCreateCmd(t, f, "job", 1, "phase")

	a1 := checkpointRegisterCmd(t, f, "job", 1, "p1", base)
	if !a1.OK || a1.Barrier.Status != BarrierWaiting || len(a1.Barrier.Arrived) != 1 {
		t.Fatalf("first candidate: %+v", a1)
	}
	a1dup := checkpointRegisterCmd(t, f, "job", 1, "p1", base)
	if !a1dup.OK || len(a1dup.Barrier.Arrived) != 1 {
		t.Fatalf("duplicate candidate must be idempotent: %+v", a1dup)
	}
	if r := arrive(t, f, "phase", 1, "p2", "r-p2", "h-p2", 1, base); r.OK {
		t.Fatalf("legacy arrival must not bypass shard registration: %+v", r)
	}
	if r := apply(t, f, Command{
		Type: CmdCheckpointRegister, Task: "job", Generation: 1, Participant: "p1",
		Resource: "r-p1", Holder: "h-p1", Token: 1, URI: "s3://changed",
		SHA256: shaP1, Size: 10, Now: base,
	}); r.OK {
		t.Fatalf("changed shard metadata must conflict")
	}
	a2 := checkpointRegisterCmd(t, f, "job", 1, "p2", base)
	if !a2.OK || a2.Barrier.Status != BarrierCompleted {
		t.Fatalf("second candidate atomically completes barrier: %+v", a2)
	}
	published := apply(t, f, Command{Type: CmdCheckpointPublish, Task: "job", Generation: 1, Now: base})
	if !published.OK {
		t.Fatalf("publish complete checkpoint: %+v", published)
	}
	again := apply(t, f, Command{Type: CmdCheckpointPublish, Task: "job", Generation: 1, Now: base})
	if !again.OK || again.Manifest == nil || again.Manifest.Shards[0].Participant != "p1" || again.Manifest.Shards[1].Participant != "p2" {
		t.Fatalf("publish retry must return original ordered manifest: %+v", again)
	}
	latest := apply(t, f, Command{Type: CmdCheckpointLatest, Task: "job", Now: base})
	if !latest.OK || latest.Manifest.Generation != 1 || len(latest.Manifest.Shards) != 2 {
		t.Fatalf("latest manifest: %+v", latest)
	}
	if adv := apply(t, f, Command{Type: CmdBarrierAdvance, Barrier: "phase", Round: 1, Now: base}); !adv.OK {
		t.Fatalf("advance: %+v", adv)
	}
	afterAdvance := apply(t, f, Command{Type: CmdCheckpointLatest, Task: "job", Now: base})
	if !afterAdvance.OK || afterAdvance.Manifest.Round != 1 {
		t.Fatalf("published manifest must survive advance: %+v", afterAdvance)
	}
}

func TestCheckpointPublishRejectsIncompleteAndStaleGenerations(t *testing.T) {
	f := New()
	setupCheckpointBarrier(t, f)
	checkpointCreateCmd(t, f, "job", 1, "phase")
	checkpointRegisterCmd(t, f, "job", 1, "p1", base)
	if r := apply(t, f, Command{Type: CmdCheckpointPublish, Task: "job", Generation: 1, Now: base}); r.OK {
		t.Fatalf("incomplete checkpoint must not publish: %+v", r)
	}

	createBarrier(t, f, "phase2", "p1", "p2")
	checkpointCreateCmd(t, f, "job", 2, "phase2")
	c1 := checkpointRegisterCmd(t, f, "job", 2, "p1", base.Add(time.Second))
	if !c1.OK {
		t.Fatalf("gen2 p1: %+v", c1)
	}
	apply(t, f, Command{Type: CmdAcquire, Resource: "r2-p2", Holder: "h-p2", TTL: 60, Now: base})
	c2 := checkpointRegisterCmd(t, f, "job", 2, "p2", base.Add(time.Second))
	if !c2.OK {
		t.Fatalf("gen2 p2: %+v", c2)
	}
	published := apply(t, f, Command{Type: CmdCheckpointPublish, Task: "job", Generation: 2, Now: base.Add(time.Second)})
	if !published.OK || published.Manifest.Generation != 2 {
		t.Fatalf("publish generation 2: %+v", published)
	}
	if r := checkpointCreateCmd(t, f, "job", 2, "phase2"); !r.OK {
		t.Fatalf("published checkpoint query by create retry should be idempotent: %+v", r)
	}
	createBarrier(t, f, "phase3", "p1", "p2")
	if r := checkpointCreateCmd(t, f, "job", 2, "phase3"); r.OK {
		t.Fatalf("generation not higher than latest must conflict")
	}
}

func TestCheckpointSnapshotRestore(t *testing.T) {
	f := New()
	setupCheckpointBarrier(t, f)
	checkpointCreateCmd(t, f, "job", 1, "phase")
	checkpointRegisterCmd(t, f, "job", 1, "p1", base)
	checkpointRegisterCmd(t, f, "job", 1, "p2", base)
	publish := apply(t, f, Command{Type: CmdCheckpointPublish, Task: "job", Generation: 1, Now: base})
	if !publish.OK {
		t.Fatalf("publish: %+v", publish)
	}

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&fakeSink{&buf}); err != nil {
		t.Fatal(err)
	}
	g := New()
	if err := g.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))); err != nil {
		t.Fatal(err)
	}
	latest := apply(t, g, Command{Type: CmdCheckpointLatest, Task: "job", Now: base})
	if !latest.OK || latest.Manifest.Generation != 1 || len(latest.Manifest.Shards) != 2 {
		t.Fatalf("restored latest manifest: %+v", latest)
	}
	query := apply(t, g, Command{Type: CmdCheckpointQuery, Task: "job", Generation: 1, Now: base})
	if !query.OK || !query.Checkpoint.Published || query.Manifest == nil {
		t.Fatalf("restored checkpoint: %+v", query)
	}
}
