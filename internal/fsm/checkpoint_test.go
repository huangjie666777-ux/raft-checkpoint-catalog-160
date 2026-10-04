package fsm

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

const testSHA = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func setupBoundCheckpoint(t *testing.T, f *FSM) {
	t.Helper()
	createBarrier(t, f, "b", "p1", "p2")
	apply(t, f, Command{Type: CmdAcquire, Resource: "r1", Holder: "h1", TTL: 60, Now: base})
	apply(t, f, Command{Type: CmdAcquire, Resource: "r2", Holder: "h2", TTL: 60, Now: base})
	r := apply(t, f, Command{Type: CmdCheckpointCreate, Task: "job", Generation: 1, Barrier: "b", Now: base})
	if !r.OK || r.Checkpoint.Round != 1 {
		t.Fatalf("checkpoint create: %+v", r)
	}
}

func submit(t *testing.T, f *FSM, gen uint64, part, res, holder string, token uint64, uri, sha string, bytes uint64) Result {
	t.Helper()
	return apply(t, f, Command{
		Type: CmdCheckpointSubmit, Task: "job", Generation: gen,
		Participant: part, Resource: res, Holder: holder, Token: token,
		URI: uri, SHA256: sha, Bytes: bytes, Now: base,
	})
}

func TestCheckpointCreateIdempotentAndConflict(t *testing.T) {
	f := New()
	setupBoundCheckpoint(t, f)
	// Identical re-issue: idempotent.
	if r := apply(t, f, Command{Type: CmdCheckpointCreate, Task: "job", Generation: 1, Barrier: "b", Now: base}); !r.OK {
		t.Fatalf("idempotent create: %+v", r)
	}
	// Same task+generation, different barrier: conflict.
	createBarrier(t, f, "b2", "p1", "p2")
	if r := apply(t, f, Command{Type: CmdCheckpointCreate, Task: "job", Generation: 1, Barrier: "b2", Now: base}); r.OK {
		t.Fatalf("conflicting create must fail")
	}
	// One barrier round binds at most one checkpoint.
	if r := apply(t, f, Command{Type: CmdCheckpointCreate, Task: "other", Generation: 1, Barrier: "b", Now: base}); r.OK {
		t.Fatalf("second binding on the same round must fail")
	}
	// Generation must be positive.
	if r := apply(t, f, Command{Type: CmdCheckpointCreate, Task: "z", Generation: 0, Barrier: "b2", Now: base}); r.OK {
		t.Fatalf("zero generation must fail")
	}
}

func TestCheckpointSubmitAtomicAndIdempotent(t *testing.T) {
	f := New()
	setupBoundCheckpoint(t, f)
	// Bad digest: rejected, nothing recorded.
	if r := submit(t, f, 1, "p1", "r1", "h1", 1, "s3://a/1", "ABCDEF", 10); r.OK {
		t.Fatalf("invalid sha256 must fail")
	}
	if r := submit(t, f, 1, "p1", "r1", "h1", 1, "s3://a/1", strings.ToUpper(testSHA), 10); r.OK {
		t.Fatalf("uppercase sha256 must fail")
	}
	// Bad lease: rejected, no one-sided state.
	if r := submit(t, f, 1, "p1", "r1", "h1", 99, "s3://a/1", testSHA, 10); r.OK {
		t.Fatalf("bad token must fail")
	}
	if got := f.findCheckpoint("job", 1); len(got.Shards) != 0 {
		t.Fatalf("rejected submit left shard state: %+v", got.Shards)
	}
	if b := f.barriers["b"]; len(b.Arrivals) != 0 {
		t.Fatalf("rejected submit left arrival state: %+v", b.Arrivals)
	}
	// Valid submit records both sides in one commit.
	r1 := submit(t, f, 1, "p1", "r1", "h1", 1, "s3://a/1", testSHA, 10)
	if !r1.OK || len(r1.Checkpoint.Shards) != 1 {
		t.Fatalf("submit p1: %+v", r1)
	}
	// Identical resend: idempotent.
	if r := submit(t, f, 1, "p1", "r1", "h1", 1, "s3://a/1", testSHA, 10); !r.OK {
		t.Fatalf("idempotent resend: %+v", r)
	}
	// Changed content: rejected.
	if r := submit(t, f, 1, "p1", "r1", "h1", 1, "s3://a/1", testSHA, 11); r.OK {
		t.Fatalf("changed bytes must fail")
	}
	if r := submit(t, f, 1, "p1", "r1", "h1", 1, "s3://a/2", testSHA, 10); r.OK {
		t.Fatalf("changed uri must fail")
	}
	// Plain barrier arrive must not bypass shard registration on a bound round.
	if r := arrive(t, f, "b", 1, "p2", "r2", "h2", 1, base); r.OK {
		t.Fatalf("plain arrive on bound round must fail")
	}
	r2 := submit(t, f, 1, "p2", "r2", "h2", 1, "s3://a/2", testSHA, 20)
	if !r2.OK || f.barriers["b"].Status != BarrierCompleted {
		t.Fatalf("submit p2 should complete the round: %+v", r2)
	}
}

func TestCheckpointPublishLifecycle(t *testing.T) {
	f := New()
	setupBoundCheckpoint(t, f)
	publish := func(gen uint64) Result {
		return apply(t, f, Command{Type: CmdCheckpointPublish, Task: "job", Generation: gen, Now: base})
	}
	// Not all arrived: rejected.
	submit(t, f, 1, "p1", "r1", "h1", 1, "s3://a/1", testSHA, 10)
	if r := publish(1); r.OK {
		t.Fatalf("publish before completion must fail")
	}
	submit(t, f, 1, "p2", "r2", "h2", 1, "s3://a/2", testSHA, 20)
	r := publish(1)
	if !r.OK || !r.Checkpoint.Published || len(r.Checkpoint.Manifest) != 2 {
		t.Fatalf("publish: %+v", r)
	}
	// Manifest is ordered by participant ID.
	if r.Checkpoint.Manifest[0].Participant != "p1" || r.Checkpoint.Manifest[1].Participant != "p2" {
		t.Fatalf("manifest not sorted by participant: %+v", r.Checkpoint.Manifest)
	}
	if r.Checkpoint.LatestGeneration != 1 {
		t.Fatalf("latest generation: %+v", r.Checkpoint)
	}
	// Retry returns the original manifest.
	if r2 := publish(1); !r2.OK || len(r2.Checkpoint.Manifest) != 2 {
		t.Fatalf("publish retry: %+v", r2)
	}
	// Advancing after publish does not change the result.
	apply(t, f, Command{Type: CmdBarrierAdvance, Barrier: "b", Round: 1, Now: base})
	if r3 := publish(1); !r3.OK || len(r3.Checkpoint.Manifest) != 2 {
		t.Fatalf("publish after advance: %+v", r3)
	}
	// Lease expiry after publish does not change the result.
	later := base.Add(2 * 60 * 1e9)
	if r4 := apply(t, f, Command{Type: CmdCheckpointQuery, Task: "job", Generation: 1, Now: later}); !r4.OK || !r4.Checkpoint.Published {
		t.Fatalf("query after lease expiry: %+v", r4)
	}
	// A regressed generation cannot publish.
	if r5 := publish(1); !r5.OK {
		t.Fatalf("republish of same generation must stay idempotent")
	}
	// Next round, next generation: bind, fail the round, ensure the failed
	// candidate never overwrites the published manifest.
	if r6 := apply(t, f, Command{Type: CmdCheckpointCreate, Task: "job", Generation: 2, Barrier: "b", Now: later}); !r6.OK {
		t.Fatalf("bind gen 2: %+v", r6)
	}
	apply(t, f, Command{Type: CmdAcquire, Resource: "r1", Holder: "h1", TTL: 60, Now: later})
	apply(t, f, Command{Type: CmdAcquire, Resource: "r2", Holder: "h2", TTL: 60, Now: later})
	submit(t, f, 2, "p1", "r1", "h1", 2, "s3://b/1", testSHA, 30)
	apply(t, f, Command{Type: CmdRelease, Resource: "r1", Holder: "h1", Token: 2, Now: later})
	if r7 := publish(2); r7.OK {
		t.Fatalf("publish of failed round must fail: %+v", r7)
	}
	q := apply(t, f, Command{Type: CmdCheckpointQuery, Task: "job", Now: later})
	if !q.OK || q.Checkpoint.Generation != 1 || q.Checkpoint.LatestGeneration != 1 {
		t.Fatalf("failed candidate must not overwrite latest: %+v", q.Checkpoint)
	}
}

func TestCheckpointPublishRejectsAdvancedUnpublished(t *testing.T) {
	f := New()
	setupBoundCheckpoint(t, f)
	submit(t, f, 1, "p1", "r1", "h1", 1, "s3://a/1", testSHA, 10)
	submit(t, f, 1, "p2", "r2", "h2", 1, "s3://a/2", testSHA, 20)
	apply(t, f, Command{Type: CmdBarrierAdvance, Barrier: "b", Round: 1, Now: base})
	if r := apply(t, f, Command{Type: CmdCheckpointPublish, Task: "job", Generation: 1, Now: base}); r.OK {
		t.Fatalf("publish of an advanced unpublished round must fail")
	}
	// Submitting to a stale bound round is also rejected.
	if r := submit(t, f, 1, "p1", "r1", "h1", 1, "s3://a/1", testSHA, 10); r.OK {
		t.Fatalf("submit after advance must fail")
	}
}

func TestCheckpointSnapshotRestore(t *testing.T) {
	f := New()
	setupBoundCheckpoint(t, f)
	submit(t, f, 1, "p1", "r1", "h1", 1, "s3://a/1", testSHA, 10)
	submit(t, f, 1, "p2", "r2", "h2", 1, "s3://a/2", testSHA, 20)
	apply(t, f, Command{Type: CmdCheckpointPublish, Task: "job", Generation: 1, Now: base})
	// An unpublished candidate rides along too.
	apply(t, f, Command{Type: CmdBarrierAdvance, Barrier: "b", Round: 1, Now: base})
	apply(t, f, Command{Type: CmdCheckpointCreate, Task: "job", Generation: 2, Barrier: "b", Now: base})

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
	q := apply(t, g, Command{Type: CmdCheckpointQuery, Task: "job", Now: base})
	if !q.OK || q.Checkpoint.Generation != 1 || len(q.Checkpoint.Manifest) != 2 {
		t.Fatalf("restored latest manifest: %+v", q.Checkpoint)
	}
	c2 := g.findCheckpoint("job", 2)
	if c2 == nil || c2.Published || c2.Round != 2 {
		t.Fatalf("restored candidate: %+v", c2)
	}
	if b := g.barriers["b"]; b.BoundTask != "job" || b.BoundGeneration != 2 {
		t.Fatalf("restored binding: %+v", b)
	}
}

func TestBarrierLivenessDeterministicFailReason(t *testing.T) {
	// Two invalid arrivals: every replica must record the same reason,
	// chosen by sorted participant order rather than map iteration.
	f := New()
	createBarrier(t, f, "b", "pa", "pb", "pc")
	apply(t, f, Command{Type: CmdAcquire, Resource: "ra", Holder: "ha", TTL: 60, Now: base})
	apply(t, f, Command{Type: CmdAcquire, Resource: "rb", Holder: "hb", TTL: 60, Now: base})
	arrive(t, f, "b", 1, "pb", "rb", "hb", 1, base)
	arrive(t, f, "b", 1, "pa", "ra", "ha", 1, base)
	apply(t, f, Command{Type: CmdRelease, Resource: "ra", Holder: "ha", Token: 1, Now: base})
	apply(t, f, Command{Type: CmdRelease, Resource: "rb", Holder: "hb", Token: 1, Now: base})
	r := apply(t, f, Command{Type: CmdBarrierQuery, Barrier: "b", Now: base})
	if r.Barrier.Status != BarrierFailed {
		t.Fatalf("expected failed: %+v", r.Barrier)
	}
	if !strings.Contains(r.Barrier.FailReason, `"pa"`) {
		t.Fatalf("fail reason must name the sorted-first invalid participant: %q", r.Barrier.FailReason)
	}
}
