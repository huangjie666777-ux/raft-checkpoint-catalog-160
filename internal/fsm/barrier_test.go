package fsm

import (
	"bytes"
	"io"
	"strings"
	"testing"
	"time"
)

var base = time.Unix(1000, 0)

func createBarrier(t *testing.T, f *FSM, name string, parts ...string) Result {
	t.Helper()
	return apply(t, f, Command{Type: CmdBarrierCreate, Barrier: name, Participants: parts, Now: base})
}

func arrive(t *testing.T, f *FSM, name string, round uint64, part, res, holder string, token uint64, now time.Time) Result {
	t.Helper()
	return apply(t, f, Command{
		Type: CmdBarrierArrive, Barrier: name, Round: round,
		Participant: part, Resource: res, Holder: holder, Token: token, Now: now,
	})
}

func TestBarrierCreateIdempotentAndConflict(t *testing.T) {
	f := New()
	r1 := createBarrier(t, f, "b", "p1", "p2")
	if !r1.OK || r1.Barrier.Round != 1 || r1.Barrier.Status != BarrierWaiting {
		t.Fatalf("create: %+v", r1)
	}
	// Same name and config: idempotent.
	r2 := createBarrier(t, f, "b", "p2", "p1")
	if !r2.OK || r2.Barrier.Round != 1 {
		t.Fatalf("idempotent create: %+v", r2)
	}
	// Different config: rejected.
	if r3 := createBarrier(t, f, "b", "p1", "p3"); r3.OK {
		t.Fatalf("conflicting create must fail: %+v", r3)
	}
	// Too few / duplicated participants rejected.
	if r := createBarrier(t, f, "c", "only"); r.OK {
		t.Fatalf("single participant must fail")
	}
	if r := createBarrier(t, f, "c", "a", "a"); r.OK {
		t.Fatalf("duplicate participants must fail")
	}
}

func TestBarrierRendezvousCompletes(t *testing.T) {
	f := New()
	createBarrier(t, f, "b", "p1", "p2")
	apply(t, f, Command{Type: CmdAcquire, Resource: "r1", Holder: "h1", TTL: 60, Now: base})
	apply(t, f, Command{Type: CmdAcquire, Resource: "r2", Holder: "h2", TTL: 60, Now: base})

	a1 := arrive(t, f, "b", 1, "p1", "r1", "h1", 1, base)
	if !a1.OK || a1.Barrier.Status != BarrierWaiting || len(a1.Barrier.Arrived) != 1 {
		t.Fatalf("first arrive: %+v", a1)
	}
	// Retransmission of the same registration: idempotent, no double count.
	a1dup := arrive(t, f, "b", 1, "p1", "r1", "h1", 1, base)
	if !a1dup.OK || len(a1dup.Barrier.Arrived) != 1 {
		t.Fatalf("idempotent arrive: %+v", a1dup)
	}
	// Conflicting re-registration of the same participant: rejected.
	if r := arrive(t, f, "b", 1, "p1", "rX", "h1", 1, base); r.OK {
		t.Fatalf("conflicting registration must fail")
	}
	// Same resource twice in one round: rejected.
	if r := arrive(t, f, "b", 1, "p2", "r1", "h1", 1, base); r.OK {
		t.Fatalf("duplicate resource must fail")
	}
	// Unknown participant: rejected.
	if r := arrive(t, f, "b", 1, "stranger", "r2", "h2", 1, base); r.OK {
		t.Fatalf("unknown participant must fail")
	}
	// Wrong lease token: rejected.
	if r := arrive(t, f, "b", 1, "p2", "r2", "h2", 99, base); r.OK {
		t.Fatalf("bad token must fail")
	}
	a2 := arrive(t, f, "b", 1, "p2", "r2", "h2", 1, base)
	if !a2.OK || a2.Barrier.Status != BarrierCompleted {
		t.Fatalf("barrier should complete: %+v", a2)
	}
	// Terminal state never regresses: further arrivals rejected.
	if r := arrive(t, f, "b", 1, "p1", "r1", "h1", 1, base); r.OK {
		t.Fatalf("arrival after completion must fail")
	}
	// Advance requires the expected round and a terminal state.
	if r := apply(t, f, Command{Type: CmdBarrierAdvance, Barrier: "b", Round: 5, Now: base}); r.OK {
		t.Fatalf("advance with wrong round must fail")
	}
	adv := apply(t, f, Command{Type: CmdBarrierAdvance, Barrier: "b", Round: 1, Now: base})
	if !adv.OK || adv.Barrier.Round != 2 || adv.Barrier.Status != BarrierWaiting || len(adv.Barrier.Arrived) != 0 {
		t.Fatalf("advance: %+v", adv)
	}
	// A concurrent advance for the old round loses.
	if r := apply(t, f, Command{Type: CmdBarrierAdvance, Barrier: "b", Round: 1, Now: base}); r.OK {
		t.Fatalf("second advance for old round must fail")
	}
	// Stale round arrivals are rejected.
	if r := arrive(t, f, "b", 1, "p1", "r1", "h1", 1, base); r.OK {
		t.Fatalf("stale round arrival must fail")
	}
}

func TestBarrierFailsOnLeaseLoss(t *testing.T) {
	f := New()
	createBarrier(t, f, "b", "p1", "p2")
	apply(t, f, Command{Type: CmdAcquire, Resource: "r1", Holder: "h1", TTL: 60, Now: base})
	apply(t, f, Command{Type: CmdAcquire, Resource: "r2", Holder: "h2", TTL: 60, Now: base})
	arrive(t, f, "b", 1, "p1", "r1", "h1", 1, base)

	// Release p1's lease while waiting; the next barrier operation fails the round.
	apply(t, f, Command{Type: CmdRelease, Resource: "r1", Holder: "h1", Token: 1, Now: base})
	r := arrive(t, f, "b", 1, "p2", "r2", "h2", 1, base)
	if r.OK || r.Barrier.Status != BarrierFailed || r.Barrier.FailReason == "" {
		t.Fatalf("round should fail after lease release: %+v", r)
	}
	// No substitution: arrivals on the failed round are rejected.
	if r2 := arrive(t, f, "b", 1, "p1", "r1", "h1", 1, base); r2.OK {
		t.Fatalf("failed round must not accept arrivals")
	}
	// Advance from failed state is allowed and clears arrivals.
	adv := apply(t, f, Command{Type: CmdBarrierAdvance, Barrier: "b", Round: 1, Now: base})
	if !adv.OK || adv.Barrier.Round != 2 || len(adv.Barrier.Arrived) != 0 {
		t.Fatalf("advance after failure: %+v", adv)
	}
}

func TestBarrierFailsOnLeaseExpiry(t *testing.T) {
	f := New()
	createBarrier(t, f, "b", "p1", "p2")
	apply(t, f, Command{Type: CmdAcquire, Resource: "r1", Holder: "h1", TTL: 5, Now: base})
	arrive(t, f, "b", 1, "p1", "r1", "h1", 1, base)
	// Lease expires before the barrier completes; the next barrier command
	// (here a query) decides the failure at its own decision time.
	later := base.Add(10 * time.Second)
	q := apply(t, f, Command{Type: CmdBarrierQuery, Barrier: "b", Now: later})
	if !q.OK || q.Barrier.Status != BarrierFailed {
		t.Fatalf("expired lease should fail the round: %+v", q)
	}
}

func TestBarrierSnapshotRestore(t *testing.T) {
	f := New()
	createBarrier(t, f, "b", "p1", "p2")
	apply(t, f, Command{Type: CmdAcquire, Resource: "r1", Holder: "h1", TTL: 600, Now: base})
	arrive(t, f, "b", 1, "p1", "r1", "h1", 1, base)

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&fakeSink{&buf}); err != nil {
		t.Fatal(err)
	}
	g := New()
	if err := g.Restore(io.NopCloser(&buf)); err != nil {
		t.Fatal(err)
	}
	q := apply(t, g, Command{Type: CmdBarrierQuery, Barrier: "b", Now: base})
	if !q.OK || q.Barrier.Round != 1 || q.Barrier.Status != BarrierWaiting || len(q.Barrier.Arrived) != 1 {
		t.Fatalf("restored barrier: %+v", q)
	}
	// Restored arrivals still count toward completion.
	apply(t, g, Command{Type: CmdAcquire, Resource: "r2", Holder: "h2", TTL: 60, Now: base})
	a := arrive(t, g, "b", 1, "p2", "r2", "h2", 1, base)
	if !a.OK || a.Barrier.Status != BarrierCompleted {
		t.Fatalf("completion after restore: %+v", a)
	}
}

func TestRestoreOldSnapshotWithoutBarriers(t *testing.T) {
	// A snapshot in the pre-barrier format must still restore.
	old := []byte(`{"leases":{},"token_bounds":{"r":3}}`)
	g := New()
	if err := g.Restore(io.NopCloser(bytes.NewReader(old))); err != nil {
		t.Fatal(err)
	}
	if got := g.TokenBound("r"); got != 3 {
		t.Fatalf("token bound from old snapshot: %d", got)
	}
	r := createBarrier(t, g, "b", "p1", "p2")
	if !r.OK {
		t.Fatalf("barrier usable after old snapshot restore: %+v", r)
	}
}

func TestQueryLeaseCommand(t *testing.T) {
	f := New()
	apply(t, f, Command{Type: CmdAcquire, Resource: "r", Holder: "h", TTL: 10, Now: base})
	q := apply(t, f, Command{Type: CmdQueryLease, Resource: "r", Now: base.Add(time.Second)})
	if !q.OK || q.Holder != "h" || q.Token != 1 {
		t.Fatalf("query: %+v", q)
	}
	expired := apply(t, f, Command{Type: CmdQueryLease, Resource: "r", Now: base.Add(20 * time.Second)})
	if expired.OK {
		t.Fatalf("expired lease must query empty: %+v", expired)
	}
}

func TestMultipleLeaseFailureReasonIsDeterministic(t *testing.T) {
	for range 20 {
		f := New()
		createBarrier(t, f, "b", "p1", "p2", "p3")
		apply(t, f, Command{Type: CmdAcquire, Resource: "r1", Holder: "h1", TTL: 60, Now: base})
		apply(t, f, Command{Type: CmdAcquire, Resource: "r2", Holder: "h2", TTL: 60, Now: base})
		arrive(t, f, "b", 1, "p1", "r1", "h1", 1, base)
		arrive(t, f, "b", 1, "p2", "r2", "h2", 1, base)
		apply(t, f, Command{Type: CmdRelease, Resource: "r1", Holder: "h1", Token: 1, Now: base})
		apply(t, f, Command{Type: CmdRelease, Resource: "r2", Holder: "h2", Token: 1, Now: base})
		q := apply(t, f, Command{Type: CmdBarrierQuery, Barrier: "b", Now: base})
		if !q.OK || q.Barrier.Status != BarrierFailed || !strings.Contains(q.Barrier.FailReason, "p1") {
			t.Fatalf("deterministic first failing participant: %+v", q)
		}
	}
}
