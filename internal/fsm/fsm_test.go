package fsm

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func apply(t *testing.T, f *FSM, cmd Command) Result {
	t.Helper()
	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatal(err)
	}
	res, ok := f.Apply(&raft.Log{Data: data}).(Result)
	if !ok {
		t.Fatalf("unexpected response type")
	}
	return res
}

func TestAcquireContentionAndTokenGrowth(t *testing.T) {
	f := New()
	now := time.Unix(1000, 0)

	r1 := apply(t, f, Command{Type: CmdAcquire, Resource: "r", Holder: "a", TTL: 10, Now: now})
	if !r1.OK || r1.Token != 1 {
		t.Fatalf("first acquire: %+v", r1)
	}
	// Contention while the lease is valid: only one side wins.
	r2 := apply(t, f, Command{Type: CmdAcquire, Resource: "r", Holder: "b", TTL: 10, Now: now.Add(time.Second)})
	if r2.OK || r2.Holder != "a" {
		t.Fatalf("contending acquire should fail: %+v", r2)
	}
	// After expiry, re-acquire needs a larger token.
	r3 := apply(t, f, Command{Type: CmdAcquire, Resource: "r", Holder: "b", TTL: 10, Now: now.Add(11 * time.Second)})
	if !r3.OK || r3.Token != 2 {
		t.Fatalf("re-acquire after expiry: %+v", r3)
	}
}

func TestReleaseKeepsTokenBound(t *testing.T) {
	f := New()
	now := time.Unix(1000, 0)

	apply(t, f, Command{Type: CmdAcquire, Resource: "r", Holder: "a", TTL: 10, Now: now})
	rel := apply(t, f, Command{Type: CmdRelease, Resource: "r", Holder: "a", Token: 1, Now: now})
	if !rel.OK {
		t.Fatalf("release: %+v", rel)
	}
	if got := f.TokenBound("r"); got != 1 {
		t.Fatalf("token bound lost after release: %d", got)
	}
	r := apply(t, f, Command{Type: CmdAcquire, Resource: "r", Holder: "b", TTL: 10, Now: now})
	if !r.OK || r.Token != 2 {
		t.Fatalf("token must keep increasing after release: %+v", r)
	}
}

func TestRenewReleaseValidation(t *testing.T) {
	f := New()
	now := time.Unix(1000, 0)

	apply(t, f, Command{Type: CmdAcquire, Resource: "r", Holder: "a", TTL: 2, Now: now})

	// Wrong holder/token rejected.
	if r := apply(t, f, Command{Type: CmdRenew, Resource: "r", Holder: "x", Token: 1, TTL: 5, Now: now}); r.OK {
		t.Fatalf("renew with wrong holder must fail")
	}
	if r := apply(t, f, Command{Type: CmdRelease, Resource: "r", Holder: "a", Token: 9, Now: now}); r.OK {
		t.Fatalf("release with wrong token must fail")
	}

	// Renew recomputes the deadline from the decision moment.
	rn := apply(t, f, Command{Type: CmdRenew, Resource: "r", Holder: "a", Token: 1, TTL: 10, Now: now.Add(time.Second)})
	if !rn.OK || rn.Expiry != now.Add(11*time.Second).Unix() {
		t.Fatalf("renew: %+v", rn)
	}

	// Stale (expired) request must not affect a later holder.
	lost := apply(t, f, Command{Type: CmdRelease, Resource: "r", Holder: "a", Token: 1, Now: now.Add(20 * time.Second)})
	if lost.OK {
		t.Fatalf("expired release must fail")
	}
	acq := apply(t, f, Command{Type: CmdAcquire, Resource: "r", Holder: "b", TTL: 10, Now: now.Add(21 * time.Second)})
	if !acq.OK || acq.Token != 2 {
		t.Fatalf("acquire by b: %+v", acq)
	}
	stale := apply(t, f, Command{Type: CmdRelease, Resource: "r", Holder: "a", Token: 1, Now: now.Add(22 * time.Second)})
	if stale.OK {
		t.Fatalf("stale release must not affect new holder")
	}
	if l := f.Query("r", now.Add(23*time.Second)); l == nil || l.Holder != "b" {
		t.Fatalf("b lease must survive stale release: %+v", l)
	}
}

func TestQueryExpiredReturnsEmpty(t *testing.T) {
	f := New()
	now := time.Unix(1000, 0)
	apply(t, f, Command{Type: CmdAcquire, Resource: "r", Holder: "a", TTL: 5, Now: now})
	if l := f.Query("r", now.Add(4*time.Second)); l == nil {
		t.Fatalf("lease should be live")
	}
	if l := f.Query("r", now.Add(5*time.Second)); l != nil {
		t.Fatalf("expired lease must query empty")
	}
}

func TestSnapshotRestore(t *testing.T) {
	f := New()
	now := time.Unix(1000, 0)
	apply(t, f, Command{Type: CmdAcquire, Resource: "r1", Holder: "a", TTL: 100, Now: now})
	apply(t, f, Command{Type: CmdAcquire, Resource: "r2", Holder: "b", TTL: 100, Now: now})
	apply(t, f, Command{Type: CmdRelease, Resource: "r2", Holder: "b", Token: 1, Now: now})

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
	if l := g.Query("r1", now.Add(time.Second)); l == nil || l.Holder != "a" || l.Token != 1 {
		t.Fatalf("restored lease r1: %+v", l)
	}
	if got := g.TokenBound("r2"); got != 1 {
		t.Fatalf("restored token bound for released resource: %d", got)
	}
	// Token keeps growing from the restored bound.
	r := apply(t, g, Command{Type: CmdAcquire, Resource: "r2", Holder: "c", TTL: 10, Now: now})
	if !r.OK || r.Token != 2 {
		t.Fatalf("acquire after restore: %+v", r)
	}
}

type fakeSink struct {
	*bytes.Buffer
}

func (s *fakeSink) ID() string    { return "fake" }
func (s *fakeSink) Cancel() error { return nil }
func (s *fakeSink) Close() error  { return nil }
