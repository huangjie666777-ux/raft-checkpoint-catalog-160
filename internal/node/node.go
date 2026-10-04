// Package node wires the Raft replication layer to the lease FSM.
package node

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"github.com/huangjie666777-ux/raft-lease-coordinator-149/internal/fsm"
)

// Peer describes one static voting member.
type Peer struct {
	ID       string
	RaftAddr string
	HTTPAddr string
}

// Config configures a single node process.
type Config struct {
	ID       string
	RaftAddr string
	HTTPAddr string
	DataDir  string
	Peers    []Peer // fixed set of three voting members, including self
}

// Node bundles the Raft instance and the lease FSM.
type Node struct {
	Raft *raft.Raft
	FSM  *fsm.FSM
	cfg  Config

	transport *raft.NetworkTransport
	store     *raftboltdb.BoltStore
}

// Open starts the Raft node. It bootstraps the fixed cluster only when the
// data directory holds no existing state; a node with data never
// re-bootstraps.
func Open(cfg Config) (*Node, error) {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, err
	}

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(cfg.ID)
	raftCfg.SnapshotInterval = 30 * time.Second
	raftCfg.SnapshotThreshold = 64
	raftCfg.TrailingLogs = 32

	// Real TCP transport for inter-node replication.
	addr, err := net.ResolveTCPAddr("tcp", cfg.RaftAddr)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(cfg.RaftAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("raft transport: %w", err)
	}

	// Persistent log, term and vote store (BoltDB).
	store, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft.db"))
	if err != nil {
		transport.Close()
		return nil, fmt.Errorf("raft store: %w", err)
	}

	snapshots, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, os.Stderr)
	if err != nil {
		transport.Close()
		store.Close()
		return nil, fmt.Errorf("snapshot store: %w", err)
	}

	f := fsm.New()
	r, err := raft.NewRaft(raftCfg, f, store, store, snapshots, transport)
	if err != nil {
		transport.Close()
		store.Close()
		return nil, fmt.Errorf("raft: %w", err)
	}

	n := &Node{Raft: r, FSM: f, cfg: cfg, transport: transport, store: store}

	// Bootstrap only on first initialization; existing state must not
	// re-bootstrap.
	hasState, err := raft.HasExistingState(store, store, snapshots)
	if err != nil {
		n.Close()
		return nil, err
	}
	if !hasState {
		servers := make([]raft.Server, 0, len(cfg.Peers))
		for _, p := range cfg.Peers {
			servers = append(servers, raft.Server{
				ID:       raft.ServerID(p.ID),
				Address:  raft.ServerAddress(p.RaftAddr),
				Suffrage: raft.Voter,
			})
		}
		cluster := raft.Configuration{Servers: servers}
		if err := r.BootstrapCluster(cluster).Error(); err != nil {
			n.Close()
			return nil, fmt.Errorf("bootstrap: %w", err)
		}
	}
	return n, nil
}

// LeaderHTTPAddr returns the HTTP address of the current leader, if known.
func (n *Node) LeaderHTTPAddr() string {
	_, leaderID := n.Raft.LeaderWithID()
	for _, p := range n.cfg.Peers {
		if p.ID == string(leaderID) {
			return p.HTTPAddr
		}
	}
	return ""
}

// IsLeader reports whether this node currently holds leadership.
func (n *Node) IsLeader() bool {
	return n.Raft.State() == raft.Leader
}

// Propose replicates a command and returns the FSM result once a majority
// has committed and applied it. A timeout means the result is unconfirmed.
func (n *Node) Propose(cmd fsm.Command, timeout time.Duration) (fsm.Result, error) {
	data, err := json.Marshal(cmd)
	if err != nil {
		return fsm.Result{}, err
	}
	future := n.Raft.Apply(data, timeout)
	if err := future.Error(); err != nil {
		return fsm.Result{}, err
	}
	res, ok := future.Response().(fsm.Result)
	if !ok {
		return fsm.Result{}, fmt.Errorf("unexpected FSM response type")
	}
	return res, nil
}

// Close shuts down networking and storage.
func (n *Node) Close() error {
	var firstErr error
	if n.Raft != nil {
		if err := n.Raft.Shutdown().Error(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if n.transport != nil {
		n.transport.Close()
	}
	if n.store != nil {
		if err := n.store.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
