// Command lockd runs one lease-lock node: Raft over TCP plus an HTTP API.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/huangjie666777-ux/raft-lease-coordinator-149/internal/api"
	"github.com/huangjie666777-ux/raft-lease-coordinator-149/internal/node"
)

// peersFlag parses "id=raftAddr=httpAddr,..." entries.
func parsePeers(spec string) ([]node.Peer, error) {
	var peers []node.Peer
	for _, entry := range strings.Split(spec, ",") {
		parts := strings.Split(entry, "=")
		if len(parts) != 3 {
			return nil, fmt.Errorf("invalid peer %q, want id=raftAddr=httpAddr", entry)
		}
		peers = append(peers, node.Peer{ID: parts[0], RaftAddr: parts[1], HTTPAddr: parts[2]})
	}
	if len(peers) == 0 {
		return nil, fmt.Errorf("at least one peer is required")
	}
	return peers, nil
}

func main() {
	var (
		id       = flag.String("id", "", "node ID (must appear in -peers)")
		raftAddr = flag.String("raft-addr", "", "Raft TCP bind address")
		httpAddr = flag.String("http-addr", "", "HTTP bind address")
		dataDir  = flag.String("data-dir", "", "node data directory")
		peers    = flag.String("peers", "", "fixed voters: id=raftAddr=httpAddr,...")
	)
	flag.Parse()

	if *id == "" || *raftAddr == "" || *httpAddr == "" || *dataDir == "" || *peers == "" {
		flag.Usage()
		os.Exit(2)
	}
	peerList, err := parsePeers(*peers)
	if err != nil {
		log.Fatalf("bad -peers: %v", err)
	}
	found := false
	for _, p := range peerList {
		if p.ID == *id {
			found = true
		}
	}
	if !found {
		log.Fatalf("node id %q not present in -peers", *id)
	}

	n, err := node.Open(node.Config{
		ID: *id, RaftAddr: *raftAddr, HTTPAddr: *httpAddr,
		DataDir: *dataDir, Peers: peerList,
	})
	if err != nil {
		log.Fatalf("open node: %v", err)
	}

	srv := api.NewServer(n, *httpAddr)
	go func() {
		log.Printf("http listening on %s", *httpAddr)
		if err := srv.ListenAndServe(); err != nil {
			log.Printf("http server stopped: %v", err)
		}
	}()
	log.Printf("node %s up: raft=%s http=%s data=%s", *id, *raftAddr, *httpAddr, *dataDir)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("shutting down")
	srv.Close()
	if err := n.Close(); err != nil {
		log.Printf("close: %v", err)
	}
}
