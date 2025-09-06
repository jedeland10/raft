//go:build integration
// +build integration

package unicache_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	raft "go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
)

// --- helpers to build/inspect MyKV -----------------------------------------
// message MyKV { bytes key=1; bytes value=2; optional uint64 proposalID=3; }
func myKVBytes(key, value []byte, proposalID *uint64) []byte {
	var b []byte
	b = protowire.AppendTag(b, 1, protowire.BytesType)
	b = protowire.AppendBytes(b, key)
	b = protowire.AppendTag(b, 2, protowire.BytesType)
	b = protowire.AppendBytes(b, value)
	if proposalID != nil {
		b = protowire.AppendTag(b, 3, protowire.VarintType)
		b = protowire.AppendVarint(b, *proposalID)
	}
	return b
}

// --- tiny test harness ------------------------------------------------------

type testNode interface {
	ID() uint64
	Propose(ctx context.Context, data []byte) error
	AppliedIndex() uint64
	IsLeader() bool
}

type testCluster interface {
	Nodes() []testNode
	Leader() testNode
	Sleep(id uint64)
	Wake(id uint64)
	WaitAwakeAppliedAtLeast(target uint64, d time.Duration) error
	WaitSynced(d time.Duration) error
	Stop()

	electLeader(id uint64)
	beatLeader()
	waitAllEqual(maxBeats int) error
}

type nodeImpl struct {
	id      uint64
	n       raft.Node
	store   *raft.MemoryStorage
	cluster *clusterImpl

	mu          sync.RWMutex
	awake       bool
	appliedIdx  uint64
	cancel      context.CancelFunc
	readyClosed chan struct{}
}

func (n *nodeImpl) ID() uint64 { return n.id }

func (n *nodeImpl) Propose(ctx context.Context, data []byte) error {
	return n.n.Propose(ctx, data)
}

func (n *nodeImpl) AppliedIndex() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.appliedIdx
}

func (n *nodeImpl) IsLeader() bool {
	st := n.n.Status()
	return st.RaftState == raft.StateLeader
}

type clusterImpl struct {
	t      *testing.T
	ctx    context.Context
	cancel context.CancelFunc

	mu    sync.RWMutex
	nodes map[uint64]*nodeImpl
	order []uint64
}

func (c *clusterImpl) Nodes() []testNode {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]testNode, 0, len(c.order))
	for _, id := range c.order {
		out = append(out, c.nodes[id])
	}
	return out
}

func (c *clusterImpl) Leader() testNode {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// prefer an awake leader
	for _, id := range c.order {
		n := c.nodes[id]
		n.mu.RLock()
		awake := n.awake
		n.mu.RUnlock()
		if awake && n.IsLeader() {
			return n
		}
	}
	// fallback: any leader (e.g., before we ever sleep anyone)
	for _, id := range c.order {
		n := c.nodes[id]
		if n.IsLeader() {
			return n
		}
	}
	return nil
}

func (c *clusterImpl) Sleep(id uint64) {
	c.mu.RLock()
	n := c.nodes[id]
	c.mu.RUnlock()
	if n == nil {
		return
	}
	n.mu.Lock()
	n.awake = false
	n.mu.Unlock()
}

func (c *clusterImpl) Wake(id uint64) {
	c.mu.RLock()
	n := c.nodes[id]
	c.mu.RUnlock()
	if n == nil {
		return
	}
	n.mu.Lock()
	n.awake = true
	n.mu.Unlock()
}

func waitForNewLeader(t *testing.T, c testCluster, oldID uint64, timeout time.Duration) testNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if l := c.Leader(); l != nil && l.ID() != oldID {
			return l
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no new leader elected")
	return nil
}

func (c *clusterImpl) WaitAwakeAppliedAtLeast(target uint64, d time.Duration) error {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		min := ^uint64(0)
		seen := 0

		c.mu.RLock()
		for _, id := range c.order {
			n := c.nodes[id]
			n.mu.RLock()
			if n.awake {
				seen++
				if n.appliedIdx < min {
					min = n.appliedIdx
				}
			}
			n.mu.RUnlock()
		}
		c.mu.RUnlock()

		// need at least 2 awake nodes in a 3-node cluster
		if seen >= 2 && min >= target {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("awake nodes did not all reach index %d in time", target)
}

func (c *clusterImpl) WaitSynced(d time.Duration) error {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		var min, max uint64
		first := true

		c.mu.RLock()
		for _, id := range c.order {
			ai := c.nodes[id].AppliedIndex()
			if first {
				min, max, first = ai, ai, false
			} else {
				if ai < min {
					min = ai
				}
				if ai > max {
					max = ai
				}
			}
		}
		c.mu.RUnlock()

		if !first && min == max {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("nodes not synced in time")
}

func (c *clusterImpl) Stop() {
	c.cancel()
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, id := range c.order {
		n := c.nodes[id]
		n.n.Stop()
		if n.cancel != nil {
			n.cancel()
		}
	}
}

// deliver raft messages, dropping inbound if receiver is asleep.
func (c *clusterImpl) send(m pb.Message) {
	c.mu.RLock()
	dst := c.nodes[m.To]
	c.mu.RUnlock()
	if dst == nil {
		return
	}
	dst.mu.RLock()
	awake := dst.awake
	dst.mu.RUnlock()
	if !awake {
		return
	}
	_ = dst.n.Step(c.ctx, m)
}

// start a 3-node raft with UniCache enabled via raft.Config.UniCacheSize.
func startCluster3(t *testing.T) testCluster {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	c := &clusterImpl{
		t:      t,
		ctx:    ctx,
		cancel: cancel,
		nodes:  make(map[uint64]*nodeImpl, 3),
		order:  []uint64{1, 2, 3},
	}

	voters := append([]uint64(nil), c.order...) // [1,2,3]

	for _, id := range c.order {
		ms := raft.NewMemoryStorage()

		// Bootstrap: non-empty snapshot + hard state so snapshots are available.
		snap := pb.Snapshot{
			Metadata: pb.SnapshotMetadata{
				Index: 1,
				Term:  1,
				ConfState: pb.ConfState{
					Voters: voters,
				},
			},
		}
		if err := ms.ApplySnapshot(snap); err != nil {
			t.Fatalf("apply bootstrap snapshot %d: %v", id, err)
		}
		if err := ms.SetHardState(pb.HardState{Term: 1, Commit: 1}); err != nil {
			t.Fatalf("set hardstate %d: %v", id, err)
		}

		cfg := &raft.Config{
			ID:              id,
			ElectionTick:    10,
			HeartbeatTick:   1,
			Storage:         ms,
			MaxSizePerMsg:   32_000,
			MaxInflightMsgs: 256,
			UniCacheSize:    50_000,
		}

		// Existing state present → use RestartNode (not StartNode).
		rn := raft.RestartNode(cfg)

		nctx, ncancel := context.WithCancel(ctx)
		ni := &nodeImpl{
			id:          id,
			n:           rn,
			store:       ms,
			cluster:     c,
			awake:       true,
			cancel:      ncancel,
			readyClosed: make(chan struct{}),
		}
		c.nodes[id] = ni

		go func(n *nodeImpl) {
			defer close(n.readyClosed)
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()

			for {
				select {
				case <-nctx.Done():
					return
				case <-ticker.C:
					n.mu.RLock()
					awake := n.awake
					n.mu.RUnlock()
					if awake {
						n.n.Tick()
					}
				case rd := <-n.n.Ready():
					if !raft.IsEmptyHardState(rd.HardState) {
						if err := n.store.SetHardState(rd.HardState); err != nil {
							t.Fatalf("SetHardState %d: %v", n.id, err)
						}
					}
					if !raft.IsEmptySnap(rd.Snapshot) {
						if err := n.store.ApplySnapshot(rd.Snapshot); err != nil {
							t.Fatalf("ApplySnapshot %d: %v", n.id, err)
						}
					}
					if len(rd.Entries) > 0 {
						if err := n.store.Append(rd.Entries); err != nil {
							t.Fatalf("Append %d: %v", n.id, err)
						}
					}
					for _, m := range rd.Messages {
						c.send(m)
					}
					for _, ent := range rd.CommittedEntries {
						if ent.Type == pb.EntryConfChange {
							var cc pb.ConfChange
							if err := cc.Unmarshal(ent.Data); err == nil {
								_ = n.n.ApplyConfChange(cc)
							}
						}
						n.mu.Lock()
						if ent.Index > n.appliedIdx {
							n.appliedIdx = ent.Index
						}
						n.mu.Unlock()
					}
					n.n.Advance()
				}
			}
		}(ni)
	}

	// wait briefly for a leader (tests also wait, so this is best-effort)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if l := c.Leader(); l != nil {
			return c
		}
		time.Sleep(10 * time.Millisecond)
	}
	return c
}

func waitForLeader(t *testing.T, c testCluster, timeout time.Duration) testNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if l := c.Leader(); l != nil {
			return l
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no leader elected within timeout")
	return nil
}

// Elect a deterministic leader (no timeouts).
func (c *clusterImpl) electLeader(id uint64) {
	c.mu.RLock()
	n := c.nodes[id]
	c.mu.RUnlock()
	_ = n.n.Step(c.ctx, pb.Message{From: id, To: id, Type: pb.MsgHup})
}

// Force one heartbeat/append round from current leader.
func (c *clusterImpl) beatLeader() {
	l := c.Leader().(*nodeImpl)
	_ = l.n.Step(c.ctx, pb.Message{From: l.id, To: l.id, Type: pb.MsgBeat})
}

// Drive until all nodes have the same applied index (bounded by beats).
func (c *clusterImpl) waitAllEqual(maxBeats int) error {
	for i := 0; i < maxBeats; i++ {
		c.beatLeader()
		c.mu.RLock()
		var min, max uint64
		first := true
		for _, id := range c.order {
			ai := c.nodes[id].AppliedIndex()
			if first {
				min, max, first = ai, ai, false
			} else {
				if ai < min {
					min = ai
				}
				if ai > max {
					max = ai
				}
			}
		}
		c.mu.RUnlock()
		if !first && min == max {
			return nil
		}
	}
	return fmt.Errorf("applied indexes not equal after %d beats", maxBeats)
}

// --- Tests ------------------------------------------------------------------

// 1) Sleep follower, keep replicating, wake it, ensure catch-up.
func TestIntegration_UniCache_FollowerCatchUp_Deterministic(t *testing.T) {
	c := startCluster3(t)
	defer c.Stop()

	// 1) Pick a leader deterministically (no election timeout).
	leader := waitForLeader(t, c, 5*time.Second)

	// 2) Seed a few entries and drive a round.
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := leader.Propose(ctx, myKVBytes([]byte("hot"), []byte(fmt.Sprintf("pre-%d", i)), nil)); err != nil {
			t.Fatalf("pre propose: %v", err)
		}
	}
	// Drive replication without sleeping.
	if err := c.waitAllEqual(20); err != nil {
		t.Fatalf("pre equal: %v", err)
	}
	base := leader.(*nodeImpl).AppliedIndex()

	// 3) Disconnect one follower.
	var sleeper testNode
	for _, n := range c.Nodes() {
		if n.ID() != leader.ID() {
			sleeper = n
			break
		}
	}
	c.Sleep(sleeper.ID())

	// 4) Propose a bunch while it's disconnected; drive a few beats so quorum commits.
	for i := 0; i < 10; i++ {
		if err := leader.Propose(ctx, myKVBytes([]byte("hot"), []byte(fmt.Sprintf("mid-%d", i)), nil)); err != nil {
			t.Fatalf("mid propose: %v", err)
		}
	}
	// Drive just enough so the two connected nodes commit >= base+10.
	// (No time wait; we can check leader’s applied index directly.)
	for i := 0; i < 20 && leader.(*nodeImpl).AppliedIndex() < base+10; i++ {
		c.beatLeader()
	}
	if leader.(*nodeImpl).AppliedIndex() < base+10 {
		t.Fatalf("leader didn't reach base+10")
	}

	// 5) Reconnect the follower and drive beats until all are equal.
	c.Wake(sleeper.ID())
	if err := c.waitAllEqual(100); err != nil {
		t.Fatalf("catch-up failed: %v", err)
	}
}

// 2) Sleep leader to force reelection; wake old leader; ensure recovery.
func TestIntegration_UniCache_LeaderSleep_Reelection_ThenRecovery(t *testing.T) {
	c := startCluster3(t)
	defer c.Stop()

	leader := waitForLeader(t, c, 5*time.Second)

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := leader.Propose(ctx, myKVBytes([]byte("hotkey"), []byte(fmt.Sprintf("seed-%d", i)), nil)); err != nil {
			t.Fatalf("propose seed %d: %v", i, err)
		}
	}
	if err := c.WaitSynced(5 * time.Second); err != nil {
		t.Fatalf("seed sync: %v", err)
	}

	c.Sleep(leader.ID())

	newLeader := waitForNewLeader(t, c, leader.ID(), 8*time.Second)

	for i := 0; i < 10; i++ {
		if err := newLeader.Propose(ctx, myKVBytes([]byte("hotkey"), []byte(fmt.Sprintf("post-%d", i)), nil)); err != nil {
			t.Fatalf("propose post %d: %v", i, err)
		}
	}
	if err := c.(*clusterImpl).WaitAwakeAppliedAtLeast(newLeader.AppliedIndex(), 5*time.Second); err != nil {
		t.Fatalf("two-node majority wait: %v", err)
	}

	c.Wake(leader.ID())
	if err := c.WaitSynced(10 * time.Second); err != nil {
		t.Fatalf("final sync: %v", err)
	}
}
