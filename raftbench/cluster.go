package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// BenchmarkResult holds the key size, duration and throughput of a run.
type BenchmarkResult struct {
	KeySize    int
	Duration   time.Duration
	Throughput float64
}

const defaultNumOps = 100000

// raftNode encapsulates a raft.Node with its storage and ID.
type raftNode struct {
	id      uint64
	node    raft.Node
	storage *raft.MemoryStorage
}

// newRaftNode creates a new raft node with the given id and peer list.
func newRaftNode(id uint64, peers []raft.Peer) *raftNode {
	storage := raft.NewMemoryStorage()
	c := &raft.Config{
		ID:              id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   1 * 1024 * 1024,
		MaxInflightMsgs: 4096 / 8,
	}
	rn := &raftNode{
		id:      id,
		storage: storage,
	}
	rn.node = raft.StartNode(c, peers)
	return rn
}

// sendMessages simulates a network transport by delivering messages to the target nodes.
func sendMessages(msgs []raftpb.Message, nodes map[uint64]*raftNode) {
	for _, m := range msgs {
		if target, ok := nodes[m.To]; ok {
			go func(target *raftNode, m raftpb.Message) {
				target.node.Step(context.TODO(), m)
			}(target, m)
		}
	}
}

// runBenchmark creates a 3-node cluster, builds a MyKV message using a constant key of size keySize,
// and then submits numOps proposals with an incrementing value. It returns a BenchmarkResult.
func runBenchmark(keySize, numOps int) BenchmarkResult {
	fmt.Printf("Running benchmark with key size %d bytes...\n", keySize)

	// Define peers for a 3-node cluster.
	peers := []raft.Peer{{ID: 1}, {ID: 2}, {ID: 3}}

	// Create three nodes.
	node1 := newRaftNode(1, peers)
	node2 := newRaftNode(2, peers)
	node3 := newRaftNode(3, peers)

	// Map nodes by their ID for message routing.
	nodes := map[uint64]*raftNode{
		1: node1,
		2: node2,
		3: node3,
	}

	// For throughput measurement, we'll count committed operations on node1.
	var commitCount int64
	commitDone := make(chan struct{})

	// Start a ticker for each node to drive raft time.
	for _, rn := range nodes {
		go func(rn *raftNode) {
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for range ticker.C {
				rn.node.Tick()
			}
		}(rn)
	}

	// Process the Ready channel for each node.
	for _, rn := range nodes {
		go func(rn *raftNode) {
			for rd := range rn.node.Ready() {
				// Persist entries into storage.
				if err := rn.storage.Append(rd.Entries); err != nil {
					log.Fatalf("node %d: failed to append entries: %v", rn.id, err)
				}

				// Process committed entries.
				for _, entry := range rd.CommittedEntries {
					if entry.Type == raftpb.EntryNormal && len(entry.Data) > 0 {
						// Only count commits on node1.
						if rn.id == 1 {
							newCount := atomic.AddInt64(&commitCount, 1)
							if newCount == int64(numOps) {
								commitDone <- struct{}{}
							}
						}
					} else if entry.Type == raftpb.EntryConfChange {
						var cc raftpb.ConfChange
						if err := cc.Unmarshal(entry.Data); err != nil {
							log.Fatalf("node %d: failed to unmarshal conf change: %v", rn.id, err)
						}
						rn.node.ApplyConfChange(cc)
					}
				}

				// Route outgoing messages.
				if len(rd.Messages) > 0 {
					sendMessages(rd.Messages, nodes)
				}

				rn.node.Advance()
			}
		}(rn)
	}

	// Wait a bit for the nodes to establish leadership.
	time.Sleep(10 * time.Second)

	// Build a constant key string of the desired size.
	keyStr := strings.Repeat("a", keySize)

	ctx := context.Background()
	fmt.Println("Start proposing, key:", keyStr)
	startTime := time.Now()
	for i := 0; i < numOps; i++ {
		// Create a new MyKV message with constant key and an incrementing value.
		kvMessage := &raftpb.MyKV{
			Key:   []byte(keyStr),
			Value: []byte(fmt.Sprintf("%d", i)),
		}
		encodedData, err := kvMessage.Marshal()
		if err != nil {
			log.Fatalf("Failed to marshal MyKV message: %v", err)
		}
		if err := node1.node.Propose(ctx, encodedData); err != nil {
			log.Printf("failed to propose command %d: %v", i, err)
		}
	}
	<-commitDone

	endTime := time.Now()
	duration := endTime.Sub(startTime)
	throughput := float64(numOps) / duration.Seconds()

	fmt.Printf("Key size: %d bytes. Committed %d operations in %v\n", keySize, numOps, duration)
	fmt.Printf("Average throughput: %.2f ops/sec\n\n", throughput)

	time.Sleep(2 * time.Second)
	node1.node.Stop()
	node2.node.Stop()
	node3.node.Stop()

	return BenchmarkResult{
		KeySize:    keySize,
		Duration:   duration,
		Throughput: throughput,
	}
}

func main() {
	// Command-line flags for a single key size and number of operations.
	keySizeFlag := flag.Int("keysize", 128, "Key size in bytes")
	numOpsFlag := flag.Int("numops", defaultNumOps, "Number of operations to run")
	runsFlag := flag.Int("runs", 5, "Number of runs for averaging")
	flag.Parse()

	var results []BenchmarkResult

	// Run the benchmark multiple times.
	for i := 0; i < *runsFlag; i++ {
		result := runBenchmark(*keySizeFlag, *numOpsFlag)
		results = append(results, result)
		fmt.Printf("Completed run %d for key size %d bytes.\n", i+1, *keySizeFlag)
		fmt.Println("----------")
		time.Sleep(3 * time.Second)
	}

	// Compute averages.
	var totalDuration time.Duration
	var totalThroughput float64
	for _, res := range results {
		totalDuration += res.Duration
		totalThroughput += res.Throughput
	}
	avgDuration := totalDuration / time.Duration(len(results))
	avgThroughput := totalThroughput / float64(len(results))

	// Print summary.
	fmt.Println("Benchmark Summary:")
	fmt.Printf("Key Size: %4d bytes, Average Duration: %v, Average Throughput: %.2f ops/sec\n",
		*keySizeFlag, avgDuration, avgThroughput)
}
