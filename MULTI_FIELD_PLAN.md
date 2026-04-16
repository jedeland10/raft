# Multi-Field Path Encoding — Implementation Plan

## Goal

Extend RepliCache to cache multiple protobuf fields at arbitrary nesting depths,
enabling bandwidth reduction for complex objects like Kubernetes resources stored in etcd.

Target: reduce K8s Deployment replication traffic by ~87% (3,491B → ~450B per write).

## Prerequisites

- [ ] Regenerate K8s etcd trace (minikube with realistic workloads, `etcdctl watch`)
- [ ] Verify trace format matches `k8s_deployment_protobuf_structure.md` analysis
- [ ] Set up local etcd build with `replace` directive pointing to raft fork

## Phase 1: Path Tree Data Structures

**Branch:** `replicache-multi-field`
**File:** `unicache/unicache.go`

### 1.1 Define path configuration types

```go
type PathStep struct {
    FieldNum  int
    SkipBytes int  // bytes to skip before parsing (e.g., 4 for k8s\x00 prefix)
}
```

### 1.2 Build path tree from flat paths

Convert user-provided paths into a tree with shared prefixes:

```
Input paths:
  [4, 2, 2, 2]      → DeploymentSpec
  [4, 2, 2, 1, 17]  → managedFields (repeated)
  [4, 2, 2, 1, 12]  → annotations

Tree:
  root
  └── 4 (PutRequest)
      └── 2 (value, skip=4 for k8s prefix)
          └── 2 (Deployment)
              ├── 2: leaf[0] (DeploymentSpec)
              └── 1 (ObjectMeta)
                  ├── 17: leaf[1] (managedFields, repeated)
                  └── 12: leaf[2] (annotations)
```

Internal types:

```go
type pathTreeNode struct {
    step     PathStep
    children []*pathTreeNode
    leafIdx  int   // >=0 for cacheable leaf, -1 for intermediate
    nodeIdx  int   // unique ID for this internal node
    repeated bool  // leaf entries cached independently
}
```

### 1.3 Pre-allocated workspace

Reused across calls to avoid per-operation allocation:

```go
type workspace struct {
    // Extraction results (sub-slices of input, zero-copy)
    leafValues [][]byte
    leafWireT  []protowire.Type
    leafFound  []bool

    // For repeated fields
    repeatValues [][]byte
    repeatCounts []int

    // Node submessage boundaries (sub-slices)
    nodeSubmsg [][]byte

    // Offset tracking for reconstruction
    nodeChildPositions [][]fieldPosition

    // Size deltas for bottom-up computation
    nodeDelta []int
}

type fieldPosition struct {
    fieldStart int  // offset of tag byte within parent submessage
    fieldEnd   int  // offset past end of field
    childNode  int  // node index, or -1 for leaf
    leafIdx    int  // leaf index if childNode == -1
}
```

### 1.4 Constructor

```go
func NewUniCacheWithPaths(
    minCacheVersion func() uint64,
    capacity int,
    paths [][]PathStep,
    repeatedLeaves []bool,
) UniCache
```

Backward compat: `NewUniCache(mcv, cap)` delegates with path `[{4,0}, {1,0}]`.

**Tests:**
- Construct path tree from K8s example paths, verify structure
- Verify backward compat constructors produce correct single-path trees

---

## Phase 2: Offset-Tracking Extraction

### 2.1 New helper: `getProtoFieldWithOffset`

Like `GetProtoFieldAndWireType` but also returns byte positions:

```go
func getProtoFieldWithOffset(data []byte, targetField int) (
    value []byte,           // sub-slice (zero-alloc for BytesType)
    wireType protowire.Type,
    fieldStart int,         // offset of tag in data
    fieldEnd int,           // offset past end of field in data
    err error,
)
```

For repeated fields, variant that returns all occurrences:

```go
func getRepeatedFieldsWithOffsets(data []byte, targetField int, dst []fieldOccurrence) (int, error)

type fieldOccurrence struct {
    value      []byte
    fieldStart int
    fieldEnd   int
}
```

Uses pre-allocated `dst` slice from workspace.

### 2.2 `walkExtract(data []byte) bool`

Depth-first traversal of path tree:

1. At each node, call `getProtoFieldWithOffset` to find the child field
2. If `SkipBytes > 0`, advance past the prefix (but record it for reconstruction)
3. Store submessage sub-slice in `ws.nodeSubmsg[nodeIdx]`
4. Store child field positions in `ws.nodeChildPositions[nodeIdx]`
5. At leaves, store value in `ws.leafValues[leafIdx]`
6. For repeated leaves, collect all occurrences

Zero allocations — everything stored in pre-allocated workspace using sub-slices.

**Tests:**
- Extract from a hand-crafted nested protobuf matching K8s structure
- Verify all leaf values and offsets are correct
- Test with SkipBytes (simulated k8s prefix)
- Test repeated field extraction

---

## Phase 3: Two-Phase Replacement

### 3.1 Phase A: Bottom-up size computation

For each modified leaf:
```
leafDelta = newFieldSize - oldFieldSize
```

For each internal node, bottom-up:
```
childrenDelta = sum of child deltas
newContentLen = oldContentLen + childrenDelta
newLenPrefixSize = SizeVarint(newContentLen)
oldLenPrefixSize = SizeVarint(oldContentLen)
nodeDelta = childrenDelta + (newLenPrefixSize - oldLenPrefixSize)
```

Store in `ws.nodeDelta[nodeIdx]`. Zero allocations.

### 3.2 Phase B: Single-alloc top-down copy

```
totalSize = len(data) + rootDelta
out = make([]byte, 0, totalSize)    // THE ONLY ALLOCATION
```

Recursive splice:
- Copy unchanged bytes between modified children
- At leaves: write new tag + replacement value
- At internal nodes: write new length prefix, recurse into submessage
- Handle SkipBytes: copy prefix bytes verbatim before spliced content

**Tests:**
- Round-trip: encode (replace 470B spec with 2B varint ID) then decode (restore)
- Verify output length matches pre-computed size exactly
- Test with multiple leaves at different depths
- Test with SkipBytes preservation
- Benchmark: verify 1 allocation per encode/decode operation

---

## Phase 4: Wire Up Core Functions

### 4.1 EncodeData

1. `walkExtract(data)`
2. For each leaf: lookup `ws.leafValues[i]` in `reverseCache`
3. For repeated leaves: lookup each entry independently
4. Build `encodedIDs []uint32` (count-prefixed for repeated leaves)
5. Two-phase replacement to produce encoded output

### 4.2 DecodeEntry

1. `walkExtract(entry.Data)`
2. For each leaf with VarintType: decode ID, lookup in `cache`
3. Two-phase replacement to restore original bytes

### 4.3 SafeEncode

1. Parse `encodedIDs` to get per-leaf IDs
2. Safety check each ID (distance + version)
3. If all safe: two-phase replacement to build fullData
4. If any unsafe: restore all, return (full, full)

### 4.4 BatchUpdateCache / UpdateCache

- Fast path: `EncodedIDs` present → map lookup per ID
- Slow path: `walkExtract` → `addOrUpdateCacheKey` per leaf value

### 4.5 EncodedIDs layout for repeated fields

```
Non-repeated leaf: 1 slot (ID or 0)
Repeated leaf:     1 + N slots (count, id_0, id_1, ..., id_N-1)
```

Decoder knows which leaves are repeated from path config.

**Tests:**
- Full encode/decode round-trip with K8s-like nested structure
- Partial cache hits (some leaves cached, some not)
- Repeated field encode/decode
- SafeEncode with evicted IDs
- BatchUpdateCache fast path with EncodedIDs
- Backward compat: single-field NewUniCache still passes all existing tests

---

## Phase 5: K8s Integration & Benchmarks

### 5.1 Configuration

Add to raft `Config`:

```go
type Config struct {
    // ...existing...
    UniCachePaths     [][]PathStep  // nil = use default single-field
    UniCacheRepeated  []bool        // which paths have repeated leaves
}
```

Wire through to `NewUniCacheWithPaths` in `raft.go` where the cache is constructed.

### 5.2 etcd integration

In etcd's raft configuration, set paths for K8s objects:

```go
paths := [][]PathStep{
    {{FieldNum: 4}, {FieldNum: 2, SkipBytes: 4}, {FieldNum: 2}, {FieldNum: 2}},       // Spec
    {{FieldNum: 4}, {FieldNum: 2, SkipBytes: 4}, {FieldNum: 2}, {FieldNum: 1}, {FieldNum: 17}}, // managedFields
    {{FieldNum: 4}, {FieldNum: 2, SkipBytes: 4}, {FieldNum: 2}, {FieldNum: 1}, {FieldNum: 12}}, // annotations
}
```

### 5.3 K8s trace replay benchmark

- Convert etcd watch trace to go-ycsb replay format
- Run on 5-node GCP multi-region cluster
- Measure: throughput, bytes sent, cache hit rate per leaf path
- Compare: Raft baseline vs single-field RepliCache vs multi-field RepliCache

### 5.4 Expected results

| Config | Bytes/write | Throughput (est.) |
|--------|------------|-------------------|
| Raft baseline | 3,491B | baseline |
| Single-field (key only, ~55B) | 3,440B | ~baseline (key too small) |
| Multi-field (spec+managedFields+annotations) | ~450B | ~2-3x baseline |

---

## Phase 6: Generalization & Paper

### 6.1 Other K8s object types

Analyze Pods, ReplicaSets, Services, ConfigMaps — each has different hot fields.
Could auto-detect cacheable fields by observing recurrence rates at runtime.

### 6.2 Adaptive path discovery

Instead of static configuration, monitor field-level recurrence and dynamically
add/remove cache paths. This removes the need for K8s-specific knowledge in the
Raft layer.

### 6.3 Paper structure

- Problem: Raft replication bandwidth in geo-distributed K8s clusters
- Approach: Field-path caching with shared path tree and single-alloc splice
- Evaluation: K8s workload trace replay on multi-region etcd
- Comparison: vs baseline Raft, vs whole-object caching, vs delta encoding
