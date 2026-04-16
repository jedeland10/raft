package unicache

import (
	"container/list"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
)

// ===================== PATH CONFIGURATION =====================

// PathStep describes one level of protobuf navigation.
type PathStep struct {
	FieldNum  int
	SkipBytes int // bytes to skip before parsing submessage (e.g., 4 for k8s\x00 prefix)
}

// ===================== PATH TREE =====================

type pathTreeNode struct {
	step     PathStep
	children []*pathTreeNode
	leafIdx  int  // >=0 for cacheable leaf, -1 for intermediate
	nodeIdx  int  // unique ID for internal nodes (those with children), -1 for leaves
	repeated bool // if true, leaf field may appear multiple times
}

// buildPathTree constructs a tree from flat paths, merging shared prefixes.
// Returns root children (the tree is implicit: root has no step), plus counts.
func buildPathTree(paths [][]PathStep, repeatedLeaves []bool) (roots []*pathTreeNode, numLeaves int, numNodes int) {
	leafCount := 0
	nodeCount := 0

	var rootChildren []*pathTreeNode

	for pathIdx, path := range paths {
		if len(path) == 0 {
			continue
		}
		isRepeated := false
		if repeatedLeaves != nil && pathIdx < len(repeatedLeaves) {
			isRepeated = repeatedLeaves[pathIdx]
		}

		children := &rootChildren
		for depth, step := range path {
			isLeaf := depth == len(path)-1

			var found *pathTreeNode
			for _, child := range *children {
				if child.step.FieldNum == step.FieldNum && child.step.SkipBytes == step.SkipBytes {
					found = child
					break
				}
			}

			if found != nil {
				if isLeaf {
					found.leafIdx = leafCount
					found.repeated = isRepeated
					leafCount++
				}
				children = &found.children
			} else {
				newNode := &pathTreeNode{
					step:    step,
					leafIdx: -1,
					nodeIdx: -1,
				}
				if isLeaf {
					newNode.leafIdx = leafCount
					newNode.repeated = isRepeated
					leafCount++
				}
				*children = append(*children, newNode)
				children = &newNode.children
			}
		}
	}

	// Assign nodeIdx to all nodes that have children.
	var assignNodeIndices func(nodes []*pathTreeNode)
	assignNodeIndices = func(nodes []*pathTreeNode) {
		for _, n := range nodes {
			if len(n.children) > 0 {
				n.nodeIdx = nodeCount
				nodeCount++
				assignNodeIndices(n.children)
			}
		}
	}
	assignNodeIndices(rootChildren)

	return rootChildren, leafCount, nodeCount
}

// ===================== WORKSPACE =====================

type fieldPosition struct {
	fieldStart int // offset of tag byte within parent submessage
	fieldEnd   int // offset past end of field
	childNode  int // node index, or -1 for leaf
	leafIdx    int // leaf index if childNode == -1
	skipBytes  int // bytes to skip within value (preserved prefix during reconstruction)
}

type fieldOccurrence struct {
	value      []byte
	fieldStart int
	fieldEnd   int
}

type workspace struct {
	leafValues [][]byte         // [leafIdx] -> value sub-slice (zero-copy)
	leafWireT  []protowire.Type // [leafIdx] -> wire type
	leafFound  []bool           // [leafIdx] -> was field present?

	// For repeated fields: extra values beyond the first
	repeatValues [][]byte
	repeatCounts []int // [leafIdx] -> total count (1 for non-repeated)

	// Per-node: submessage bytes and child field positions
	nodeSubmsg   [][]byte          // [nodeIdx] -> submessage content (after SkipBytes)
	nodeFullVal  [][]byte          // [nodeIdx] -> full field value (before SkipBytes)
	nodeChildPos [][]fieldPosition // positions of descended-into children

	// Root-level field positions (in the outermost data buffer)
	rootFieldPos []fieldPosition // one per pathRoot that was found

	// Size deltas for bottom-up computation (stored by computeNodeDelta)
	nodeDelta []int // [nodeIdx] -> submessage content delta

	// Scratch buffer for repeated field collection
	repeatScratch []fieldOccurrence
}

func newWorkspace(numLeaves, numNodes int) *workspace {
	ws := &workspace{
		leafValues:    make([][]byte, numLeaves),
		leafWireT:     make([]protowire.Type, numLeaves),
		leafFound:     make([]bool, numLeaves),
		repeatValues:  nil,
		repeatCounts:  make([]int, numLeaves),
		nodeSubmsg:    make([][]byte, numNodes),
		nodeFullVal:   make([][]byte, numNodes),
		nodeChildPos:  make([][]fieldPosition, numNodes),
		rootFieldPos:  nil,
		nodeDelta:     make([]int, numNodes),
		repeatScratch: make([]fieldOccurrence, 0, 16),
	}
	for i := range ws.nodeChildPos {
		ws.nodeChildPos[i] = make([]fieldPosition, 0, 4)
	}
	return ws
}

func (ws *workspace) reset() {
	for i := range ws.leafValues {
		ws.leafValues[i] = nil
		ws.leafWireT[i] = 0
		ws.leafFound[i] = false
		ws.repeatCounts[i] = 0
	}
	for i := range ws.nodeSubmsg {
		ws.nodeSubmsg[i] = nil
		ws.nodeFullVal[i] = nil
		ws.nodeChildPos[i] = ws.nodeChildPos[i][:0]
		ws.nodeDelta[i] = 0
	}
	ws.repeatValues = ws.repeatValues[:0]
	ws.rootFieldPos = ws.rootFieldPos[:0]
}

// ===================== PROTO FIELD WITH OFFSET =====================

// getProtoFieldWithOffset returns sub-slice AND byte positions within parent buffer.
// Zero-alloc for BytesType (returns sub-slice).
func getProtoFieldWithOffset(data []byte, targetField int) (
	value []byte, wireType protowire.Type, fieldStart int, fieldEnd int, err error) {

	buf := data
	offset := 0

	for len(buf) > 0 {
		fStart := offset
		fieldNum, wt, n := protowire.ConsumeTag(buf)
		if n < 0 {
			return nil, 0, 0, 0, errors.New("failed to consume tag")
		}
		buf = buf[n:]
		offset += n

		if int(fieldNum) == targetField {
			switch wt {
			case protowire.VarintType:
				_, nn := protowire.ConsumeVarint(buf)
				if nn < 0 {
					return nil, 0, 0, 0, errors.New("failed to consume varint")
				}
				return buf[:nn], wt, fStart, offset + nn, nil
			case protowire.BytesType:
				v, nn := protowire.ConsumeBytes(buf)
				if nn < 0 {
					return nil, 0, 0, 0, errors.New("failed to consume bytes")
				}
				return v, wt, fStart, offset + nn, nil
			case protowire.Fixed32Type:
				_, nn := protowire.ConsumeFixed32(buf)
				if nn < 0 {
					return nil, 0, 0, 0, errors.New("failed to consume fixed32")
				}
				return buf[:nn], wt, fStart, offset + nn, nil
			case protowire.Fixed64Type:
				_, nn := protowire.ConsumeFixed64(buf)
				if nn < 0 {
					return nil, 0, 0, 0, errors.New("failed to consume fixed64")
				}
				return buf[:nn], wt, fStart, offset + nn, nil
			case protowire.StartGroupType:
				_, nn := protowire.ConsumeGroup(fieldNum, buf)
				if nn < 0 {
					return nil, 0, 0, 0, errors.New("failed to consume group")
				}
				return buf[:nn], wt, fStart, offset + nn, nil
			default:
				return nil, 0, 0, 0, fmt.Errorf("unknown wire type: %v", wt)
			}
		}

		skip := skipField(wt, fieldNum, buf)
		if skip < 0 {
			return nil, 0, 0, 0, fmt.Errorf("failed to skip field %d", fieldNum)
		}
		buf = buf[skip:]
		offset += skip
	}

	return nil, 0, 0, 0, fmt.Errorf("field number %d not found", targetField)
}

// getRepeatedFieldsWithOffsets collects all occurrences of a repeated field.
func getRepeatedFieldsWithOffsets(data []byte, targetField int, dst []fieldOccurrence) (int, error) {
	buf := data
	offset := 0
	count := 0

	for len(buf) > 0 {
		fStart := offset
		fieldNum, wt, n := protowire.ConsumeTag(buf)
		if n < 0 {
			return count, errors.New("failed to consume tag")
		}
		buf = buf[n:]
		offset += n

		if int(fieldNum) == targetField {
			var val []byte
			var nn int
			switch wt {
			case protowire.BytesType:
				val, nn = protowire.ConsumeBytes(buf)
			case protowire.VarintType:
				_, nn = protowire.ConsumeVarint(buf)
				if nn > 0 {
					val = buf[:nn]
				}
			default:
				nn = skipField(wt, fieldNum, buf)
				if nn > 0 {
					val = buf[:nn]
				}
			}
			if nn < 0 {
				return count, errors.New("failed to consume field")
			}
			fEnd := offset + nn
			if count < len(dst) {
				dst[count] = fieldOccurrence{value: val, fieldStart: fStart, fieldEnd: fEnd}
			}
			count++
			buf = buf[nn:]
			offset = fEnd
			continue
		}

		skip := skipField(wt, fieldNum, buf)
		if skip < 0 {
			return count, fmt.Errorf("failed to skip field %d", fieldNum)
		}
		buf = buf[skip:]
		offset += skip
	}

	return count, nil
}

// ===================== LEAF REPLACEMENT =====================

type leafReplacement struct {
	newValue    []byte
	newWireType protowire.Type
}

// ===================== UniCache INTERFACE =====================

// UniCache defines methods for encoding/decoding entries with key caching.
type UniCache interface {
	NewUniCache(minCacheVersion func() uint64, capacity int) UniCache
	EncodeData(data []byte, currCacheIdx uint64) ([]byte, []uint32)
	DecodeEntry(entry pb.Entry) (pb.Entry, bool)
	SafeEncode(data []byte, appendIdx uint64, encodedIDs []uint32) ([]byte, []byte)
	GetNextId() uint32
	UpdateCache(entry pb.Entry) (pb.Entry, bool)
	BatchUpdateCache(entries []pb.Entry) ([]pb.Entry, bool)
	PurgeEvicted()
	CacheHits() uint64
	ResetCacheHits() uint64
	Restores() uint64
	ResetRestores() uint64
	GetMinCacheIdx(currMinIdx uint64) uint64
}

// ===================== CACHE ENTRY =====================

type cacheEntry struct {
	id       uint32
	key      []byte
	lastIdx  uint64
	addedIdx uint64
}

// ===================== uniCache STRUCT =====================

type uniCache struct {
	cache        map[uint32]*cacheEntry
	reverseCache map[string]uint32
	nextID       uint32
	capacity     int

	lruList *list.List
	lruMap  map[uint32]*list.Element

	evicted         map[uint32]*list.Element
	evictOrder      *list.List
	evictedCapacity int

	maxCommit       *uint64
	minCacheVersion func() uint64

	cachehits    uint64
	restores     uint64
	lastInFlight uint64

	// Path tree configuration
	pathRoots []*pathTreeNode
	numLeaves int
	numNodes  int
	paths     [][]PathStep // original paths for NewUniCache recreation
	repeated  []bool       // original repeated config

	// Pre-allocated workspace (protected by wsMu for concurrent access)
	wsMu sync.Mutex
	ws   *workspace
}

// ===================== CONSTRUCTORS =====================

// NewUniCache constructs a UniCache with backward-compatible single field caching.
// Caches field 1 nested inside field 4.
func NewUniCache(minCacheVersion func() uint64, capacity int) UniCache {
	return NewUniCacheWithPaths(minCacheVersion, capacity,
		[][]PathStep{{{FieldNum: 4}, {FieldNum: 1}}}, nil)
}

// NewUniCacheMultiField constructs a UniCache that caches multiple protobuf fields.
func NewUniCacheMultiField(minCacheVersion func() uint64, capacity int, nestedField int, cachedFields []int) UniCache {
	paths := make([][]PathStep, len(cachedFields))
	for i, f := range cachedFields {
		if nestedField > 0 {
			paths[i] = []PathStep{{FieldNum: nestedField}, {FieldNum: f}}
		} else {
			paths[i] = []PathStep{{FieldNum: f}}
		}
	}
	return NewUniCacheWithPaths(minCacheVersion, capacity, paths, nil)
}

// NewUniCacheWithPaths constructs a UniCache with arbitrary-depth field paths.
func NewUniCacheWithPaths(minCacheVersion func() uint64, capacity int, paths [][]PathStep, repeatedLeaves []bool) UniCache {
	roots, numLeaves, numNodes := buildPathTree(paths, repeatedLeaves)

	uc := &uniCache{
		cache:        make(map[uint32]*cacheEntry),
		reverseCache: make(map[string]uint32),

		lruList: list.New(),
		lruMap:  make(map[uint32]*list.Element),

		nextID:   1,
		capacity: capacity,

		evicted:         make(map[uint32]*list.Element),
		evictOrder:      list.New(),
		evictedCapacity: 2 * capacity,

		minCacheVersion: minCacheVersion,

		cachehits:    uint64(0),
		lastInFlight: math.MaxUint64,

		pathRoots: roots,
		numLeaves: numLeaves,
		numNodes:  numNodes,
		paths:     paths,
		repeated:  repeatedLeaves,

		ws: newWorkspace(numLeaves, numNodes),
	}

	return uc
}

// NewUniCache implements the UniCache interface for recreation.
func (uc *uniCache) NewUniCache(minCacheVersion func() uint64, capacity int) UniCache {
	return NewUniCacheWithPaths(minCacheVersion, capacity, uc.paths, uc.repeated)
}

// ===================== STATS =====================

func (uc *uniCache) CacheHits() uint64 {
	return atomic.LoadUint64(&uc.cachehits)
}

func (uc *uniCache) ResetCacheHits() uint64 {
	atomic.StoreUint64(&uc.cachehits, 0)
	return atomic.LoadUint64(&uc.cachehits)
}

func (uc *uniCache) Restores() uint64 {
	return atomic.LoadUint64(&uc.restores)
}

func (uc *uniCache) ResetRestores() uint64 {
	atomic.StoreUint64(&uc.restores, 0)
	return atomic.LoadUint64(&uc.restores)
}

func (uc *uniCache) GetNextId() uint32 {
	return uc.nextID
}

func (uc *uniCache) GetMinCacheIdx(currMinIdx uint64) uint64 {
	if uc.lastInFlight == math.MaxUint64 {
		return currMinIdx
	}
	return uc.lastInFlight
}

// ===================== LRU =====================

func (uc *uniCache) updateLRU(id uint32) {
	if elem, ok := uc.lruMap[id]; ok {
		uc.lruList.MoveToFront(elem)
	}
}

func (uc *uniCache) addToLRU(ce *cacheEntry) {
	elem := uc.lruList.PushFront(ce)
	uc.lruMap[ce.id] = elem
	uc.evictLRU(ce.lastIdx)
}

func (uc *uniCache) evictLRU(currIdx uint64) {
	if len(uc.cache) <= uc.capacity {
		return
	}

	elem := uc.lruList.Back()
	if elem == nil {
		return
	}
	entry := elem.Value.(*cacheEntry)

	if currIdx-entry.lastIdx <= uint64(uc.capacity) {
		return
	}

	evictedElem := uc.evictOrder.PushBack(entry)
	uc.evicted[entry.id] = evictedElem

	delete(uc.cache, entry.id)
	delete(uc.reverseCache, string(entry.key))
	delete(uc.lruMap, entry.id)
	uc.lruList.Remove(elem)

	for len(uc.evicted) > uc.evictedCapacity {
		front := uc.evictOrder.Front()
		if front == nil {
			break
		}
		oldest := front.Value.(*cacheEntry)
		uc.evictOrder.Remove(front)
		delete(uc.evicted, oldest.id)
	}
}

// PurgeEvicted removes entries from the front of evictOrder whose lastIdx is
// below the safety threshold T = max(0, minCacheVersion - capacity).
func (uc *uniCache) PurgeEvicted() {
	minC := uc.minCacheVersion()
	var T uint64
	if minC > uint64(uc.capacity) {
		T = minC - uint64(uc.capacity)
	}
	for uc.evictOrder.Len() > 0 {
		front := uc.evictOrder.Front()
		e := front.Value.(*cacheEntry)
		if e.lastIdx >= T {
			break
		}
		uc.evictOrder.Remove(front)
		delete(uc.evicted, e.id)
	}
	for len(uc.evicted) > uc.evictedCapacity {
		front := uc.evictOrder.Front()
		if front == nil {
			break
		}
		oldest := front.Value.(*cacheEntry)
		uc.evictOrder.Remove(front)
		delete(uc.evicted, oldest.id)
	}
}

// ===================== WALK EXTRACT =====================

// walkExtract performs depth-first traversal of the path tree, extracting all
// leaf field values into the workspace. Returns true if at least one leaf was found.
// ZERO allocations — everything uses pre-allocated workspace and sub-slices.
func (uc *uniCache) walkExtract(data []byte) bool {
	uc.ws.reset()
	anyFound := false

	for _, root := range uc.pathRoots {
		fieldNum := root.step.FieldNum

		val, wt, fStart, fEnd, err := getProtoFieldWithOffset(data, fieldNum)
		if err != nil {
			continue
		}

		fp := fieldPosition{
			fieldStart: fStart,
			fieldEnd:   fEnd,
			skipBytes:  root.step.SkipBytes,
		}

		if root.leafIdx >= 0 && len(root.children) == 0 {
			// Root is a pure leaf
			fp.childNode = -1
			fp.leafIdx = root.leafIdx

			leafVal := val
			if root.step.SkipBytes > 0 && wt == protowire.BytesType && len(val) >= root.step.SkipBytes {
				leafVal = val[root.step.SkipBytes:]
			}
			uc.ws.leafValues[root.leafIdx] = leafVal
			uc.ws.leafWireT[root.leafIdx] = wt
			uc.ws.leafFound[root.leafIdx] = true
			uc.ws.repeatCounts[root.leafIdx] = 1
			anyFound = true
		} else if root.nodeIdx >= 0 {
			// Root is an internal node
			fp.childNode = root.nodeIdx
			fp.leafIdx = -1

			submsg := val
			if root.step.SkipBytes > 0 && len(val) >= root.step.SkipBytes {
				submsg = val[root.step.SkipBytes:]
			}
			uc.ws.nodeSubmsg[root.nodeIdx] = submsg
			uc.ws.nodeFullVal[root.nodeIdx] = val

			if uc.walkExtractChildren(root, submsg) {
				anyFound = true
			}

			// If root is also a leaf (has both children and leaf)
			if root.leafIdx >= 0 {
				uc.ws.leafValues[root.leafIdx] = submsg
				uc.ws.leafWireT[root.leafIdx] = wt
				uc.ws.leafFound[root.leafIdx] = true
				uc.ws.repeatCounts[root.leafIdx] = 1
				anyFound = true
			}
		}

		uc.ws.rootFieldPos = append(uc.ws.rootFieldPos, fp)
	}

	return anyFound
}

// walkExtractChildren extracts child fields from an internal node's submessage.
func (uc *uniCache) walkExtractChildren(node *pathTreeNode, submsg []byte) bool {
	anyFound := false

	for _, child := range node.children {
		// Handle repeated leaf
		if child.leafIdx >= 0 && child.repeated && len(child.children) == 0 {
			if uc.walkExtractRepeatedLeaf(child, submsg, node.nodeIdx) {
				anyFound = true
			}
			continue
		}

		fieldNum := child.step.FieldNum
		cVal, cWT, cStart, cEnd, cErr := getProtoFieldWithOffset(submsg, fieldNum)
		if cErr != nil {
			continue
		}

		fp := fieldPosition{
			fieldStart: cStart,
			fieldEnd:   cEnd,
			skipBytes:  child.step.SkipBytes,
		}

		if child.leafIdx >= 0 && len(child.children) == 0 {
			// Pure leaf child
			fp.childNode = -1
			fp.leafIdx = child.leafIdx

			leafVal := cVal
			if child.step.SkipBytes > 0 && cWT == protowire.BytesType && len(cVal) >= child.step.SkipBytes {
				leafVal = cVal[child.step.SkipBytes:]
			}

			uc.ws.leafValues[child.leafIdx] = leafVal
			uc.ws.leafWireT[child.leafIdx] = cWT
			uc.ws.leafFound[child.leafIdx] = true
			uc.ws.repeatCounts[child.leafIdx] = 1
			anyFound = true
		} else if child.nodeIdx >= 0 {
			// Internal child - recurse
			fp.childNode = child.nodeIdx
			fp.leafIdx = -1

			childSubmsg := cVal
			if child.step.SkipBytes > 0 && len(cVal) >= child.step.SkipBytes {
				childSubmsg = cVal[child.step.SkipBytes:]
			}
			uc.ws.nodeSubmsg[child.nodeIdx] = childSubmsg
			uc.ws.nodeFullVal[child.nodeIdx] = cVal

			if uc.walkExtractChildren(child, childSubmsg) {
				anyFound = true
			}

			// If child is also a leaf
			if child.leafIdx >= 0 {
				uc.ws.leafValues[child.leafIdx] = childSubmsg
				uc.ws.leafWireT[child.leafIdx] = cWT
				uc.ws.leafFound[child.leafIdx] = true
				uc.ws.repeatCounts[child.leafIdx] = 1
				anyFound = true
			}
		}

		uc.ws.nodeChildPos[node.nodeIdx] = append(uc.ws.nodeChildPos[node.nodeIdx], fp)
	}

	return anyFound
}

func (uc *uniCache) walkExtractRepeatedLeaf(node *pathTreeNode, parentSubmsg []byte, parentNodeIdx int) bool {
	fieldNum := node.step.FieldNum

	// Ensure scratch buffer is large enough
	if cap(uc.ws.repeatScratch) < 32 {
		uc.ws.repeatScratch = make([]fieldOccurrence, 32)
	} else {
		uc.ws.repeatScratch = uc.ws.repeatScratch[:cap(uc.ws.repeatScratch)]
	}

	count, err := getRepeatedFieldsWithOffsets(parentSubmsg, fieldNum, uc.ws.repeatScratch)
	if err != nil || count == 0 {
		return false
	}

	// If we need more space, grow and retry
	if count > len(uc.ws.repeatScratch) {
		uc.ws.repeatScratch = make([]fieldOccurrence, count+8)
		count, err = getRepeatedFieldsWithOffsets(parentSubmsg, fieldNum, uc.ws.repeatScratch)
		if err != nil || count == 0 {
			return false
		}
	}

	lidx := node.leafIdx

	// First occurrence
	occ := uc.ws.repeatScratch[0]
	val := occ.value
	if node.step.SkipBytes > 0 && len(val) >= node.step.SkipBytes {
		val = val[node.step.SkipBytes:]
	}
	uc.ws.leafValues[lidx] = val
	if len(parentSubmsg) > occ.fieldStart {
		uc.ws.leafWireT[lidx] = protowire.Type(parentSubmsg[occ.fieldStart] & 0x07)
	}
	uc.ws.leafFound[lidx] = true
	uc.ws.repeatCounts[lidx] = count

	// Record field positions for parent
	for i := 0; i < count; i++ {
		fp := fieldPosition{
			fieldStart: uc.ws.repeatScratch[i].fieldStart,
			fieldEnd:   uc.ws.repeatScratch[i].fieldEnd,
			childNode:  -1,
			leafIdx:    lidx,
			skipBytes:  node.step.SkipBytes,
		}
		uc.ws.nodeChildPos[parentNodeIdx] = append(uc.ws.nodeChildPos[parentNodeIdx], fp)
	}

	// Additional occurrences go into repeatValues
	if count > 1 {
		for i := 1; i < count; i++ {
			rVal := uc.ws.repeatScratch[i].value
			if node.step.SkipBytes > 0 && len(rVal) >= node.step.SkipBytes {
				rVal = rVal[node.step.SkipBytes:]
			}
			uc.ws.repeatValues = append(uc.ws.repeatValues, rVal)
		}
	}

	return true
}

// ===================== TWO-PHASE REPLACEMENT =====================

// Phase A: computeDeltas computes size deltas bottom-up and stores them in
// ws.nodeDelta. Returns the total delta for the outermost buffer.
func (uc *uniCache) computeDeltas(replacements map[int]leafReplacement) int {
	// Compute per-node deltas bottom-up
	for _, root := range uc.pathRoots {
		if root.nodeIdx >= 0 {
			uc.computeNodeDelta(root, replacements)
		}
	}

	// Compute total root-level delta
	totalDelta := 0
	for ri, root := range uc.pathRoots {
		if ri >= len(uc.ws.rootFieldPos) {
			continue
		}
		rfp := uc.ws.rootFieldPos[ri]

		if root.leafIdx >= 0 && len(root.children) == 0 {
			if repl, ok := replacements[root.leafIdx]; ok {
				oldSize := rfp.fieldEnd - rfp.fieldStart
				newSize := encodedFieldSize(root.step.FieldNum, repl, rfp.skipBytes)
				totalDelta += newSize - oldSize
			}
		} else if root.nodeIdx >= 0 {
			contentDelta := uc.ws.nodeDelta[root.nodeIdx]
			if contentDelta != 0 {
				oldSubmsgLen := len(uc.ws.nodeFullVal[root.nodeIdx])
				newSubmsgLen := oldSubmsgLen + contentDelta
				oldVarSize := protowire.SizeVarint(uint64(oldSubmsgLen))
				newVarSize := protowire.SizeVarint(uint64(newSubmsgLen))

				oldFieldSize := rfp.fieldEnd - rfp.fieldStart
				newFieldSize := oldFieldSize + contentDelta + (newVarSize - oldVarSize)
				totalDelta += newFieldSize - oldFieldSize
			}
		}
	}

	return totalDelta
}

// computeNodeDelta computes and stores the content-level delta for a node's
// submessage. The delta is the change in bytes of the submessage content only
// (not including the node's own tag or length prefix).
func (uc *uniCache) computeNodeDelta(node *pathTreeNode, replacements map[int]leafReplacement) {
	if node.nodeIdx < 0 {
		return
	}

	childPositions := uc.ws.nodeChildPos[node.nodeIdx]
	delta := 0

	for _, fp := range childPositions {
		if fp.childNode >= 0 {
			childNode := findNodeByIdxIn(uc.pathRoots, fp.childNode)
			if childNode == nil {
				continue
			}
			// Recurse first to compute child delta
			uc.computeNodeDelta(childNode, replacements)
			childContentDelta := uc.ws.nodeDelta[fp.childNode]
			if childContentDelta != 0 {
				oldSubmsgLen := len(uc.ws.nodeFullVal[fp.childNode])
				newSubmsgLen := oldSubmsgLen + childContentDelta
				oldVarSize := protowire.SizeVarint(uint64(oldSubmsgLen))
				newVarSize := protowire.SizeVarint(uint64(newSubmsgLen))

				oldFieldSize := fp.fieldEnd - fp.fieldStart
				newFieldSize := oldFieldSize + childContentDelta + (newVarSize - oldVarSize)
				delta += newFieldSize - oldFieldSize
			}
		} else if fp.leafIdx >= 0 {
			if repl, ok := replacements[fp.leafIdx]; ok {
				leafNode := findLeafByIdxIn(uc.pathRoots, fp.leafIdx)
				if leafNode == nil {
					continue
				}
				oldSize := fp.fieldEnd - fp.fieldStart
				newSize := encodedFieldSize(leafNode.step.FieldNum, repl, fp.skipBytes)
				delta += newSize - oldSize
			}
		}
	}

	uc.ws.nodeDelta[node.nodeIdx] = delta
}

// encodedFieldSize computes the encoded size of a replacement field.
func encodedFieldSize(fieldNum int, repl leafReplacement, skipBytes int) int {
	tagSize := protowire.SizeTag(protowire.Number(fieldNum))
	valLen := len(repl.newValue)
	if skipBytes > 0 && repl.newWireType == protowire.BytesType {
		valLen += skipBytes
	}
	if repl.newWireType == protowire.BytesType {
		return tagSize + protowire.SizeVarint(uint64(valLen)) + valLen
	}
	return tagSize + valLen
}

// Phase B: buildOutput allocates ONE output buffer of exact size and fills it
// with a top-down recursive copy. Unchanged segments are copied verbatim,
// replacements are spliced at leaves, and new length prefixes are written at
// internal nodes using the pre-computed deltas.
func (uc *uniCache) buildOutput(data []byte, replacements map[int]leafReplacement) []byte {
	totalDelta := uc.computeDeltas(replacements)
	totalSize := len(data) + totalDelta
	if totalSize <= 0 {
		totalSize = len(data)
	}

	// THE single allocation
	out := make([]byte, 0, totalSize)

	// Sort root field positions by fieldStart
	positions := uc.ws.rootFieldPos
	for i := 1; i < len(positions); i++ {
		for j := i; j > 0 && positions[j].fieldStart < positions[j-1].fieldStart; j-- {
			positions[j], positions[j-1] = positions[j-1], positions[j]
		}
	}

	cursor := 0
	for _, rfp := range positions {
		// Find corresponding root node by matching leafIdx or nodeIdx
		var root *pathTreeNode
		for _, r := range uc.pathRoots {
			if r.leafIdx >= 0 && rfp.leafIdx == r.leafIdx && len(r.children) == 0 {
				root = r
				break
			}
			if r.nodeIdx >= 0 && rfp.childNode == r.nodeIdx {
				root = r
				break
			}
		}
		if root == nil {
			continue
		}

		needsMod := false
		if root.leafIdx >= 0 && len(root.children) == 0 {
			_, needsMod = replacements[root.leafIdx]
		} else if root.nodeIdx >= 0 {
			needsMod = uc.ws.nodeDelta[root.nodeIdx] != 0
		}

		if !needsMod {
			continue
		}

		// Copy data before this field
		out = append(out, data[cursor:rfp.fieldStart]...)

		if root.leafIdx >= 0 && len(root.children) == 0 {
			out = uc.writeLeafField(out, root, replacements[root.leafIdx], rfp.skipBytes, data)
		} else if root.nodeIdx >= 0 {
			out = uc.writeNodeField(out, root, replacements)
		}

		cursor = rfp.fieldEnd
	}

	// Copy remaining data
	out = append(out, data[cursor:]...)

	return out
}

// writeLeafField appends a replaced leaf field directly to out.
// parentData is the enclosing message data (for extracting skipBytes prefix).
func (uc *uniCache) writeLeafField(out []byte, node *pathTreeNode, repl leafReplacement, skipBytes int, parentData []byte) []byte {
	out = protowire.AppendTag(out, protowire.Number(node.step.FieldNum), repl.newWireType)
	if skipBytes > 0 && repl.newWireType == protowire.BytesType {
		totalLen := skipBytes + len(repl.newValue)
		out = protowire.AppendVarint(out, uint64(totalLen))
		// Extract the original field value to get the skipBytes prefix
		origVal, _, _, _, err := getProtoFieldWithOffset(parentData, node.step.FieldNum)
		if err == nil && len(origVal) >= skipBytes {
			out = append(out, origVal[:skipBytes]...)
		}
		out = append(out, repl.newValue...)
	} else if repl.newWireType == protowire.BytesType {
		out = protowire.AppendBytes(out, repl.newValue)
	} else {
		out = append(out, repl.newValue...)
	}
	return out
}

// writeNodeField appends an internal node field directly to out.
// It uses pre-computed nodeDelta to write the correct length prefix,
// then recurses to write submessage content directly into the same buffer.
func (uc *uniCache) writeNodeField(out []byte, node *pathTreeNode, replacements map[int]leafReplacement) []byte {
	nIdx := node.nodeIdx
	submsg := uc.ws.nodeSubmsg[nIdx]
	fullVal := uc.ws.nodeFullVal[nIdx]
	skipBytes := node.step.SkipBytes
	contentDelta := uc.ws.nodeDelta[nIdx]

	// Compute new full value length (including skipBytes prefix)
	newFullValLen := len(fullVal) + contentDelta

	// Write tag + length prefix
	out = protowire.AppendTag(out, protowire.Number(node.step.FieldNum), protowire.BytesType)
	out = protowire.AppendVarint(out, uint64(newFullValLen))

	// Write skipBytes prefix verbatim
	if skipBytes > 0 {
		out = append(out, fullVal[:skipBytes]...)
	}

	// Write submessage content with replacements spliced in
	out = uc.writeSubmsgContent(out, node, submsg, replacements)

	return out
}

// writeSubmsgContent writes submessage bytes with child replacements directly
// into out. No intermediate buffer — everything goes straight to out.
func (uc *uniCache) writeSubmsgContent(out []byte, node *pathTreeNode, submsg []byte, replacements map[int]leafReplacement) []byte {
	childPositions := uc.ws.nodeChildPos[node.nodeIdx]
	if len(childPositions) == 0 {
		return append(out, submsg...)
	}

	// Sort child positions by fieldStart
	for i := 1; i < len(childPositions); i++ {
		for j := i; j > 0 && childPositions[j].fieldStart < childPositions[j-1].fieldStart; j-- {
			childPositions[j], childPositions[j-1] = childPositions[j-1], childPositions[j]
		}
	}

	cursor := 0

	for _, fp := range childPositions {
		needsReplacement := false
		if fp.childNode >= 0 {
			needsReplacement = uc.ws.nodeDelta[fp.childNode] != 0
		} else if fp.leafIdx >= 0 {
			_, needsReplacement = replacements[fp.leafIdx]
		}

		if !needsReplacement {
			continue
		}

		// Copy unchanged bytes before this child
		out = append(out, submsg[cursor:fp.fieldStart]...)

		if fp.childNode >= 0 {
			// Recursive: write child node field directly into out
			childNode := findNodeByIdxIn(uc.pathRoots, fp.childNode)
			if childNode != nil {
				out = uc.writeNodeField(out, childNode, replacements)
			}
		} else if fp.leafIdx >= 0 {
			// Write leaf replacement directly into out
			repl := replacements[fp.leafIdx]
			leafNode := findLeafByIdxIn(uc.pathRoots, fp.leafIdx)
			if leafNode != nil {
				out = protowire.AppendTag(out, protowire.Number(leafNode.step.FieldNum), repl.newWireType)
				if fp.skipBytes > 0 && repl.newWireType == protowire.BytesType {
					totalLen := fp.skipBytes + len(repl.newValue)
					out = protowire.AppendVarint(out, uint64(totalLen))
					// Get original prefix from the field value in submessage
					origFullVal, _, _, _, err := getProtoFieldWithOffset(submsg, leafNode.step.FieldNum)
					if err == nil && len(origFullVal) >= fp.skipBytes {
						out = append(out, origFullVal[:fp.skipBytes]...)
					}
					out = append(out, repl.newValue...)
				} else if repl.newWireType == protowire.BytesType {
					out = protowire.AppendBytes(out, repl.newValue)
				} else {
					out = append(out, repl.newValue...)
				}
			}
		}

		cursor = fp.fieldEnd
	}

	// Copy remaining submessage bytes
	out = append(out, submsg[cursor:]...)

	return out
}

// findNodeByIdxIn finds a node by its nodeIdx in the tree.
func findNodeByIdxIn(roots []*pathTreeNode, nodeIdx int) *pathTreeNode {
	for _, root := range roots {
		if n := findNodeByIdxRecursive(root, nodeIdx); n != nil {
			return n
		}
	}
	return nil
}

func findNodeByIdxRecursive(node *pathTreeNode, nodeIdx int) *pathTreeNode {
	if node.nodeIdx == nodeIdx {
		return node
	}
	for _, child := range node.children {
		if n := findNodeByIdxRecursive(child, nodeIdx); n != nil {
			return n
		}
	}
	return nil
}

// findLeafByIdxIn finds a leaf node by its leafIdx in the tree.
func findLeafByIdxIn(roots []*pathTreeNode, leafIdx int) *pathTreeNode {
	for _, root := range roots {
		if n := findLeafRecursive(root, leafIdx); n != nil {
			return n
		}
	}
	return nil
}

func findLeafRecursive(node *pathTreeNode, leafIdx int) *pathTreeNode {
	if node.leafIdx == leafIdx {
		return node
	}
	for _, child := range node.children {
		if n := findLeafRecursive(child, leafIdx); n != nil {
			return n
		}
	}
	return nil
}

// twoPhaseReplace performs the complete two-phase replacement.
func (uc *uniCache) twoPhaseReplace(data []byte, replacements map[int]leafReplacement) ([]byte, error) {
	if len(replacements) == 0 {
		return data, nil
	}
	return uc.buildOutput(data, replacements), nil
}

// ===================== ENCODE / DECODE =====================

// EncodeData encodes cached fields in data. Returns the encoded data
// and a slice of cache IDs.
// Layout for IDs: non-repeated leaf = 1 slot. Repeated leaf = count + IDs.
func (uc *uniCache) EncodeData(data []byte, currCacheIdx uint64) ([]byte, []uint32) {
	if len(data) == 0 {
		return data, nil
	}

	uc.wsMu.Lock()
	defer uc.wsMu.Unlock()

	if !uc.walkExtract(data) {
		return data, nil
	}

	// Compute total ID slots needed
	totalSlots := 0
	for i := 0; i < uc.numLeaves; i++ {
		leaf := findLeafByIdxIn(uc.pathRoots, i)
		if leaf != nil && leaf.repeated && uc.ws.leafFound[i] && uc.ws.repeatCounts[i] > 1 {
			totalSlots += 1 + uc.ws.repeatCounts[i] // count + IDs
		} else {
			totalSlots++
		}
	}

	ids := make([]uint32, totalSlots)
	replacements := make(map[int]leafReplacement)
	anyHit := false

	slotIdx := 0
	repeatValueOffset := 0
	for i := 0; i < uc.numLeaves; i++ {
		leaf := findLeafByIdxIn(uc.pathRoots, i)
		isRepeated := leaf != nil && leaf.repeated && uc.ws.leafFound[i] && uc.ws.repeatCounts[i] > 1

		if !uc.ws.leafFound[i] {
			if isRepeated {
				slotIdx += 1 + uc.ws.repeatCounts[i]
			} else {
				slotIdx++
			}
			continue
		}

		if isRepeated {
			count := uc.ws.repeatCounts[i]
			ids[slotIdx] = uint32(count) // count prefix
			slotIdx++

			// First value
			keyStr := string(uc.ws.leafValues[i])
			if id, ok := uc.reverseCache[keyStr]; ok {
				if _, exists := uc.cache[id]; exists {
					if uc.isSafeID(id) {
						ids[slotIdx] = id
					}
				}
			}
			slotIdx++

			// Remaining values
			for j := 1; j < count; j++ {
				rIdx := repeatValueOffset + (j - 1)
				if rIdx < len(uc.ws.repeatValues) {
					rKeyStr := string(uc.ws.repeatValues[rIdx])
					if id, ok := uc.reverseCache[rKeyStr]; ok {
						if _, exists := uc.cache[id]; exists {
							if uc.isSafeID(id) {
								ids[slotIdx] = id
							}
						}
					}
				}
				slotIdx++
			}
			repeatValueOffset += count - 1
		} else {
			if uc.ws.leafWireT[i] != protowire.BytesType {
				slotIdx++
				continue
			}

			keyStr := string(uc.ws.leafValues[i])
			id, ok := uc.reverseCache[keyStr]
			if !ok {
				slotIdx++
				continue
			}
			if _, exists := uc.cache[id]; !exists {
				slotIdx++
				continue
			}
			if !uc.isSafeID(id) {
				slotIdx++
				continue
			}

			encodedID := protowire.AppendVarint(nil, uint64(id))
			replacements[i] = leafReplacement{newValue: encodedID, newWireType: protowire.VarintType}
			ids[slotIdx] = id
			anyHit = true
			slotIdx++
		}
	}

	if !anyHit {
		return data, nil
	}

	newData, err := uc.twoPhaseReplace(data, replacements)
	if err != nil {
		return data, nil
	}
	return newData, ids
}

func (uc *uniCache) isSafeID(id uint32) bool {
	if uc.nextID > uint32(uc.capacity) {
		minActiveID := uc.nextID - uint32(uc.capacity/2)
		if id < minActiveID {
			return false
		}
	}
	return true
}

// DecodeEntry restores all encoded fields back to original bytes.
func (uc *uniCache) DecodeEntry(entry pb.Entry) (pb.Entry, bool) {
	if len(entry.Data) == 0 {
		return entry, true
	}

	uc.wsMu.Lock()
	defer uc.wsMu.Unlock()

	return uc.decodeEntryLocked(entry)
}

// decodeEntryLocked is the internal version that assumes wsMu is held.
func (uc *uniCache) decodeEntryLocked(entry pb.Entry) (pb.Entry, bool) {

	// Fast check: if all fields are unencoded, nothing to do.
	if uc.allUnencoded(entry.Data) {
		return entry, true
	}

	if !uc.walkExtract(entry.Data) {
		return entry, true
	}

	replacements := make(map[int]leafReplacement)
	anyDecoded := false

	for i := 0; i < uc.numLeaves; i++ {
		if !uc.ws.leafFound[i] {
			continue
		}

		if uc.ws.leafWireT[i] == protowire.BytesType {
			continue // already decoded
		}

		if uc.ws.leafWireT[i] == protowire.VarintType {
			id, n := protowire.ConsumeVarint(uc.ws.leafValues[i])
			if n <= 0 {
				return entry, false
			}

			elem, ok := uc.cache[uint32(id)]
			if !ok {
				return entry, false
			}

			replacements[i] = leafReplacement{newValue: elem.key, newWireType: protowire.BytesType}
			anyDecoded = true
		}
	}

	if !anyDecoded {
		return entry, true
	}

	newData, err := uc.twoPhaseReplace(entry.Data, replacements)
	if err != nil {
		return entry, false
	}
	entry.Data = newData
	return entry, true
}

// allUnencoded checks if all cached fields are in original bytes form.
func (uc *uniCache) allUnencoded(data []byte) bool {
	if !uc.walkExtract(data) {
		return true
	}
	for i := 0; i < uc.numLeaves; i++ {
		if !uc.ws.leafFound[i] {
			continue
		}
		if uc.ws.leafWireT[i] != protowire.BytesType {
			return false
		}
	}
	return true
}

// hasAnyEncoded checks if any cached field in data is varint-encoded.
func (uc *uniCache) hasAnyEncoded(data []byte) bool {
	if !uc.walkExtract(data) {
		return false
	}
	for i := 0; i < uc.numLeaves; i++ {
		if uc.ws.leafFound[i] && uc.ws.leafWireT[i] == protowire.VarintType {
			return true
		}
	}
	return false
}

// SafeEncode checks if all encoded IDs are safe to send, returns (encoded, full) data.
func (uc *uniCache) SafeEncode(data []byte, appendIdx uint64, encodedIDs []uint32) ([]byte, []byte) {
	if len(data) == 0 || len(encodedIDs) == 0 {
		return data, nil
	}

	uc.wsMu.Lock()
	defer uc.wsMu.Unlock()

	// Check if any ID is non-zero
	anyEncoded := false
	for _, id := range encodedIDs {
		if id != 0 {
			anyEncoded = true
			break
		}
	}
	if !anyEncoded {
		return data, nil
	}

	// Parse encodedIDs: handle repeated count-prefix layout
	type leafID struct {
		leafIdx int
		id      uint32
	}
	var leafIDs []leafID
	slotIdx := 0
	for i := 0; i < uc.numLeaves && slotIdx < len(encodedIDs); i++ {
		leaf := findLeafByIdxIn(uc.pathRoots, i)
		isRepeated := leaf != nil && leaf.repeated

		if isRepeated && slotIdx < len(encodedIDs) {
			count := int(encodedIDs[slotIdx])
			slotIdx++
			for j := 0; j < count && slotIdx < len(encodedIDs); j++ {
				if encodedIDs[slotIdx] != 0 {
					leafIDs = append(leafIDs, leafID{leafIdx: i, id: encodedIDs[slotIdx]})
				}
				slotIdx++
			}
		} else {
			if slotIdx < len(encodedIDs) && encodedIDs[slotIdx] != 0 {
				leafIDs = append(leafIDs, leafID{leafIdx: i, id: encodedIDs[slotIdx]})
			}
			slotIdx++
		}
	}

	// Walk extract to get field positions
	uc.walkExtract(data)

	// Check safety for all encoded IDs
	allSafe := true
	replacements := make(map[int]leafReplacement)

	for _, lid := range leafIDs {
		elem, ok := uc.cache[lid.id]
		if !ok {
			// Check evicted
			if evElem, ok := uc.evicted[lid.id]; ok {
				ev := evElem.Value.(*cacheEntry)
				atomic.AddUint64(&uc.restores, 1)
				replacements[lid.leafIdx] = leafReplacement{newValue: ev.key, newWireType: protowire.BytesType}
				allSafe = false
				continue
			}
			return nil, nil
		}

		minCV := uc.minCacheVersion()
		distOk := appendIdx-elem.lastIdx <= uint64(uc.capacity)
		versionOk := minCV >= elem.addedIdx

		if !distOk || !versionOk {
			restoreCount := atomic.AddUint64(&uc.restores, 1)
			if restoreCount <= 5 || restoreCount%10000 == 0 {
				fmt.Printf("[SafeEncode restore] appendIdx=%d lastIdx=%d addedIdx=%d minCV=%d capacity=%d distOk=%v versionOk=%v\n",
					appendIdx, elem.lastIdx, elem.addedIdx, minCV, uc.capacity, distOk, versionOk)
			}
			replacements[lid.leafIdx] = leafReplacement{newValue: elem.key, newWireType: protowire.BytesType}
			allSafe = false
		}
	}

	if allSafe {
		// All IDs are safe to send encoded
		atomic.AddUint64(&uc.cachehits, 1)

		// Build fullData with all fields restored
		restoreAll := make(map[int]leafReplacement)
		for _, lid := range leafIDs {
			elem := uc.cache[lid.id]
			restoreAll[lid.leafIdx] = leafReplacement{newValue: elem.key, newWireType: protowire.BytesType}
		}
		// Need to re-walk since we'll use workspace for replacement
		uc.walkExtract(data)
		fullData, err := uc.twoPhaseReplace(data, restoreAll)
		if err != nil {
			return nil, nil
		}
		return data, fullData
	}

	// Not all safe — restore everything
	for _, lid := range leafIDs {
		if _, already := replacements[lid.leafIdx]; already {
			continue
		}
		elem, ok := uc.cache[lid.id]
		if !ok {
			continue
		}
		replacements[lid.leafIdx] = leafReplacement{newValue: elem.key, newWireType: protowire.BytesType}
	}

	// Re-walk since twoPhaseReplace needs fresh workspace
	uc.walkExtract(data)
	newData, err := uc.twoPhaseReplace(data, replacements)
	if err != nil {
		return nil, nil
	}
	return newData, newData
}

// ===================== CACHE UPDATE =====================

func (uc *uniCache) UpdateCache(entry pb.Entry) (pb.Entry, bool) {
	if len(entry.Data) == 0 {
		return entry, true
	}

	uc.wsMu.Lock()
	defer uc.wsMu.Unlock()

	// If entry has encoded fields, decode first
	if uc.hasAnyEncoded(entry.Data) {
		decoded, ok := uc.decodeEntryLocked(entry)
		if !ok {
			return entry, false
		}
		entry = decoded
	}

	if !uc.allUnencoded(entry.Data) {
		return entry, false
	}

	uc.walkExtract(entry.Data)

	for i := 0; i < uc.numLeaves; i++ {
		if !uc.ws.leafFound[i] || uc.ws.leafWireT[i] != protowire.BytesType {
			continue
		}
		uc.addOrUpdateCacheKey(uc.ws.leafValues[i], entry.Index)
	}

	return entry, true
}

// addOrUpdateCacheKey adds a key to the cache or updates its lastIdx.
func (uc *uniCache) addOrUpdateCacheKey(keyField []byte, entryIndex uint64) {
	keyStr := string(keyField)

	if id, exists := uc.reverseCache[keyStr]; exists {
		ent := uc.cache[id]
		if ent.lastIdx < entryIndex {
			ent.lastIdx = entryIndex
			uc.updateLRU(id)
		}
		return
	}

	newID := uc.nextID
	uc.nextID++
	newElem := &cacheEntry{
		id:       newID,
		key:      keyField,
		lastIdx:  entryIndex,
		addedIdx: entryIndex,
	}
	uc.cache[newID] = newElem
	uc.reverseCache[keyStr] = newID
	uc.addToLRU(newElem)
}

// updateExistingCacheEntry updates lastIdx and LRU for an entry already in the
// active cache. Returns false if the id is not in the active cache.
func (uc *uniCache) updateExistingCacheEntry(id uint32, entryIndex uint64) bool {
	ent, ok := uc.cache[id]
	if !ok {
		return false
	}
	if ent.lastIdx < entryIndex {
		ent.lastIdx = entryIndex
		uc.updateLRU(id)
	}
	return true
}

// updateCacheFromEvicted restores an evicted key as a new cache entry.
func (uc *uniCache) updateCacheFromEvicted(evictedID uint32, entryIndex uint64) bool {
	evElem, ok := uc.evicted[evictedID]
	if !ok {
		return false
	}
	ev := evElem.Value.(*cacheEntry)

	uc.evictOrder.Remove(evElem)
	delete(uc.evicted, evictedID)

	newID := uc.nextID
	uc.nextID++
	newElem := &cacheEntry{
		id:       newID,
		key:      ev.key,
		lastIdx:  entryIndex,
		addedIdx: entryIndex,
	}
	uc.cache[newID] = newElem
	uc.reverseCache[string(ev.key)] = newID
	uc.addToLRU(newElem)
	return true
}

func (uc *uniCache) BatchUpdateCache(entries []pb.Entry) ([]pb.Entry, bool) {
	uc.wsMu.Lock()
	defer uc.wsMu.Unlock()

	for i := range entries {
		entry := entries[i]

		if len(entry.Data) == 0 || entry.Type != pb.EntryNormal {
			continue
		}

		// Fast path: EncodedIDs from the multi-field path
		if len(entry.EncodedIDs) > 0 {
			allFound := true
			for _, id := range entry.EncodedIDs {
				if id == 0 {
					continue
				}
				if uc.updateExistingCacheEntry(id, entry.Index) {
					continue
				}
				if uc.updateCacheFromEvicted(id, entry.Index) {
					continue
				}
				allFound = false
			}
			if allFound {
				continue
			}
			// Fall through to slow path for any missed IDs
		}

		// Legacy fast path: single EncodedID (backward compat)
		if entry.EncodedID != 0 && len(entry.EncodedIDs) == 0 {
			if uc.updateExistingCacheEntry(entry.EncodedID, entry.Index) {
				continue
			}
			if uc.updateCacheFromEvicted(entry.EncodedID, entry.Index) {
				continue
			}
		}

		// Slow path: parse proto to extract keys
		currentData := entry.Data

		if uc.hasAnyEncoded(currentData) {
			decoded, ok := uc.decodeEntryLocked(entry)
			if !ok {
				return nil, false
			}
			currentData = decoded.Data
		}

		if !uc.allUnencoded(currentData) {
			return nil, false
		}

		uc.walkExtract(currentData)

		for li := 0; li < uc.numLeaves; li++ {
			if !uc.ws.leafFound[li] || uc.ws.leafWireT[li] != protowire.BytesType {
				continue
			}
			uc.addOrUpdateCacheKey(uc.ws.leafValues[li], entry.Index)
		}
	}

	return entries, true
}

// ===================== STATIC HELPERS =====================

// IsEncodedData reports whether data contains a RepliCache-encoded entry.
func IsEncodedData(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	putBytes, putWT, err := GetProtoFieldAndWireType(data, 4)
	if err != nil || putWT != protowire.BytesType {
		// Try flat layout
		return len(data) > 0 && protowire.Type(data[0]&0x07) == protowire.VarintType
	}
	if len(putBytes) == 0 {
		return false
	}
	return len(putBytes) > 0 && protowire.Type(putBytes[0]&0x07) == protowire.VarintType
}

// ===================== PROTO HELPERS (kept for external use) =====================

// ReplaceProtoField replaces a single protobuf field (kept for compatibility).
func ReplaceProtoField(data []byte, targetField int, newValue []byte, newWireType protowire.Type) ([]byte, error) {
	// Fast path: when the target field is first in the serialized data
	if len(data) > 0 && int(data[0]>>3) == targetField {
		oldWireType := protowire.Type(data[0] & 0x07)
		var fieldEnd int
		switch oldWireType {
		case protowire.VarintType:
			_, n := protowire.ConsumeVarint(data[1:])
			if n < 0 {
				return replaceProtoFieldGeneral(data, targetField, newValue, newWireType)
			}
			fieldEnd = 1 + n
		case protowire.BytesType:
			_, n := protowire.ConsumeBytes(data[1:])
			if n < 0 {
				return replaceProtoFieldGeneral(data, targetField, newValue, newWireType)
			}
			fieldEnd = 1 + n
		default:
			return replaceProtoFieldGeneral(data, targetField, newValue, newWireType)
		}
		rest := data[fieldEnd:]

		newTag := byte(targetField<<3) | byte(newWireType)

		var newFieldLen int
		if newWireType == protowire.BytesType {
			newFieldLen = protowire.SizeVarint(uint64(len(newValue))) + len(newValue)
		} else {
			newFieldLen = len(newValue)
		}
		out := make([]byte, 0, 1+newFieldLen+len(rest))
		out = append(out, newTag)
		if newWireType == protowire.BytesType {
			out = protowire.AppendVarint(out, uint64(len(newValue)))
			out = append(out, newValue...)
		} else {
			out = append(out, newValue...)
		}
		out = append(out, rest...)
		return out, nil
	}
	return replaceProtoFieldGeneral(data, targetField, newValue, newWireType)
}

func replaceProtoFieldGeneral(data []byte, targetField int, newValue []byte, newWireType protowire.Type) ([]byte, error) {
	var out []byte
	buf := data

	for len(buf) > 0 {
		fieldNum, wireType, n := protowire.ConsumeTag(buf)
		if n < 0 {
			return nil, errors.New("failed to consume tag")
		}
		tagBytes := buf[:n]
		buf = buf[n:]

		shouldReplace := int(fieldNum) == targetField

		var skip int
		switch wireType {
		case protowire.VarintType:
			_, skip = protowire.ConsumeVarint(buf)
		case protowire.Fixed32Type:
			_, skip = protowire.ConsumeFixed32(buf)
		case protowire.Fixed64Type:
			_, skip = protowire.ConsumeFixed64(buf)
		case protowire.BytesType:
			_, skip = protowire.ConsumeBytes(buf)
		case protowire.StartGroupType:
			_, skip = protowire.ConsumeGroup(fieldNum, buf)
		default:
			return nil, fmt.Errorf("unknown wire type: %v", wireType)
		}
		if skip < 0 {
			return nil, errors.New("failed to consume field value")
		}

		if shouldReplace {
			newTag := protowire.AppendTag(nil, protowire.Number(fieldNum), newWireType)
			out = append(out, newTag...)
			if newWireType == protowire.BytesType {
				out = protowire.AppendBytes(out, newValue)
			} else {
				out = append(out, newValue...)
			}
		} else {
			out = append(out, tagBytes...)
			out = append(out, buf[:skip]...)
		}
		buf = buf[skip:]
	}

	return out, nil
}

// GetProtoFieldAndWireType extracts a single field's value and wire type.
func GetProtoFieldAndWireType(data []byte, targetField int) ([]byte, protowire.Type, error) {
	// Fast path: target field is first in the serialized data.
	if len(data) > 0 && int(data[0]>>3) == targetField {
		wireType := protowire.Type(data[0] & 0x07)
		switch wireType {
		case protowire.VarintType:
			_, n := protowire.ConsumeVarint(data[1:])
			if n > 0 {
				return data[1 : 1+n], wireType, nil
			}
		case protowire.BytesType:
			v, n := protowire.ConsumeBytes(data[1:])
			if n > 0 {
				return v, wireType, nil
			}
		}
	}
	return getProtoFieldGeneral(data, targetField)
}

func getProtoFieldGeneral(data []byte, targetField int) ([]byte, protowire.Type, error) {
	for len(data) > 0 {
		fieldNum, wireType, n := protowire.ConsumeTag(data)
		if n < 0 {
			return nil, 0, errors.New("failed to consume tag")
		}
		data = data[n:]
		if int(fieldNum) == targetField {
			switch wireType {
			case protowire.VarintType:
				v, nn := protowire.ConsumeVarint(data)
				if nn < 0 {
					return nil, 0, errors.New("failed to consume varint")
				}
				return protowire.AppendVarint(nil, v), wireType, nil
			case protowire.BytesType:
				v, nn := protowire.ConsumeBytes(data)
				if nn < 0 {
					return nil, 0, errors.New("failed to consume bytes")
				}
				return v, wireType, nil
			case protowire.Fixed32Type:
				v, nn := protowire.ConsumeFixed32(data)
				if nn < 0 {
					return nil, 0, errors.New("failed to consume fixed32")
				}
				return protowire.AppendFixed32(nil, v), wireType, nil
			case protowire.Fixed64Type:
				v, nn := protowire.ConsumeFixed64(data)
				if nn < 0 {
					return nil, 0, errors.New("failed to consume fixed64")
				}
				return protowire.AppendFixed64(nil, v), wireType, nil
			case protowire.StartGroupType:
				v, nn := protowire.ConsumeGroup(fieldNum, data)
				if nn < 0 {
					return nil, 0, errors.New("failed to consume group")
				}
				return v, wireType, nil
			default:
				return nil, 0, fmt.Errorf("unknown wire type: %v", wireType)
			}
		} else {
			skip := skipField(wireType, fieldNum, data)
			if skip < 0 {
				return nil, 0, errors.New("failed to skip field")
			}
			data = data[skip:]
		}
	}
	return nil, 0, fmt.Errorf("field number %d not found", targetField)
}

// skipField skips over a protobuf field value of the given wire type.
func skipField(wireType protowire.Type, fieldNum protowire.Number, data []byte) int {
	switch wireType {
	case protowire.VarintType:
		_, n := protowire.ConsumeVarint(data)
		return n
	case protowire.Fixed32Type:
		_, n := protowire.ConsumeFixed32(data)
		return n
	case protowire.Fixed64Type:
		_, n := protowire.ConsumeFixed64(data)
		return n
	case protowire.BytesType:
		_, n := protowire.ConsumeBytes(data)
		return n
	case protowire.StartGroupType:
		_, n := protowire.ConsumeGroup(fieldNum, data)
		return n
	default:
		return -1
	}
}
