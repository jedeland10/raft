package unicache

import (
	"bytes"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
)

// --- helpers to build/inspect MyKV -----------------------------------------

// myKVBytes builds a protobuf message for:
// message MyKV { bytes key=1; bytes value=2; optional uint64 proposalID=3; }
func myKVBytes(key, value []byte, proposalID *uint64) []byte {
	var b []byte
	// field 1: key
	b = protowire.AppendTag(b, 1, protowire.BytesType)
	b = protowire.AppendBytes(b, key)
	// field 2: value
	b = protowire.AppendTag(b, 2, protowire.BytesType)
	b = protowire.AppendBytes(b, value)
	// field 3: optional proposalID
	if proposalID != nil {
		b = protowire.AppendTag(b, 3, protowire.VarintType)
		b = protowire.AppendVarint(b, *proposalID)
	}
	return b
}

// parseKeyField inspects field #1 of a MyKV message and returns its wire type,
// the key bytes if BytesType, or the id if VarintType.
func parseKeyField(b []byte) (typ protowire.Type, key []byte, id uint32, ok bool) {
	// Expect field #1 first (we encode in field order)
	num, typ, n := protowire.ConsumeTag(b)
	if n <= 0 || num != cachedFieldNumber {
		return 0, nil, 0, false
	}
	b = b[n:]
	switch typ {
	case protowire.BytesType:
		v, m := protowire.ConsumeBytes(b)
		if m <= 0 {
			return 0, nil, 0, false
		}
		return typ, v, 0, true
	case protowire.VarintType:
		v, m := protowire.ConsumeVarint(b)
		if m <= 0 {
			return 0, nil, 0, false
		}
		return typ, nil, uint32(v), true
	default:
		return 0, nil, 0, false
	}
}

// --- tests ------------------------------------------------------------------

func TestUniCache_UpdateEncodeDecode_Flow_WithMyKV(t *testing.T) {
	var maxCommit uint64
	minV := uint64(1)
	minCache := func() uint64 { return minV }
	uc := NewUniCache(&maxCommit, minCache, 3)

	// Learn 3 keys (with values/proposalID)
	keys := [][]byte{[]byte("k1"), []byte("k2"), []byte("k3")}
	vals := [][]byte{[]byte("v1"), []byte("v2"), []byte("v3")}
	for i := range keys {
		data := myKVBytes(keys[i], vals[i], nil)
		e := pb.Entry{Index: uint64(i + 1), Data: data}
		if _, ok := uc.UpdateCache(e); !ok {
			t.Fatalf("UpdateCache failed at i=%d", i)
		}
	}

	// After learning 3 keys, nextId should be 4.
	if got := uc.GetNextId(); got != 4 {
		t.Fatalf("GetNextId = %d, want 4", got)
	}

	// Encode a known key (k2). Only field #1 should flip to varint id.
	rawFull := myKVBytes([]byte("k2"), []byte("v2"), nil)
	encoded, id := uc.EncodeData(rawFull)
	if id == 0 {
		t.Fatalf("EncodeData returned id=0 for cached key")
	}
	typ, _, gotID, ok := parseKeyField(encoded)
	if !ok || typ != protowire.VarintType || gotID != id {
		t.Fatalf("encoded key field not varint id; typ=%v ok=%v id=%d want=%d", typ, ok, gotID, id)
	}

	// Decode back to bytes using the cache; entire message should match original.
	out, ok := uc.DecodeEntry(pb.Entry{Index: 10, Data: encoded})
	if !ok {
		t.Fatalf("DecodeEntry failed to restore bytes for cached id=%d", id)
	}
	if !bytes.Equal(out.Data, rawFull) {
		t.Fatalf("DecodeEntry data mismatch; got=%x want=%x", out.Data, rawFull)
	}
}

func TestUniCache_SafeEncode_FromCacheAndEvicted_WithMyKV(t *testing.T) {
	var maxCommit uint64
	minV := uint64(1)
	minCache := func() uint64 { return minV }

	// Small capacity to force eviction. Capacity 3 means when we reach 3 we evict
	uc := NewUniCache(&maxCommit, minCache, 3)

	// Learn k1 (idx=1), k2 (idx=2)
	e1 := pb.Entry{Index: 1, Data: myKVBytes([]byte("k1"), []byte("v1"), nil)}
	e2 := pb.Entry{Index: 2, Data: myKVBytes([]byte("k2"), []byte("v2"), nil)}
	if _, ok := uc.UpdateCache(e1); !ok {
		t.Fatal("UpdateCache k1 failed")
	}
	if _, ok := uc.UpdateCache(e2); !ok {
		t.Fatal("UpdateCache k2 failed")
	}

	// Encode k1 while still in cache.
	raw1 := myKVBytes([]byte("k1"), []byte("v1"), nil)
	enc1, id1 := uc.EncodeData(raw1)
	if id1 == 0 {
		t.Fatal("EncodeData returned id=0 for k1")
	}

	// Add k3 (idx=3) -> should evict k1 to 'evicted' (minCacheVersion != 0).
	e3 := pb.Entry{Index: 3, Data: myKVBytes([]byte("k3"), []byte("v3"), nil)}
	if _, ok := uc.UpdateCache(e3); !ok {
		t.Fatal("UpdateCache k3 failed")
	}

	// Now DecodeEntry on enc1 should fail (id1 not in active cache).
	if _, ok := uc.DecodeEntry(pb.Entry{Index: 4, Data: enc1}); ok {
		t.Fatal("DecodeEntry unexpectedly succeeded for evicted id")
	}

	// SafeEncode should restore full bytes from 'evicted'.
	newData, full := uc.SafeEncode(enc1, 4, id1)
	if full == nil {
		t.Fatal("SafeEncode did not provide full restored data from evicted")
	}
	wantFull := raw1
	if !bytes.Equal(full, wantFull) || !bytes.Equal(newData, full) {
		t.Fatalf("SafeEncode evicted restore mismatch; new=%x full=%x want=%x", newData, full, wantFull)
	}
}

func TestUniCache_CacheHits_Reset_WithMyKV(t *testing.T) {
	var maxCommit uint64
	// Ensure minCacheVersion >= addedIdx to take the fast path that increments cachehits.
	minV := uint64(100)
	minCache := func() uint64 { return minV }

	uc := NewUniCache(&maxCommit, minCache, 10)

	// Learn key at idx=5.
	entry := pb.Entry{Index: 5, Data: myKVBytes([]byte("hot"), []byte("val"), nil)}
	if _, ok := uc.UpdateCache(entry); !ok {
		t.Fatal("UpdateCache failed")
	}

	// Encode while in cache.
	raw := myKVBytes([]byte("hot"), []byte("val"), nil)
	enc, id := uc.EncodeData(raw)
	if id == 0 {
		t.Fatal("EncodeData failed to produce id for cached key")
	}

	// SafeEncode should count a cache hit (still in active cache and within capacity window).
	before := uc.CacheHits()
	encodedAgain, full := uc.SafeEncode(enc /*appendIdx=*/, 5, id)
	if full == nil {
		t.Fatal("expected SafeEncode to return full data")
	}
	if !bytes.Equal(encodedAgain, enc) {
		t.Fatal("SafeEncode should keep encoded data as first return value on cache-hit path")
	}
	after := uc.CacheHits()
	if after != before+1 {
		t.Fatalf("CacheHits not incremented: before=%d after=%d", before, after)
	}

	// Reset should zero it.
	if got := uc.ResetCacheHits(); got != 0 {
		t.Fatalf("ResetCacheHits = %d, want 0", got)
	}
}

func TestUniCache_PurgeEvicted_RemovesOldestWhenWindowAllows_WithMyKV(t *testing.T) {
	var maxCommit uint64
	minV := uint64(1)
	minCache := func() uint64 { return minV }

	// Capacity 1 to build up a few evicted entries.
	ucI := NewUniCache(&maxCommit, minCache, 1)
	uc := ucI.(*uniCache) // same package: inspect internals

	// Add four distinct keys at indices 1..4; each new one evicts the previous.
	for i := 1; i <= 4; i++ {
		key := []byte{byte('a' + i)}
		val := []byte{byte('A' + i)}
		if _, ok := uc.UpdateCache(pb.Entry{Index: uint64(i), Data: myKVBytes(key, val, nil)}); !ok {
			t.Fatalf("UpdateCache failed at i=%d", i)
		}
	}
	if got := uc.evictOrder.Len(); got < 3 {
		t.Fatalf("expected >=3 evicted entries, got %d", got)
	}

	// With minCacheVersion = 3 and capacity = 1 => window = 2.
	// We should drop the entry with lastIdx=1 from 'evicted'.
	minV = 3
	uc.PurgeEvicted(0)

	if _, ok := uc.evicted[1]; ok {
		t.Fatalf("expected oldest evicted (id=1) to be purged")
	}
	if uc.evictOrder.Len() == 0 {
		t.Fatalf("expected some evicted entries to remain after purge")
	}
}

func TestUniCache_UpdateCache_VarintInputReturnsFalse_WithMyKV(t *testing.T) {
	var maxCommit uint64
	minV := uint64(1)
	minCache := func() uint64 { return minV }
	uc := NewUniCache(&maxCommit, minCache, 2)

	// Learn k1 as bytes first.
	raw := myKVBytes([]byte("k1"), []byte("v1"), nil)
	if _, ok := uc.UpdateCache(pb.Entry{Index: 1, Data: raw}); !ok {
		t.Fatal("UpdateCache k1 failed")
	}

	// Produce an encoded MyKV where key field #1 is a varint id.
	enc, _ := uc.EncodeData(raw)

	// Pass that to UpdateCache: function should return false on VARINT key.
	_, ok := uc.UpdateCache(pb.Entry{Index: 2, Data: enc})
	if ok {
		t.Fatalf("UpdateCache should return false for varint key field")
	}
}

func TestUniCache_EncodeData_UnknownKeyReturnsZero_WithMyKV(t *testing.T) {
	var maxCommit uint64
	minV := uint64(1)
	minCache := func() uint64 { return minV }
	uc := NewUniCache(&maxCommit, minCache, 2)

	// Unknown key (never learned).
	raw := myKVBytes([]byte("ghost"), []byte("v"), nil)
	enc, id := uc.EncodeData(raw)
	if id != 0 || !bytes.Equal(enc, raw) {
		t.Fatalf("EncodeData changed data or returned id for unknown key; id=%d", id)
	}
}

func TestUniCache_SafeEncode_Guards_WithMyKV(t *testing.T) {
	var maxCommit uint64
	minCache := func() uint64 { return 1 }
	uc := NewUniCache(&maxCommit, minCache, 2)

	// encodedID==0 => should return (data, nil)
	d := myKVBytes([]byte("any"), []byte("val"), nil)
	out, full := uc.SafeEncode(d, 1, 0)
	if !bytes.Equal(out, d) || full != nil {
		t.Fatalf("SafeEncode guard failed: out=%x full=%v", out, full)
	}
}
