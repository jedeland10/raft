package unicache_test

import (
	"bytes"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/jedeland10/raft/unicache"
)

// CloneEntry makes a deep copy of a pb.Entry.
func CloneEntry(ent pb.Entry) pb.Entry {
	newData := make([]byte, len(ent.Data))
	copy(newData, ent.Data)
	return pb.Entry{
		Term:  ent.Term,
		Index: ent.Index,
		Type:  ent.Type,
		Data:  newData,
	}
}

// printCacheState logs the internal state of the UniCache instance.
func printCacheState(prefix string, uc unicache.UniCache, t *testing.T) {
	t.Logf("%s UniCache state: %+v", prefix, uc)
}

// TestEncodeDecodeComplexMessageConsistent tests encoding/decoding using the message:
//
//	[10 8 47 109 121 45 107 101 121 50 18 5 102 111 111 100 100]
//
// where
//   - Field 1 (tag 0x0a) contains "/my-key2"
//   - Field 2 (tag 0x12) contains "foodd"
//
// Since cachedFieldNumber is set to 2, UniCache caches field 2.
func TestEncodeDecodeComplexMessageConsistent(t *testing.T) {
	// Provided message bytes:
	// Field 1: tag 0x0a (field 1, wire type 2), length 8, value "/my-key2"
	// Field 2: tag 0x12 (field 2, wire type 2), length 5, value "foodd"
	originalData := []byte{
		10, 8, 47, 109, 121, 45, 107, 101, 121, 50,
		18, 5, 102, 111, 111, 100, 100,
	}

	// Expected values.
	expectedNonCacheValue := []byte("/my-key2")
	expectedCacheValue := []byte("foodd") // This is the value that will be cached from field 2.

	// Construct the entry.
	entry := pb.Entry{
		Term:  1,
		Index: 1,
		Type:  pb.EntryNormal,
		Data:  originalData,
	}

	t.Logf("Original entry data: %x", entry.Data)

	// Create a new UniCache instance.
	uc := unicache.NewUniCache()
	printCacheState("Before any encoding", uc, t)

	// --- First encoding (cache miss) ---
	// Since field 2 is not yet cached, the call should add its value ("foodd") to the cache.
	encoded1 := uc.EncodeEntry(CloneEntry(entry))
	t.Logf("After first encoding (cache miss), entry data: %x", encoded1.Data)
	printCacheState("After first encoding", uc, t)

	// Verify that field 2 still contains the full value.
	cacheField1, err := unicache.GetProtoField(encoded1.Data, 2)
	if err != nil {
		t.Fatalf("First encoding: failed to extract field 2: %v", err)
	}
	if !bytes.Equal(cacheField1, expectedCacheValue) {
		t.Errorf("First encoding: expected field 2 %q, got %q", expectedCacheValue, cacheField1)
	}
	// Field 1 should remain unchanged.
	nonCacheField1, err := unicache.GetProtoField(encoded1.Data, 1)
	if err != nil {
		t.Fatalf("First encoding: failed to extract field 1: %v", err)
	}
	if !bytes.Equal(nonCacheField1, expectedNonCacheValue) {
		t.Errorf("First encoding: expected field 1 %q, got %q", expectedNonCacheValue, nonCacheField1)
	}

	// --- Second encoding (cache hit) ---
	// Now that field 2 is cached, a second call should detect a cache hit
	// and replace field 2's value with a varint-encoded cache ID.
	encoded2 := uc.EncodeEntry(CloneEntry(encoded1))
	t.Logf("After second encoding (cache hit), entry data: %x", encoded2.Data)
	printCacheState("After second encoding", uc, t)

	// --- Decoding ---
	// Now decode the entry so that the varint-encoded field 2 is replaced
	// with its full bytes value.
	decoded := uc.DecodeEntry(CloneEntry(encoded2))
	t.Logf("After decoding, entry data: %x", decoded.Data)
	printCacheState("After decoding", uc, t)

	// Verify that field 2 is restored.
	decodedCacheField, err := unicache.GetProtoField(decoded.Data, 2)
	if err != nil {
		t.Fatalf("Decoding: failed to extract field 2: %v", err)
	}
	if !bytes.Equal(decodedCacheField, expectedCacheValue) {
		t.Errorf("Decoding: expected field 2 %q, got %q", expectedCacheValue, decodedCacheField)
	}
	// Field 1 should remain unchanged.
	decodedNonCacheField, err := unicache.GetProtoField(decoded.Data, 1)
	if err != nil {
		t.Fatalf("Decoding: failed to extract field 1: %v", err)
	}
	if !bytes.Equal(decodedNonCacheField, expectedNonCacheValue) {
		t.Errorf("Decoding: expected field 1 %q, got %q", expectedNonCacheValue, decodedNonCacheField)
	}
}
