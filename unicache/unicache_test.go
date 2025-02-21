package unicache_test

import (
	"bytes"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"

	"go.etcd.io/raft/v3/unicache"
)

// Dummy data and parameters for benchmarking.
var (
	// rawPutBytes is the nested message containing the key field.
	rawPutBytes = []byte{10, 15, 109, 121, 107, 101, 121, 121, 121, 121, 121, 121, 121, 50, 50, 50, 50, 18, 15, 116, 104, 105, 115, 32, 105, 115, 32, 97, 119, 101, 115, 111, 109, 101}
	// entryData is the overall message containing the nested PutRequest.
	entryData = []byte{34, 34, 10, 15, 109, 121, 107, 101, 121, 121, 121, 121, 121, 121, 121, 50, 50, 50, 50, 18, 15, 116, 104, 105, 115, 32, 105, 115, 32, 97, 119, 101, 115, 111, 109, 101, 162, 6, 10, 8, 134, 128, 233, 212, 182, 164, 229, 180, 50}

	cachedFieldNumber = 1
	targetFieldNumber = 4
	// Let's assume the cached id is 1, so its varint encoding is just one byte.
	encodedID = protowire.AppendVarint(nil, uint64(1))
	// For our purposes, newWireType for key field is Varint and for nested PutRequest field is Bytes.
)

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
	encoded1 := uc.EncodeEntry(unicache.CloneEntry(entry))
	t.Logf("After first encoding (cache miss), entry data: %x", encoded1.Data)
	printCacheState("After first encoding", uc, t)

	// Verify that field 2 still contains the full value.
	cacheField1, _, err := unicache.GetProtoFieldAndWireType(encoded1.Data, 2)
	if err != nil {
		t.Fatalf("First encoding: failed to extract field 2: %v", err)
	}
	if !bytes.Equal(cacheField1, expectedCacheValue) {
		t.Errorf("First encoding: expected field 2 %q, got %q", expectedCacheValue, cacheField1)
	}
	// Field 1 should remain unchanged.
	nonCacheField1, _, err := unicache.GetProtoFieldAndWireType(encoded1.Data, 1)
	if err != nil {
		t.Fatalf("First encoding: failed to extract field 1: %v", err)
	}
	if !bytes.Equal(nonCacheField1, expectedNonCacheValue) {
		t.Errorf("First encoding: expected field 1 %q, got %q", expectedNonCacheValue, nonCacheField1)
	}

	// --- Second encoding (cache hit) ---
	// Now that field 2 is cached, a second call should detect a cache hit
	// and replace field 2's value with a varint-encoded cache ID.
	encoded2 := uc.EncodeEntry(unicache.CloneEntry(encoded1))
	t.Logf("After second encoding (cache hit), entry data: %x", encoded2.Data)
	printCacheState("After second encoding", uc, t)

	// --- Decoding ---
	// Now decode the entry so that the varint-encoded field 2 is replaced
	// with its full bytes value.
	decoded := uc.DecodeEntry(unicache.CloneEntry(encoded2))
	t.Logf("After decoding, entry data: %x", decoded.Data)
	printCacheState("After decoding", uc, t)

	// Verify that field 2 is restored.
	decodedCacheField, _, err := unicache.GetProtoFieldAndWireType(decoded.Data, 2)
	if err != nil {
		t.Fatalf("Decoding: failed to extract field 2: %v", err)
	}
	if !bytes.Equal(decodedCacheField, expectedCacheValue) {
		t.Errorf("Decoding: expected field 2 %q, got %q", expectedCacheValue, decodedCacheField)
	}
	// Field 1 should remain unchanged.
	decodedNonCacheField, _, err := unicache.GetProtoFieldAndWireType(decoded.Data, 1)
	if err != nil {
		t.Fatalf("Decoding: failed to extract field 1: %v", err)
	}
	if !bytes.Equal(decodedNonCacheField, expectedNonCacheValue) {
		t.Errorf("Decoding: expected field 1 %q, got %q", expectedNonCacheValue, decodedNonCacheField)
	}
}

// Benchmark using new slice method.
func BenchmarkReplaceProtoFieldNewSlice(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// Work on a copy of rawPutBytes to simulate independent calls.
		nestedMsg := make([]byte, len(rawPutBytes))
		copy(nestedMsg, rawPutBytes)

		// Replace the key field (field 1) in the nested PutRequest.
		newNested, err := unicache.ReplaceProtoField(nestedMsg, cachedFieldNumber, encodedID, protowire.VarintType)
		if err != nil {
			b.Fatal(err)
		}

		// Work on a copy of entryData.
		overallMsg := make([]byte, len(entryData))
		copy(overallMsg, entryData)
		// Replace the PutRequest field (field 4) in the overall entry with the updated nested bytes.
		_, err = unicache.ReplaceProtoField(overallMsg, targetFieldNumber, newNested, protowire.BytesType)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark using in-place compression method.
func BenchmarkReplaceProtoFieldInPlaceCompress(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// Work on a copy of rawPutBytes.
		nestedMsg := make([]byte, len(rawPutBytes))
		copy(nestedMsg, rawPutBytes)
		newNested, err := unicache.ReplaceProtoFieldInPlaceCompress(nestedMsg, cachedFieldNumber, encodedID, protowire.VarintType)
		if err != nil {
			b.Fatal(err)
		}

		// Work on a copy of entryData.
		overallMsg := make([]byte, len(entryData))
		copy(overallMsg, entryData)
		_, err = unicache.ReplaceProtoFieldInPlaceCompress(overallMsg, targetFieldNumber, newNested, protowire.BytesType)
		if err != nil {
			b.Fatal(err)
		}
	}
}
