package unicache_test

import (
	"fmt"
	"reflect"
	"testing"
	"unsafe"

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

// makeEntry creates a pb.Entry with a nested PutRequest message (field 4) containing a key (field 1).
// The key is provided as a byte slice.
func makeEntry(key []byte) pb.Entry {
	// Build the nested PutRequest: field 1 (key) with BytesType.
	rawPut := protowire.AppendTag(nil, 1, protowire.BytesType)
	rawPut = protowire.AppendBytes(rawPut, key)
	// Build the outer message: field 4 (PutRequest) with BytesType.
	data := protowire.AppendTag(nil, 4, protowire.BytesType)
	data = protowire.AppendBytes(data, rawPut)
	return pb.Entry{
		Data: data,
	}
}

func TestCacheEviction(t *testing.T) {
	// Create a new UniCache.
	cache := unicache.NewUniCache()

	// Use reflection with unsafe to modify the unexported capacity field.
	ucVal := reflect.ValueOf(cache).Elem()
	capField := ucVal.FieldByName("capacity")
	if !capField.IsValid() {
		t.Fatal("capacity field not found")
	}
	// Create a writable version of the unexported field.
	writableCapField := reflect.NewAt(capField.Type(), unsafe.Pointer(capField.UnsafeAddr())).Elem()
	writableCapField.SetInt(10)

	// Add 15 entries with unique keys to force eviction.
	totalEntries := 15
	for i := 0; i < totalEntries; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		entry := makeEntry(key)

		entryCopy := pb.Entry{
			Term:  entry.Term,
			Index: entry.Index,
			Type:  entry.Type,
		}
		// The EncodeEntry method updates the cache's internal state.
		entryCopy.Data = cache.EncodeData(entry.Data)
	}

	// Check that the internal cache map size is equal to the capacity (i.e., eviction occurred).
	cacheMap := ucVal.FieldByName("cache")
	if !cacheMap.IsValid() {
		t.Fatal("cache map field not found")
	}

	if cacheMap.Len() != 10 {
		t.Errorf("expected cache map length to be 10, got %d", cacheMap.Len())
	} else {
		t.Logf("TestCacheEviction: cache map length is %d as expected", cacheMap.Len())
	}
}
