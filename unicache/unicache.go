package unicache

import (
	"container/list"
	"errors"
	"fmt"

	"github.com/cespare/xxhash/v2"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
)

const cachedFieldNumber = 1
const maxCacheSize = 10000 // cache capacity

// UniCache defines methods for encoding/decoding entries with key caching.
type UniCache interface {
	NewUniCache() UniCache
	EncodeData(data []byte, nextId *uint32) []byte
	DecodeEntry(entry pb.Entry) (pb.Entry, bool)
	GetNextId() uint32
}

type cacheEntry struct {
	id  uint32
	key []byte
}

// uniCache uses xxhash for fingerprinting and container/list for LRU tracking.
type uniCache struct {
	// fingerprint -> cache ID
	fp2id map[uint64]uint32
	// cache ID -> original key bytes
	cache map[uint32][]byte
	// LRU eviction tracking
	lruList *list.List
	lruMap  map[uint32]*list.Element
	// next cache ID
	nextID uint32
	// maximum entries to cache
	capacity int
}

// NewUniCache constructs a UniCache with hashing-based lookup and LRU eviction.
func NewUniCache() UniCache {
	return &uniCache{
		fp2id:    make(map[uint64]uint32, maxCacheSize),
		cache:    make(map[uint32][]byte, maxCacheSize),
		lruList:  list.New(),
		lruMap:   make(map[uint32]*list.Element, maxCacheSize),
		nextID:   1,
		capacity: maxCacheSize,
	}
}

func (uc *uniCache) NewUniCache() UniCache {
	return NewUniCache()
}

func (uc *uniCache) GetNextId() uint32 {
	return uc.nextID
}

// EncodeData replaces cached key bytes with varint IDs or records new keys.
func (uc *uniCache) EncodeData(data []byte, nextId *uint32) []byte {
	if len(data) == 0 {
		return data
	}
	// Extract key bytes
	keyBytes, _, err := GetProtoFieldAndWireType(data, cachedFieldNumber)
	if err != nil {
		return data
	}
	// Fingerprint
	h := xxhash.Sum64(keyBytes)
	id, hit := uc.fp2id[h]
	if hit && id < *nextId {
		uc.updateLRU(id)
		// Build varint encoding
		encodedID := protowire.AppendVarint(nil, uint64(id))
		newData, err := ReplaceProtoField(data, cachedFieldNumber, encodedID, protowire.VarintType)
		if err == nil {
			return newData
		}
	}
	// Cache miss: assign new ID, record, and LRU track
	id = *nextId
	uc.fp2id[h] = id
	uc.cache[id] = keyBytes
	uc.addToLRU(id, keyBytes)
	*nextId++
	return data
}

// DecodeEntry restores original key bytes for varint IDs, or caches new raw keys.
func (uc *uniCache) DecodeEntry(entry pb.Entry) (pb.Entry, bool) {
	data := entry.Data
	if len(data) == 0 {
		return entry, true
	}
	fieldVal, wireType, err := GetProtoFieldAndWireType(data, cachedFieldNumber)
	if err != nil {
		return entry, true
	}
	switch wireType {
	case protowire.VarintType:
		// decode ID
		id64, n := protowire.ConsumeVarint(fieldVal)
		if n <= 0 {
			return entry, false
		}
		id := uint32(id64)
		orig, ok := uc.cache[id]
		if !ok {
			return entry, false
		}
		uc.updateLRU(id)
		newData, err := ReplaceProtoField(data, cachedFieldNumber, orig, protowire.BytesType)
		if err == nil {
			entry.Data = newData
		}
		return entry, true

	case protowire.BytesType:
		// first-seen raw key
		keyBytes := fieldVal
		h := xxhash.Sum64(keyBytes)
		if _, exists := uc.fp2id[h]; !exists {
			id := uc.nextID
			uc.nextID++
			uc.fp2id[h] = id
			uc.cache[id] = keyBytes
			uc.addToLRU(id, keyBytes)
		}
		return entry, true

	default:
		return entry, true
	}
}

// LRU helpers
func (uc *uniCache) updateLRU(id uint32) {
	if elem, ok := uc.lruMap[id]; ok {
		uc.lruList.MoveToFront(elem)
	}
}
func (uc *uniCache) addToLRU(id uint32, key []byte) {
	node := uc.lruList.PushFront(cacheEntry{id: id, key: key})
	uc.lruMap[id] = node
	if uc.lruList.Len() > uc.capacity {
		uc.evictLRU()
	}
}
func (uc *uniCache) evictLRU() {
	elem := uc.lruList.Back()
	if elem == nil {
		return
	}
	entry := elem.Value.(cacheEntry)
	delete(uc.cache, entry.id)
	// remove fingerprint mapping
	for fp, cid := range uc.fp2id {
		if cid == entry.id {
			delete(uc.fp2id, fp)
			break
		}
	}
	delete(uc.lruMap, entry.id)
	uc.lruList.Remove(elem)
}

// ReplaceProtoField is a helper that scans a protobuf-encoded message in data,
// and whenever it finds a field with number targetField it replaces that field’s value
// with newValue and uses newWireType. (It leaves all other fields unchanged.)
func ReplaceProtoField(data []byte, targetField int, newValue []byte, newWireType protowire.Type) ([]byte, error) {
	var out []byte
	for len(data) > 0 {
		fieldNum, wireType, n := protowire.ConsumeTag(data)
		if n < 0 {
			return nil, errors.New("failed to consume tag")
		}
		originalTag := protowire.AppendTag(nil, fieldNum, wireType)
		data = data[n:]
		var fieldBytes []byte
		var skip int
		switch wireType {
		case protowire.VarintType:
			v, m := protowire.ConsumeVarint(data)
			if m < 0 {
				return nil, errors.New("failed to consume varint")
			}
			fieldBytes = protowire.AppendVarint(nil, v)
			skip = m
		case protowire.Fixed32Type:
			v, m := protowire.ConsumeFixed32(data)
			if m < 0 {
				return nil, errors.New("failed to consume fixed32")
			}
			fieldBytes = protowire.AppendFixed32(nil, v)
			skip = m
		case protowire.Fixed64Type:
			v, m := protowire.ConsumeFixed64(data)
			if m < 0 {
				return nil, errors.New("failed to consume fixed64")
			}
			fieldBytes = protowire.AppendFixed64(nil, v)
			skip = m
		case protowire.BytesType:
			v, m := protowire.ConsumeBytes(data)
			if m < 0 {
				return nil, errors.New("failed to consume bytes")
			}
			fieldBytes = protowire.AppendBytes(nil, v)
			skip = m
		case protowire.StartGroupType:
			v, m := protowire.ConsumeGroup(fieldNum, data)
			if m < 0 {
				return nil, errors.New("failed to consume group")
			}
			fieldBytes = v
			skip = m
		default:
			return nil, fmt.Errorf("unknown wire type: %v", wireType)
		}

		if int(fieldNum) == targetField {
			// Build the new field.
			var encodedNewValue []byte
			if newWireType == protowire.BytesType {
				encodedNewValue = protowire.AppendBytes(nil, newValue)
			} else {
				encodedNewValue = newValue
			}
			newTag := protowire.AppendTag(nil, protowire.Number(targetField), newWireType)
			out = append(out, newTag...)
			out = append(out, encodedNewValue...)
		} else {
			out = append(out, originalTag...)
			out = append(out, fieldBytes...)
		}
		data = data[skip:]
	}
	return out, nil
}

// GetProtoFieldAndWireType scans the provided protobuf-encoded data looking for the first
// occurrence of the field with number targetField. It returns the raw value bytes, the field’s wire type,
// or an error if the field isn’t found.
func GetProtoFieldAndWireType(data []byte, targetField int) ([]byte, protowire.Type, error) {
	for len(data) > 0 {
		fieldNum, wireType, n := protowire.ConsumeTag(data)
		if n < 0 {
			return nil, 0, errors.New("failed to consume tag")
		}
		data = data[n:]
		if int(fieldNum) == targetField {
			switch wireType {
			case protowire.VarintType:
				v, n := protowire.ConsumeVarint(data)
				if n < 0 {
					return nil, 0, errors.New("failed to consume varint")
				}
				return protowire.AppendVarint(nil, v), wireType, nil
			case protowire.BytesType:
				v, n := protowire.ConsumeBytes(data)
				if n < 0 {
					return nil, 0, errors.New("failed to consume bytes")
				}
				return v, wireType, nil
			case protowire.Fixed32Type:
				v, n := protowire.ConsumeFixed32(data)
				if n < 0 {
					return nil, 0, errors.New("failed to consume fixed32")
				}
				return protowire.AppendFixed32(nil, v), wireType, nil
			case protowire.Fixed64Type:
				v, n := protowire.ConsumeFixed64(data)
				if n < 0 {
					return nil, 0, errors.New("failed to consume fixed64")
				}
				return protowire.AppendFixed64(nil, v), wireType, nil
			case protowire.StartGroupType:
				v, n := protowire.ConsumeGroup(fieldNum, data)
				if n < 0 {
					return nil, 0, errors.New("failed to consume group")
				}
				return v, wireType, nil
			default:
				return nil, 0, fmt.Errorf("unknown wire type: %v", wireType)
			}
		} else {
			var skip int
			switch wireType {
			case protowire.VarintType:
				_, skip = protowire.ConsumeVarint(data)
			case protowire.Fixed32Type:
				_, skip = protowire.ConsumeFixed32(data)
			case protowire.Fixed64Type:
				_, skip = protowire.ConsumeFixed64(data)
			case protowire.BytesType:
				_, skip = protowire.ConsumeBytes(data)
			case protowire.StartGroupType:
				_, skip = protowire.ConsumeGroup(fieldNum, data)
			default:
				return nil, 0, fmt.Errorf("unknown wire type: %v", wireType)
			}
			if skip < 0 {
				return nil, 0, errors.New("failed to skip field")
			}
			data = data[skip:]
		}
	}
	return nil, 0, fmt.Errorf("field number %d not found", targetField)
}
