package unicache

import (
	"container/list"
	"errors"
	"fmt"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
)

// cachedFieldNumber is the protobuf field number that we want to cache.
const cachedFieldNumber = 1

// maxCacheSize defines the maximum number of entries in the cache.
const maxCacheSize = 1000

// UniCache is the interface that every Raft instance will implement.
type UniCache interface {
	NewUniCache() UniCache
	EncodeData(data []byte) []byte
	EncodeEntry(entry pb.Entry) pb.Entry
	DecodeEntry(entry pb.Entry) pb.Entry
}

// cacheEntry is used to store cache information for LRU eviction.
type cacheEntry struct {
	id  int
	key []byte
}

// uniCache is a concrete implementation of the UniCache interface.
type uniCache struct {
	cache        map[int][]byte        // id -> key bytes
	reverseCache map[string]int        // key string -> id
	lruList      *list.List            // Doubly linked list to track LRU order.
	lruMap       map[int]*list.Element // mapping from id to list element
	nextID       int                   // next id to assign
	capacity     int                   // maximum number of cache entries
}

// NewUniCache creates a new uniCache instance.
func NewUniCache() UniCache {
	return &uniCache{
		cache:        make(map[int][]byte),
		reverseCache: make(map[string]int),
		lruList:      list.New(),
		lruMap:       make(map[int]*list.Element),
		nextID:       1,
		capacity:     maxCacheSize,
	}
}

// NewUniCache implements the UniCache interface.
func (uc *uniCache) NewUniCache() UniCache {
	return NewUniCache()
}

// updateLRU moves the element for the given id to the front of the LRU list.
func (uc *uniCache) updateLRU(id int) {
	if elem, ok := uc.lruMap[id]; ok {
		uc.lruList.MoveToFront(elem)
	}
}

// addToLRU adds a new cache entry to the LRU list.
func (uc *uniCache) addToLRU(id int, key []byte) {
	entry := cacheEntry{id: id, key: key}
	elem := uc.lruList.PushFront(entry)
	uc.lruMap[id] = elem
	// Evict if we exceed capacity.
	if uc.lruList.Len() > uc.capacity {
		uc.evictLRU()
	}
}

// evictLRU removes the least recently used item from the cache.
func (uc *uniCache) evictLRU() {
	elem := uc.lruList.Back()
	if elem == nil {
		return
	}
	entry := elem.Value.(cacheEntry)
	// Remove from all maps.
	delete(uc.cache, entry.id)
	delete(uc.reverseCache, string(entry.key))
	delete(uc.lruMap, entry.id)
	uc.lruList.Remove(elem)
}

// EncodeData and EncodeEntry update the cache and record access in the LRU list.
func (uc *uniCache) EncodeData(data []byte) []byte {
	if len(data) == 0 {
		return data
	}
	// 1) Extract rawPutBytes.
	rawPutBytes, _, err := GetProtoFieldAndWireType(data, 4)
	if err != nil {
		return data
	}
	// 2) Extract the keyBytes.
	keyBytes, _, err := GetProtoFieldAndWireType(rawPutBytes, cachedFieldNumber)
	if err != nil {
		return data
	}
	keyStr := string(keyBytes)
	// 3) Check if key is cached.
	if id, ok := uc.reverseCache[keyStr]; ok {
		// Update LRU status.
		uc.updateLRU(id)
		encodedID := protowire.AppendVarint(nil, uint64(id))
		newRawPutBytes, err := ReplaceProtoField(rawPutBytes, cachedFieldNumber, encodedID, protowire.VarintType)
		if err != nil {
			return data
		}
		newData, err := ReplaceProtoField(data, 4, newRawPutBytes, protowire.BytesType)
		if err != nil {
			return data
		}
		return newData
	} else {
		// Cache miss: add the key.
		newID := uc.nextID
		uc.nextID++
		uc.cache[newID] = keyBytes
		uc.reverseCache[keyStr] = newID
		uc.addToLRU(newID, keyBytes)
	}
	return data
}

// EncodeEntry looks into the PutRequest (field 4) of entry.Data,
// then into its key (field 1). If that key has been seen before, it replaces
// the key with a varint–encoded id; otherwise, it adds the key to the cache.
func (uc *uniCache) EncodeEntry(entry pb.Entry) pb.Entry {
	if len(entry.Data) == 0 {
		return entry
	}
	rawPutBytes, _, err := GetProtoFieldAndWireType(entry.Data, 4)
	if err != nil {
		return entry
	}
	keyBytes, _, err := GetProtoFieldAndWireType(rawPutBytes, cachedFieldNumber)
	if err != nil {
		return entry
	}
	keyStr := string(keyBytes)
	if id, ok := uc.reverseCache[keyStr]; ok {
		// Update LRU status.
		uc.updateLRU(id)
		encodedID := protowire.AppendVarint(nil, uint64(id))
		newRawPutBytes, err := ReplaceProtoFieldInPlaceCompress(rawPutBytes, cachedFieldNumber, encodedID, protowire.VarintType)
		if err != nil {
			return entry
		}
		newData, err := ReplaceProtoFieldInPlaceCompress(entry.Data, 4, newRawPutBytes, protowire.BytesType)
		if err != nil {
			return entry
		}
		entry.Data = newData
	} else {
		newID := uc.nextID
		uc.nextID++
		uc.cache[newID] = keyBytes
		uc.reverseCache[keyStr] = newID
		uc.addToLRU(newID, keyBytes)
	}
	return entry
}

func (uc *uniCache) DecodeEntry(entry pb.Entry) pb.Entry {
	if len(entry.Data) == 0 {
		return entry
	}
	rawPutBytes, _, err := GetProtoFieldAndWireType(entry.Data, 4)
	if err != nil {
		return entry
	}
	keyField, wireType, err := GetProtoFieldAndWireType(rawPutBytes, cachedFieldNumber)
	if err != nil {
		return entry
	}
	if wireType == protowire.BytesType {
		keyStr := string(keyField)
		if id, ok := uc.reverseCache[keyStr]; !ok {
			newID := uc.nextID
			uc.nextID++
			uc.cache[newID] = keyField
			uc.reverseCache[keyStr] = newID
			uc.addToLRU(newID, keyField)
		} else {
			uc.updateLRU(id)
		}
		return entry
	} else if wireType == protowire.VarintType {
		id, n := protowire.ConsumeVarint(keyField)
		if n <= 0 {
			return entry
		}
		origKey, ok := uc.cache[int(id)]
		if !ok {
			fmt.Println("DecodeEntry - id not found in cache:", id)
			return entry
		}
		uc.updateLRU(int(id))
		newRawPutBytes, err := ReplaceProtoField(rawPutBytes, cachedFieldNumber, origKey, protowire.BytesType)
		if err != nil {
			fmt.Println("DecodeEntry - error replacing key field:", err)
			return entry
		}
		newData, err := ReplaceProtoField(entry.Data, 4, newRawPutBytes, protowire.BytesType)
		if err != nil {
			fmt.Println("DecodeEntry - error replacing nested PutRequest field:", err)
			return entry
		}
		entry.Data = newData
		return entry
	} else {
		return entry
	}
}

// ReplaceProtoField, ReplaceProtoFieldInPlaceCompress, and GetProtoFieldAndWireType
// remain unchanged and are used for protobuf field manipulation.

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
			// For non-replaced fields, we want to keep the full encoding (tag + length + value)
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
			// Keep the field unchanged.
			out = append(out, originalTag...)
			out = append(out, fieldBytes...)
		}
		data = data[skip:]
	}
	return out, nil
}

// ReplaceProtoFieldInPlaceCompress replaces occurrences of the target field in the
// protobuf message contained in data, handling only the compressing case (new encoding is shorter).
// For BytesType fields, it correctly inserts the length prefix.
func ReplaceProtoFieldInPlaceCompress(data []byte, targetField int, newValue []byte, newWireType protowire.Type) ([]byte, error) {
	type fieldInfo struct {
		start    int  // start index of the field in the original slice
		end      int  // end index (exclusive)
		isTarget bool // whether this field is the one to replace
		newLen   int  // the length of the field after replacement
	}
	var fields []fieldInfo
	i := 0
	// First pass: record each field's boundaries and compute new lengths.
	for i < len(data) {
		start := i
		// Consume the tag.
		fieldNum, wireType, n := protowire.ConsumeTag(data[i:])
		if n < 0 {
			return nil, errors.New("failed to consume tag")
		}
		i += n

		var skip int
		switch wireType {
		case protowire.VarintType:
			_, m := protowire.ConsumeVarint(data[i:])
			if m < 0 {
				return nil, errors.New("failed to consume varint")
			}
			skip = m
		case protowire.Fixed32Type:
			_, m := protowire.ConsumeFixed32(data[i:])
			if m < 0 {
				return nil, errors.New("failed to consume fixed32")
			}
			skip = m
		case protowire.Fixed64Type:
			_, m := protowire.ConsumeFixed64(data[i:])
			if m < 0 {
				return nil, errors.New("failed to consume fixed64")
			}
			skip = m
		case protowire.BytesType:
			_, m := protowire.ConsumeBytes(data[i:])
			if m < 0 {
				return nil, errors.New("failed to consume bytes")
			}
			skip = m
		case protowire.StartGroupType:
			_, m := protowire.ConsumeGroup(fieldNum, data[i:])
			if m < 0 {
				return nil, errors.New("failed to consume group")
			}
			skip = m
		default:
			return nil, fmt.Errorf("unknown wire type: %v", wireType)
		}
		i += skip

		origFieldLen := i - start
		isTarget := int(fieldNum) == targetField
		newFieldLen := origFieldLen
		if isTarget {
			// Build the new tag.
			newTag := protowire.AppendTag(nil, protowire.Number(targetField), newWireType)
			// For BytesType fields, the proper encoding uses protowire.AppendBytes,
			// which adds a length prefix. For other types, we use newValue directly.
			var newFieldBytes []byte
			if newWireType == protowire.BytesType {
				newFieldBytes = protowire.AppendBytes(nil, newValue)
			} else {
				newFieldBytes = newValue
			}
			newFieldLen = len(newTag) + len(newFieldBytes)
			// We expect newFieldLen to be <= origFieldLen.
			if newFieldLen > origFieldLen {
				return nil, fmt.Errorf("new field encoding is larger than original; expected compressing")
			}
		}
		fields = append(fields, fieldInfo{start: start, end: i, isTarget: isTarget, newLen: newFieldLen})
	}

	// Calculate the total new length.
	newTotalLen := 0
	for _, f := range fields {
		newTotalLen += f.newLen
	}
	// In a compressing scenario, newTotalLen is guaranteed to be <= len(data).

	// Second pass: Copy fields backwards to avoid overwriting data that hasn't been moved.
	writePos := newTotalLen
	for j := len(fields) - 1; j >= 0; j-- {
		f := fields[j]
		writePos -= f.newLen
		if f.isTarget {
			newTag := protowire.AppendTag(nil, protowire.Number(targetField), newWireType)
			var newFieldBytes []byte
			if newWireType == protowire.BytesType {
				newFieldBytes = protowire.AppendBytes(nil, newValue)
			} else {
				newFieldBytes = newValue
			}
			copy(data[writePos:], newTag)
			copy(data[writePos+len(newTag):], newFieldBytes)
		} else {
			copy(data[writePos:], data[f.start:f.end])
		}
	}

	// Return the slice re-sliced to the new length.
	return data[:newTotalLen], nil
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
			// Skip this field.
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
