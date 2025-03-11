package unicache

import (
	"errors"
	"fmt"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
)

// cachedFieldNumber is the protobuf field number that we want to cache.
// (Change this if your application uses a different field number.)
const cachedFieldNumber = 1

// UniCache is the interface that every Raft instance will implement.
type UniCache interface {
	// NewUniCache creates a new cache instance.
	NewUniCache() UniCache

	EncodeData(data []byte) []byte

	// EncodeEntry processes a Raft log entry: it looks for the cached field in the
	// entry’s Data and, if the field has been seen before, replaces its full value
	// with a small integer reference.
	EncodeEntry(entry pb.Entry) pb.Entry

	// DecodeEntry undoes the encoding: if the entry’s Data contains an integer
	// reference instead of a full key, it looks up the original bytes and restores them.
	DecodeEntry(entry pb.Entry) pb.Entry
}

// uniCache is a concrete implementation of the UniCache interface.
// It maintains two maps: one from int -> []byte and a reverse map from key (as string) -> int.
type uniCache struct {
	cache        map[int][]byte // id -> key bytes
	reverseCache map[string]int // key string -> id
	nextID       int            // next id to assign
}

// cloneEntry creates a deep copy of the pb.Entry.
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

// NewUniCache creates a new uniCache instance.
func NewUniCache() UniCache {
	return &uniCache{
		cache:        make(map[int][]byte),
		reverseCache: make(map[string]int),
		nextID:       1,
	}
}

// NewUniCache implements the UniCache interface.
func (uc *uniCache) NewUniCache() UniCache {
	return NewUniCache()
}

func (uc *uniCache) EncodeData(data []byte) []byte {
	if len(data) == 0 {
		return data
	}

	// 2) Extract the keyBytes
	keyBytes, _, err := GetProtoFieldAndWireType(data, cachedFieldNumber)
	if err != nil {
		return data
	}

	// 3) Check if key is cached
	if id, ok := uc.reverseCache[string(keyBytes)]; ok {
		// Already cached -> replace with varint
		encodedID := protowire.AppendVarint(nil, uint64(id))
		newData, err := ReplaceProtoField(keyBytes, cachedFieldNumber, encodedID, protowire.VarintType)
		if err != nil {
			return data
		}
		data = newData
	} else {
		// Cache miss -> store key
		newID := uc.nextID
		uc.nextID++
		uc.cache[newID] = keyBytes
		uc.reverseCache[string(keyBytes)] = newID
	}
	return data
}

// EncodeEntry looks for the cached field inside entry.Data.
// If the key is already known, it replaces the field value with a varint id.
// Otherwise it adds the key to the cache and leaves the entry unchanged.
func (uc *uniCache) EncodeEntry(entry pb.Entry) pb.Entry {
	// Try to extract the key field from the entry data.
	keyBytes, _, err := GetProtoFieldAndWireType(entry.Data, cachedFieldNumber)
	if err != nil {
		// Field not found – nothing to cache.
		return entry
	}

	// Check if the key is already known.
	if id, ok := uc.reverseCache[string(keyBytes)]; ok {
		// Create a new varint value representing the id.
		newValue := protowire.AppendVarint(nil, uint64(id))
		// Replace the field (with field number cachedFieldNumber) with the id (and wire type Varint).
		newData, err := ReplaceProtoFieldInPlaceCompress(entry.Data, cachedFieldNumber, newValue, protowire.VarintType)
		if err != nil {
			// In case of error (e.g. parsing problem), return the entry unchanged.
			return entry
		}
		entry.Data = newData
	} else {
		// Key not in cache: assign a new id and add it.
		id := uc.nextID
		uc.nextID++
		uc.cache[id] = keyBytes
		uc.reverseCache[string(keyBytes)] = id
		// Leave entry.Data unchanged.
	}
	return entry
}

// DecodeEntry reverses the encoding: if the target field is a varint (i.e. an id reference)
// then it looks up the original key bytes in the cache and replaces the field with a bytes value.
func (uc *uniCache) DecodeEntry(entry pb.Entry) pb.Entry {
	// Scan the entry data for the target field.
	data := entry.Data
	var foundWireType protowire.Type
	var fieldValue []byte
	var found bool
	for len(data) > 0 {
		fieldNum, wireType, n := protowire.ConsumeTag(data)
		if n < 0 {
			break
		}
		data = data[n:]
		if int(fieldNum) == cachedFieldNumber {
			switch wireType {
			case protowire.VarintType:
				v, n := protowire.ConsumeVarint(data)
				if n < 0 {
					break
				}
				fieldValue = protowire.AppendVarint(nil, v)
				foundWireType = wireType
				found = true
			case protowire.BytesType:
				v, n := protowire.ConsumeBytes(data)
				if n < 0 {
					break
				}
				fieldValue = v
				foundWireType = wireType
				found = true
			default:
				// For our purposes we expect either Varint (the encoded id)
				// or Bytes (the full key).
			}
			break
		} else {
			// Skip non-target fields.
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
			}
			if skip < 0 {
				break
			}
			data = data[skip:]
		}
	}
	if !found {
		// Nothing to do.
		return entry
	}

	// If the field is encoded as varint, it is an id reference.
	if foundWireType == protowire.VarintType {
		// Decode the id.
		id, _ := protowire.ConsumeVarint(fieldValue)
		// Look up the key bytes in the cache.
		keyBytes, ok := uc.cache[int(id)]
		if !ok {
			// If the cache is missing the key, return the entry as is.
			return entry
		}
		// Replace the field with the original key bytes, encoded as a bytes field.
		newValue := protowire.AppendBytes(nil, keyBytes)
		newData, err := ReplaceProtoField(entry.Data, cachedFieldNumber, newValue, protowire.BytesType)
		if err != nil {
			return entry
		}
		entry.Data = newData
	} else if foundWireType == protowire.BytesType {
		// This is a full key. To help future decoding, add it to the cache if it isn’t already there.
		if _, ok := uc.reverseCache[string(fieldValue)]; !ok {
			id := uc.nextID
			uc.nextID++
			uc.cache[id] = fieldValue
			uc.reverseCache[string(fieldValue)] = id
		}
	}
	return entry
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
		// Get the original tag bytes.
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
			// Re-encode the consumed bytes with a length prefix.
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
			newTag := protowire.AppendTag(nil, protowire.Number(targetField), newWireType)
			out = append(out, newTag...)
			out = append(out, newValue...)
		} else {
			// Otherwise, keep the original tag and re-encoded value.
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
