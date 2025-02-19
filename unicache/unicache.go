package unicache

import (
	"errors"
	"fmt"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
)

// cachedFieldNumber is the protobuf field number that we want to cache.
// (Change this if your application uses a different field number.)
const cachedFieldNumber = 2

// UniCache is the interface that every Raft instance will implement.
type UniCache interface {
	// NewUniCache creates a new cache instance.
	NewUniCache() UniCache

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

// EncodeEntry looks into the PutRequest (field 4) of entry.Data,
// then into its key (field 1). If that key has been seen before, it replaces
// the key with a varint–encoded id; otherwise, it adds the key to the cache.
func (uc *uniCache) EncodeEntry(entry pb.Entry) pb.Entry {
	if len(entry.Data) == 0 {
		return entry
	}
	// Get the PutRequest field from the parent message (InternalRaftRequest)
	rawPutBytes, _, err := GetProtoFieldAndWireType(entry.Data, 4)
	if err != nil {
		// Put field not found – nothing to cache.
		return entry
	}

	// Extract the key from the nested PutRequest (field 1)
	keyBytes, _, err := GetProtoFieldAndWireType(rawPutBytes, cachedFieldNumber)
	if err != nil {
		// Key field not found; nothing to do.
		return entry
	}

	// Check if this key is already cached.
	if id, ok := uc.reverseCache[string(keyBytes)]; ok {
		// Already cached: create a varint encoding of the id.
		encodedID := protowire.AppendVarint(nil, uint64(id))
		// Replace the key field (field 1) in the nested PutRequest with the encoded id.
		newRawPutBytes, err := ReplaceProtoField(rawPutBytes, cachedFieldNumber, encodedID, protowire.VarintType)
		if err != nil {
			return entry
		}
		// Replace the PutRequest field (field 4) in the overall entry with the updated nested bytes.
		newData, err := ReplaceProtoField(entry.Data, 4, newRawPutBytes, protowire.BytesType)
		if err != nil {
			return entry
		}
		entry.Data = newData
		//		fmt.Println("cache hit! new entry.Data:", entry.Data)
	} else {
		// Cache miss: add the key to the cache.
		newID := uc.nextID
		uc.nextID++
		uc.cache[newID] = keyBytes
		uc.reverseCache[string(keyBytes)] = newID
		//		fmt.Println("cache miss for key:", keyBytes)
		// Optionally, you could also choose to encode it right away.
	}

	return entry
}

func (uc *uniCache) DecodeEntry(entry pb.Entry) pb.Entry {
	if len(entry.Data) == 0 {
		return entry
	}
	// Get the nested PutRequest field.
	rawPutBytes, _, err := GetProtoFieldAndWireType(entry.Data, 4)
	if err != nil {
		return entry
	}
	// Get the key field from the nested PutRequest along with its wire type.
	keyField, wireType, err := GetProtoFieldAndWireType(rawPutBytes, cachedFieldNumber)
	if err != nil {
		return entry
	}
	// If the field is encoded as BytesType, it’s already the full key.
	if wireType == protowire.BytesType {
		if _, ok := uc.reverseCache[string(keyField)]; !ok {
			newID := uc.nextID
			uc.nextID++
			uc.cache[newID] = keyField
			uc.reverseCache[string(keyField)] = newID
			//			fmt.Println("DecodeEntry - cache miss; adding key:", keyField)
		}
		return entry
	} else if wireType == protowire.VarintType {
		// It is encoded as a varint: decode the id.
		id, n := protowire.ConsumeVarint(keyField)
		if n <= 0 {
			// Should not happen since wire type is Varint.
			return entry
		}
		// Look up the original key from the cache.
		origKey, ok := uc.cache[int(id)]
		if !ok {
			fmt.Println("DecodeEntry - id not found in cache:", id)
			return entry
		}
		// Replace the key field in the nested PutRequest with the original key bytes.
		newRawPutBytes, err := ReplaceProtoField(rawPutBytes, cachedFieldNumber, origKey, protowire.BytesType)
		if err != nil {
			fmt.Println("DecodeEntry - error replacing key field:", err)
			return entry
		}
		// Replace the PutRequest field in the overall entry.
		newData, err := ReplaceProtoField(entry.Data, 4, newRawPutBytes, protowire.BytesType)
		if err != nil {
			fmt.Println("DecodeEntry - error replacing nested PutRequest field:", err)
			return entry
		}
		entry.Data = newData
		return entry
	} else {
		// For any other wire type, return unchanged.
		return entry
	}
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
