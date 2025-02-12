package unicache

import (
	"errors"
	"fmt"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
)

type Encoded interface{}

// UnencodedField represents a field that could not be encoded.
type NotEncoded interface{}

// EncodableField represents a field that can be encoded.
type Encodable interface{}

// UnencodableField represents a field that cannot be encoded.
type NotEncodable interface{}

// / The UniCache interface. Implement this trait for your own UniCache implementation.
type UniCache interface {
	// Create a new UniCache instance.
	NewUniCache() UniCache

	// Process a Raft log entry: encodes applicable fields and updates the cache.
	EncodeEntry(entry pb.Entry) pb.Entry

	// Decode an entry, replacing encoded fields with their original values.
	DecodeEntry(entry pb.Entry) pb.Entry
}

// MaybeEncoded represents a value that can be either encoded or not.
type MaybeEncoded[EncodableType Encodable, EncodedType Encoded] interface {
	IsEncoded() bool
	Get() interface{}
}

// FieldCache is responsible for encoding and decoding fields.
type FieldCache[EncodableType Encodable, EncodedType Encoded] interface {
	// Create a new FieldCache instance with a given size.
	NewFieldCache(size int) FieldCache[EncodableType, EncodedType]

	// Try to encode a field by checking if it exists in the cache.
	TryEncode(field EncodableType) MaybeEncoded[EncodableType, EncodedType]

	// Decode an encoded field back to its original form.
	Decode(encoded EncodedType) (EncodableType, bool)

	// Store a new encoded field in the cache.
	Put(field EncodableType, encoded EncodedType)

	// Check if a field exists in the cache.
	Has(field EncodableType) bool
}

// GetProtoField scans the provided protobuf-encoded data looking for the
// first occurrence of the field with number targetField. It returns the raw
// value bytes (which you may then decode according to the expected type)
// or an error if the field isn’t found or there is a parsing error.
func GetProtoField(data []byte, targetField int) ([]byte, error) {
	// Iterate over the entire byte slice.
	for len(data) > 0 {
		// Consume the tag to obtain the field number and wire type.
		fieldNum, wireType, n := protowire.ConsumeTag(data)
		if n < 0 {
			return nil, errors.New("failed to consume tag")
		}
		// Advance the data past the tag.
		data = data[n:]

		// Check if this is the field we’re looking for.
		if int(fieldNum) == targetField {
			switch wireType {
			case protowire.VarintType:
				// Consume a varint value.
				v, n := protowire.ConsumeVarint(data)
				if n < 0 {
					return nil, errors.New("failed to consume varint")
				}
				// Re-encode the varint into its raw bytes.
				return protowire.AppendVarint(nil, v), nil

			case protowire.Fixed32Type:
				// Consume a 32-bit fixed value.
				v, n := protowire.ConsumeFixed32(data)
				if n < 0 {
					return nil, errors.New("failed to consume fixed32")
				}
				return protowire.AppendFixed32(nil, v), nil

			case protowire.Fixed64Type:
				// Consume a 64-bit fixed value.
				v, n := protowire.ConsumeFixed64(data)
				if n < 0 {
					return nil, errors.New("failed to consume fixed64")
				}
				return protowire.AppendFixed64(nil, v), nil

			case protowire.BytesType:
				// Consume a length-delimited (bytes) value.
				v, n := protowire.ConsumeBytes(data)
				if n < 0 {
					return nil, errors.New("failed to consume bytes")
				}
				return v, nil

			case protowire.StartGroupType:
				// Groups are deprecated in proto3, but if you use them,
				// ConsumeGroup will grab the entire group.
				v, n := protowire.ConsumeGroup(fieldNum, data)
				if n < 0 {
					return nil, errors.New("failed to consume group")
				}
				return v, nil

			default:
				return nil, fmt.Errorf("unknown wire type: %v", wireType)
			}
		} else {
			// Not our target field; skip over this field.
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
				return nil, fmt.Errorf("unknown wire type: %v", wireType)
			}
			if skip < 0 {
				return nil, errors.New("failed to skip field")
			}
			data = data[skip:]
		}
	}
	return nil, fmt.Errorf("field number %d not found", targetField)
}
