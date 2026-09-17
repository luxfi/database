// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package codec

// Manager defines the interface for codec management
type Manager interface {
	Marshal(version uint16, source any) ([]byte, error)
	Unmarshal(source []byte, destination any) (uint16, error)
	RegisterCodec(version uint16, codec Codec) error
	Size(version uint16, source any) (int, error)
}

// Codec defines the interface for encoding/decoding
type Codec interface {
	MarshalInto(source any, destination []byte) error
	Unmarshal(source []byte, destination any) error
	Size(value any) (int, error)
}
