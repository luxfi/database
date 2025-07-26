// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package linear

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"

	"github.com/luxfi/db/codec"
)

var (
	ErrUnknownVersion    = errors.New("unknown codec version")
	ErrMarshalNil        = errors.New("can't marshal nil")
	ErrUnmarshalNil      = errors.New("can't unmarshal nil")
	ErrUnexpectedType    = errors.New("unexpected type")
	ErrDoesNotImplement  = errors.New("type does not implement interface")
	ErrUnexportedField   = errors.New("unexported field")
	ErrMaxSliceLenExceeded = errors.New("max slice length exceeded")
)

const MaxInt = math.MaxInt32

// Manager implements codec.Manager
type Manager struct {
	codecs map[uint16]codec.Codec
}

// NewManager creates a new codec manager
func NewManager(maxSize int) codec.Manager {
	return &Manager{
		codecs: make(map[uint16]codec.Codec),
	}
}

// NewDefault creates a new linear codec
func NewDefault() codec.Codec {
	return &LinearCodec{}
}

// RegisterCodec registers a codec for a version
func (m *Manager) RegisterCodec(version uint16, codec codec.Codec) error {
	if _, exists := m.codecs[version]; exists {
		return fmt.Errorf("codec version %d already registered", version)
	}
	m.codecs[version] = codec
	return nil
}

// Marshal encodes the source object
func (m *Manager) Marshal(version uint16, source interface{}) ([]byte, error) {
	codec, exists := m.codecs[version]
	if !exists {
		return nil, fmt.Errorf("%w: %d", ErrUnknownVersion, version)
	}

	size, err := codec.Size(source)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, size+2) // +2 for version
	binary.BigEndian.PutUint16(buf, version)
	
	if err := codec.MarshalInto(source, buf[2:]); err != nil {
		return nil, err
	}
	
	return buf, nil
}

// Unmarshal decodes into the destination object
func (m *Manager) Unmarshal(source []byte, destination interface{}) (uint16, error) {
	if len(source) < 2 {
		return 0, errors.New("insufficient bytes for codec version")
	}

	version := binary.BigEndian.Uint16(source)
	codec, exists := m.codecs[version]
	if !exists {
		return 0, fmt.Errorf("%w: %d", ErrUnknownVersion, version)
	}

	if err := codec.Unmarshal(source[2:], destination); err != nil {
		return version, err
	}

	return version, nil
}

// Size returns the encoded size of the object
func (m *Manager) Size(version uint16, source interface{}) (int, error) {
	codec, exists := m.codecs[version]
	if !exists {
		return 0, fmt.Errorf("%w: %d", ErrUnknownVersion, version)
	}
	
	size, err := codec.Size(source)
	if err != nil {
		return 0, err
	}
	
	return size + 2, nil // +2 for version
}

// LinearCodec is a simple codec implementation
type LinearCodec struct{}

// MarshalInto marshals the source into the destination buffer
func (c *LinearCodec) MarshalInto(source interface{}, destination []byte) error {
	return marshal(reflect.ValueOf(source), destination)
}

// Unmarshal unmarshals from source into destination
func (c *LinearCodec) Unmarshal(source []byte, destination interface{}) error {
	destPtr := reflect.ValueOf(destination)
	if destPtr.Kind() != reflect.Ptr {
		return fmt.Errorf("%w: expected pointer, got %T", ErrUnexpectedType, destination)
	}
	
	_, err := unmarshal(source, destPtr.Elem())
	return err
}

// Size returns the encoded size of the value
func (c *LinearCodec) Size(value interface{}) (int, error) {
	return calcSize(reflect.ValueOf(value))
}

// marshal encodes a value
func marshal(v reflect.Value, dest []byte) error {
	switch v.Kind() {
	case reflect.Bool:
		if v.Bool() {
			dest[0] = 1
		} else {
			dest[0] = 0
		}
		return nil
		
	case reflect.Uint8:
		dest[0] = byte(v.Uint())
		return nil
		
	case reflect.Uint16:
		binary.BigEndian.PutUint16(dest, uint16(v.Uint()))
		return nil
		
	case reflect.Uint32:
		binary.BigEndian.PutUint32(dest, uint32(v.Uint()))
		return nil
		
	case reflect.Uint64:
		binary.BigEndian.PutUint64(dest, v.Uint())
		return nil
		
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			// []byte - encode length then bytes
			binary.BigEndian.PutUint32(dest, uint32(v.Len()))
			copy(dest[4:], v.Bytes())
			return nil
		}
		return fmt.Errorf("%w: unsupported slice type %v", ErrUnexpectedType, v.Type())
		
	case reflect.Struct:
		offset := 0
		for i := 0; i < v.NumField(); i++ {
			field := v.Field(i)
			fieldType := v.Type().Field(i)
			
			// Skip unexported fields
			if !fieldType.IsExported() {
				continue
			}
			
			// Check for serialize tag
			tag := fieldType.Tag.Get("serialize")
			if tag == "-" || tag == "false" {
				continue
			}
			
			size, err := calcSize(field)
			if err != nil {
				return err
			}
			
			if err := marshal(field, dest[offset:]); err != nil {
				return err
			}
			
			offset += size
		}
		return nil
		
	default:
		return fmt.Errorf("%w: cannot marshal %v", ErrUnexpectedType, v.Kind())
	}
}

// unmarshal decodes a value
func unmarshal(src []byte, v reflect.Value) (int, error) {
	switch v.Kind() {
	case reflect.Bool:
		if len(src) < 1 {
			return 0, errors.New("insufficient bytes for bool")
		}
		v.SetBool(src[0] != 0)
		return 1, nil
		
	case reflect.Uint8:
		if len(src) < 1 {
			return 0, errors.New("insufficient bytes for uint8")
		}
		v.SetUint(uint64(src[0]))
		return 1, nil
		
	case reflect.Uint16:
		if len(src) < 2 {
			return 0, errors.New("insufficient bytes for uint16")
		}
		v.SetUint(uint64(binary.BigEndian.Uint16(src)))
		return 2, nil
		
	case reflect.Uint32:
		if len(src) < 4 {
			return 0, errors.New("insufficient bytes for uint32")
		}
		v.SetUint(uint64(binary.BigEndian.Uint32(src)))
		return 4, nil
		
	case reflect.Uint64:
		if len(src) < 8 {
			return 0, errors.New("insufficient bytes for uint64")
		}
		v.SetUint(binary.BigEndian.Uint64(src))
		return 8, nil
		
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			// []byte - decode length then bytes
			if len(src) < 4 {
				return 0, errors.New("insufficient bytes for slice length")
			}
			length := binary.BigEndian.Uint32(src)
			if length > uint32(MaxInt) {
				return 0, ErrMaxSliceLenExceeded
			}
			if len(src) < 4+int(length) {
				return 0, errors.New("insufficient bytes for slice data")
			}
			v.SetBytes(src[4 : 4+length])
			return 4 + int(length), nil
		}
		return 0, fmt.Errorf("%w: unsupported slice type %v", ErrUnexpectedType, v.Type())
		
	case reflect.Struct:
		offset := 0
		for i := 0; i < v.NumField(); i++ {
			field := v.Field(i)
			fieldType := v.Type().Field(i)
			
			// Skip unexported fields
			if !fieldType.IsExported() {
				continue
			}
			
			// Check for serialize tag
			tag := fieldType.Tag.Get("serialize")
			if tag == "-" || tag == "false" {
				continue
			}
			
			n, err := unmarshal(src[offset:], field)
			if err != nil {
				return offset, err
			}
			
			offset += n
		}
		return offset, nil
		
	default:
		return 0, fmt.Errorf("%w: cannot unmarshal %v", ErrUnexpectedType, v.Kind())
	}
}

// calcSize calculates the encoded size of a value
func calcSize(v reflect.Value) (int, error) {
	switch v.Kind() {
	case reflect.Bool, reflect.Uint8:
		return 1, nil
	case reflect.Uint16:
		return 2, nil
	case reflect.Uint32:
		return 4, nil
	case reflect.Uint64:
		return 8, nil
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return 4 + v.Len(), nil // 4 bytes for length + data
		}
		return 0, fmt.Errorf("%w: unsupported slice type %v", ErrUnexpectedType, v.Type())
	case reflect.Struct:
		size := 0
		for i := 0; i < v.NumField(); i++ {
			fieldType := v.Type().Field(i)
			
			// Skip unexported fields
			if !fieldType.IsExported() {
				continue
			}
			
			// Check for serialize tag
			tag := fieldType.Tag.Get("serialize")
			if tag == "-" || tag == "false" {
				continue
			}
			
			fieldSize, err := calcSize(v.Field(i))
			if err != nil {
				return 0, err
			}
			size += fieldSize
		}
		return size, nil
	default:
		return 0, fmt.Errorf("%w: cannot calculate size of %v", ErrUnexpectedType, v.Kind())
	}
}