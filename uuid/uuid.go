// Package with support of Tarantool's UUID data type.
//
// UUID data type supported in Tarantool since 2.4.1.
//
// Since: 1.6.0.
//
// # See also
//
// * Tarantool commit with UUID support https://github.com/tarantool/tarantool/commit/d68fc29246714eee505bc9bbcd84a02de17972c5
//
// * Tarantool data model https://www.tarantool.io/en/doc/latest/book/box/data_model/
//
// * Module UUID https://www.tarantool.io/en/doc/latest/reference/reference_lua/uuid/
package uuid

import (
	"fmt"
	"reflect"

	"github.com/google/uuid"
)

// UUID external type.
const UUID_extId = 2

func encodeUUID(e *encoder, v reflect.Value) error {
	id := v.Interface().(uuid.UUID)

	bytes, err := id.MarshalBinary()
	if err != nil {
		return fmt.Errorf("msgpack: can't marshal binary uuid: %w", err)
	}

	_, err = e.Writer().Write(bytes)
	if err != nil {
		return fmt.Errorf("msgpack: can't write bytes to encoder writer: %w", err)
	}

	return nil
}

func decodeUUID(d *decoder, v reflect.Value) error {
	var bytesCount int = 16
	bytes := make([]byte, bytesCount)

	n, err := d.Buffered().Read(bytes)
	if err != nil {
		return fmt.Errorf("msgpack: can't read bytes on uuid decode: %w", err)
	}
	if n < bytesCount {
		return fmt.Errorf("msgpack: unexpected end of stream after %d uuid bytes", n)
	}

	id, err := uuid.FromBytes(bytes)
	if err != nil {
		return fmt.Errorf("msgpack: can't create uuid from bytes: %w", err)
	}

	v.Set(reflect.ValueOf(id))
	return nil
}
