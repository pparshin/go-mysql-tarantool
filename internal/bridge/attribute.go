package bridge

import (
	"errors"
	"fmt"

	"github.com/siddontang/go-mysql/schema"
)

// attribute represents MySQL column mapped to Tarantool.
type attribute struct {
	colIndex uint64 // column sequence number in MySQL table
	tupIndex uint64 // attribute sequence number in Tarantool tuple
	name     string // unique attribute name
	unsigned bool   // whether attribute contains unsigned number or not
}

func newAttr(table *schema.Table, tupIndex uint64, name string) (*attribute, error) {
	idx := table.FindColumn(name)
	if idx == -1 {
		return nil, fmt.Errorf("column not found, name: schema: %s, table: %s, name: %s", table.Schema, table.Name, name)
	}

	col := table.Columns[idx]

	return &attribute{
		colIndex: uint64(idx),
		tupIndex: tupIndex,
		name:     col.Name,
		unsigned: col.IsUnsigned,
	}, nil
}

func newAttrsFromPKs(table *schema.Table) []*attribute {
	pks := make([]*attribute, 0)
	for i, pki := range table.PKColumns {
		col := table.GetPKColumn(pki)
		pks = append(pks, &attribute{
			colIndex: uint64(pki),
			tupIndex: uint64(i),
			name:     col.Name,
			unsigned: col.IsUnsigned,
		})
	}
	return pks
}

func (a *attribute) fetchValue(row []interface{}) (interface{}, error) {
	if a.colIndex >= uint64(len(row)) {
		return nil, fmt.Errorf("column index (%d) equals or greater than row length (%d)", a.colIndex, len(row))
	}

	value := row[a.colIndex]
	if a.unsigned {
		return toUint64(value)
	}

	return value, nil
}

func toUint64(i interface{}) (uint64, error) {
	switch i := i.(type) {
	case int:
		return uint64(i), nil
	case int8:
		return uint64(i), nil
	case int16:
		return uint64(i), nil
	case int32:
		return uint64(i), nil
	case int64:
		return uint64(i), nil
	case uint:
		return uint64(i), nil
	case uint8:
		return uint64(i), nil
	case uint16:
		return uint64(i), nil
	case uint32:
		return uint64(i), nil
	case uint64:
		return i, nil
	}

	return 0, errors.New("could not cast to uint64")
}
