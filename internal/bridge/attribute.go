package bridge

import (
	"fmt"

	"github.com/siddontang/go-mysql/schema"
)

type attrType int

//nolint
const (
	typeNumber    attrType = iota + 1 // tinyint, smallint, int, bigint, year
	typeFloat                         // float, double
	typeEnum                          // enum
	typeSet                           // set
	typeString                        // char, varchar, etc.
	typeDatetime                      // datetime
	typeTimestamp                     // timestamp
	typeDate                          // date
	typeTime                          // time
	typeBit                           // bit
	typeJSON                          // json
	typeDecimal                       // decimal
	typeMediumInt                     // medium int
	typeBinary                        // binary, varbinary
	typePoint                         // coordinates
)

type castType int

const (
	castNone     castType = iota // do not cast
	castUnsigned                 // unsigned
)

func castTypeFromString(str string) castType {
	switch str {
	case "unsigned":
		return castUnsigned
	default:
		return castNone
	}
}

// attribute represents MySQL column mapped to Tarantool.
type attribute struct {
	colIndex uint64   // column sequence number in MySQL table
	tupIndex uint64   // attribute sequence number in Tarantool tuple
	name     string   // unique attribute name
	vType    attrType // value type stored in the column
	cType    castType // value must be casted to this type
	unsigned bool     // whether attribute contains unsigned number or not
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
		vType:    attrType(col.Type),
		cType:    castNone,
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
			vType:    attrType(col.Type),
			cType:    castNone,
			unsigned: col.IsUnsigned,
		})
	}

	return pks
}

func (a *attribute) castTo(t castType) {
	a.cType = t
}

func (a *attribute) fetchValue(row []interface{}) (interface{}, error) {
	if a.colIndex >= uint64(len(row)) {
		return nil, fmt.Errorf("column index (%d) equals or greater than row length (%d)", a.colIndex, len(row))
	}

	value := row[a.colIndex]

	if a.shouldCastToUInt64(value) {
		v, err := toUint64(value)
		if err != nil {
			return nil, err
		}

		return v, nil
	}

	return value, nil
}

func (a *attribute) shouldCastToUInt64(value interface{}) bool {
	if value == nil {
		return false
	}

	if a.cType == castUnsigned {
		return true
	}

	if !a.unsigned {
		return false
	}

	return a.vType == typeNumber || a.vType == typeMediumInt
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

	return 0, fmt.Errorf("could not cast %T to uint64: %v", i, i)
}
