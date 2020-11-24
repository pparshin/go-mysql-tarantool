package bridge

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_attribute_fetchValue(t *testing.T) {
	type fields struct {
		colIndex uint64
		tupIndex uint64
		name     string
		vType    attrType
		cType    castType
		unsigned bool
	}
	type args struct {
		row []interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{
		{
			name: "String",
			fields: fields{
				colIndex: 0,
				tupIndex: 0,
				name:     "name",
				vType:    typeString,
				cType:    castNone,
				unsigned: false,
			},
			args: args{
				row: []interface{}{"alice"},
			},
			want:    "alice",
			wantErr: false,
		},
		{
			name: "SignedNumber",
			fields: fields{
				colIndex: 0,
				tupIndex: 0,
				name:     "speed",
				vType:    typeNumber,
				cType:    castNone,
				unsigned: false,
			},
			args: args{
				row: []interface{}{-20},
			},
			want:    -20,
			wantErr: false,
		},
		{
			name: "Number_ForceCastToUnsigned",
			fields: fields{
				colIndex: 0,
				tupIndex: 0,
				name:     "speed",
				vType:    typeNumber,
				cType:    castUnsigned,
				unsigned: false,
			},
			args: args{
				row: []interface{}{20},
			},
			want:    uint64(20),
			wantErr: false,
		},
		{
			name: "Float_ForceCastToUnsigned_Error",
			fields: fields{
				colIndex: 0,
				tupIndex: 0,
				name:     "speed",
				vType:    typeFloat,
				cType:    castUnsigned,
				unsigned: false,
			},
			args: args{
				row: []interface{}{4654.123},
			},
			want:    0,
			wantErr: true,
		},
		{
			name: "UnsignedMediumInt",
			fields: fields{
				colIndex: 0,
				tupIndex: 0,
				name:     "id",
				vType:    typeMediumInt,
				cType:    castNone,
				unsigned: true,
			},
			args: args{
				row: []interface{}{10},
			},
			want:    uint64(10),
			wantErr: false,
		},
		{
			name: "UnsignedNumber",
			fields: fields{
				colIndex: 0,
				tupIndex: 0,
				name:     "id",
				vType:    typeNumber,
				cType:    castNone,
				unsigned: true,
			},
			args: args{
				row: []interface{}{10},
			},
			want:    uint64(10),
			wantErr: false,
		},
		{
			name: "ColumnIndexEqualRowLen",
			fields: fields{
				colIndex: 1,
				tupIndex: 1,
				name:     "name",
				vType:    typeString,
				cType:    castNone,
				unsigned: false,
			},
			args: args{
				row: []interface{}{"alice"},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "ColumnIndexGTRowLen",
			fields: fields{
				colIndex: 5,
				tupIndex: 5,
				name:     "name",
				vType:    typeString,
				cType:    castNone,
				unsigned: false,
			},
			args: args{
				row: []interface{}{1, "alice"},
			},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			a := &attribute{
				colIndex: tt.fields.colIndex,
				tupIndex: tt.fields.tupIndex,
				name:     tt.fields.name,
				vType:    tt.fields.vType,
				cType:    tt.fields.cType,
				unsigned: tt.fields.unsigned,
			}
			got, err := a.fetchValue(tt.args.row)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, got)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func Test_toUint64(t *testing.T) {
	tests := []struct {
		name    string
		arg     interface{}
		want    uint64
		wantErr bool
	}{
		{
			name:    "int",
			arg:     10,
			want:    10,
			wantErr: false,
		},
		{
			name:    "int8",
			arg:     int8(10),
			want:    10,
			wantErr: false,
		},
		{
			name:    "int16",
			arg:     int16(10),
			want:    10,
			wantErr: false,
		},
		{
			name:    "int32",
			arg:     int32(10),
			want:    10,
			wantErr: false,
		},
		{
			name:    "int64",
			arg:     int64(10),
			want:    10,
			wantErr: false,
		},
		{
			name:    "uint",
			arg:     uint(10),
			want:    10,
			wantErr: false,
		},
		{
			name:    "uint8",
			arg:     uint8(10),
			want:    10,
			wantErr: false,
		},
		{
			name:    "uint16",
			arg:     uint16(10),
			want:    10,
			wantErr: false,
		},
		{
			name:    "uint32",
			arg:     uint32(10),
			want:    10,
			wantErr: false,
		},
		{
			name:    "uint64",
			arg:     uint64(10),
			want:    10,
			wantErr: false,
		},
		{
			name:    "string",
			arg:     "10",
			want:    0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := toUint64(tt.arg)
			if tt.wantErr {
				assert.Error(t, err)
				assert.EqualValues(t, 0, got)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}
