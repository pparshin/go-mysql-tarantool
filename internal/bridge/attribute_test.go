package bridge

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
