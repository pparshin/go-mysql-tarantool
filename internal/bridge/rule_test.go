package bridge

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_ruleKey(t *testing.T) {
	tests := []struct {
		schema string
		table  string
		want   string
	}{
		{
			schema: "city",
			table:  "users",
			want:   "city:users",
		},
	}

	for _, tt := range tests {
		got := ruleKey(tt.schema, tt.table)
		assert.Equal(t, tt.want, got)
	}
}
