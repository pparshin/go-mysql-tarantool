package bridge

import (
	"strings"

	"github.com/siddontang/go-mysql/schema"
)

type rule struct {
	schema string
	table  string
	pks    []*attribute // primary keys
	attrs  []*attribute // mapping attributes except primary keys

	space string

	tableInfo *schema.Table
}

func ruleKey(schema, table string) string {
	var sb strings.Builder
	sb.Grow(len(schema) + len(table) + 1)
	sb.WriteString(schema)
	sb.WriteByte(':')
	sb.WriteString(table)
	return sb.String()
}
