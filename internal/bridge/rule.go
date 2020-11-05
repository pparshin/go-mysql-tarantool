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

func ruleKey(db, table string) string {
	var sb strings.Builder
	sb.Grow(len(db) + len(table) + 1)
	sb.WriteString(db)
	sb.WriteByte(':')
	sb.WriteString(table)

	return sb.String()
}
