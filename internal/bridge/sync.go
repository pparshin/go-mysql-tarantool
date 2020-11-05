package bridge

import (
	"errors"
	"fmt"

	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

var emptyGTID = mustCreateGTID(mysql.MySQLFlavor, "")

func mustCreateGTID(flavor, s string) mysql.GTIDSet {
	set, err := mysql.ParseGTIDSet(flavor, s)
	if err != nil {
		panic(err)
	}

	return set
}

type eventHandler struct {
	bridge   *Bridge
	gtidMode bool
}

func newEventHandler(b *Bridge, gtidMode bool) *eventHandler {
	return &eventHandler{
		bridge:   b,
		gtidMode: gtidMode,
	}
}

func (h *eventHandler) OnRotate(_ *replication.RotateEvent) error {
	return h.bridge.ctx.Err()
}

func (h *eventHandler) OnTableChanged(schema, table string) error {
	err := h.bridge.updateRule(schema, table)
	if err != nil && !errors.Is(err, ErrRuleNotExist) {
		return err
	}

	return nil
}

func (h *eventHandler) OnDDL(_ mysql.Position, _ *replication.QueryEvent) error {
	return h.bridge.ctx.Err()
}

func (h *eventHandler) OnXID(_ mysql.Position) error {
	return h.bridge.ctx.Err()
}

func (h *eventHandler) OnRow(e *canal.RowsEvent) error {
	rule, ok := h.bridge.rules[ruleKey(e.Table.Schema, e.Table.Name)]
	if !ok {
		return nil
	}

	var reqs []*request
	var err error
	switch e.Action {
	case canal.InsertAction:
		reqs, err = makeInsertBatch(rule, e.Rows)
	case canal.DeleteAction:
		reqs, err = makeDeleteBatch(rule, e.Rows)
	case canal.UpdateAction:
		reqs, err = makeUpdateRequests(rule, e.Rows)
	default:
		err = fmt.Errorf("invalid rows action: %s", e.Action)
	}

	if err != nil {
		h.bridge.cancel()

		return fmt.Errorf("sync %s request, what: %w", e.Action, err)
	}

	batch := &batch{
		action: action(e.Action),
		reqs:   reqs,
	}

	h.bridge.syncCh <- batch

	return h.bridge.ctx.Err()
}

func (h *eventHandler) OnGTID(set mysql.GTIDSet) error {
	if h.gtidMode {
		h.bridge.syncCh <- &savePos{
			pos:   newGTIDSet(set),
			force: false,
		}
	}

	return h.bridge.ctx.Err()
}

func (h *eventHandler) OnPosSynced(pos mysql.Position, set mysql.GTIDSet, force bool) error {
	if h.gtidMode {
		if force && !emptyGTID.Equal(set) {
			h.bridge.syncCh <- &savePos{
				pos:   newGTIDSet(set),
				force: force,
			}
		}
	} else {
		h.bridge.syncCh <- &savePos{
			pos:   newBinlogPos(pos),
			force: force,
		}
	}

	return h.bridge.ctx.Err()
}

func (h *eventHandler) String() string {
	return "TarantoolBridgeEventHandler"
}
