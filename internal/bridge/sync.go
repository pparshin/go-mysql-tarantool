package bridge

import (
	"github.com/pkg/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

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

func (h *eventHandler) OnTableChanged(schema string, table string) error {
	err := h.bridge.updateRule(schema, table)
	if err != nil && err != ErrRuleNotExist {
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

	var reqs []request
	var err error
	switch e.Action {
	case canal.InsertAction:
		reqs, err = makeInsertRequests(rule, e.Rows)
	case canal.DeleteAction:
		reqs, err = makeDeleteRequests(rule, e.Rows)
	case canal.UpdateAction:
		reqs, err = makeUpdateRequests(rule, e.Rows)
	default:
		err = errors.Errorf("invalid rows action: %s", e.Action)
	}

	if err != nil {
		h.bridge.cancel()
		return errors.Errorf("sync %s request, what: %s", e.Action, err)
	}

	batch := batch{
		action: action(e.Action),
		reqs:   reqs,
	}

	h.bridge.syncCh <- batch

	return h.bridge.ctx.Err()
}

func (h *eventHandler) OnGTID(_ mysql.GTIDSet) error {
	return h.bridge.ctx.Err()
}

func (h *eventHandler) OnPosSynced(pos mysql.Position, set mysql.GTIDSet, force bool) error {
	if h.gtidMode {
		h.bridge.syncCh <- &savePos{
			pos:   newGTIDSet(set),
			force: force,
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

func makeUpdateOrCreateRequests(action action, r *rule, rows [][]interface{}) ([]request, error) {
	reqs := make([]request, 0, len(rows))

	for _, row := range rows {
		keys := make([]interface{}, 0, len(r.pks))
		for _, pk := range r.pks {
			value, err := r.tableInfo.GetColumnValue(pk, row)
			if err != nil {
				return nil, err
			}
			keys = append(keys, value)
		}

		args := make([]interface{}, 0, len(r.columns))
		for _, col := range r.columns {
			value, err := r.tableInfo.GetColumnValue(col, row)
			if err != nil {
				return nil, err
			}
			args = append(args, value)
		}

		req := request{
			action: action,
			space:  r.space,
			keys:   keys,
			args:   args,
		}

		reqs = append(reqs, req)
	}

	return reqs, nil
}

func makeInsertRequests(r *rule, rows [][]interface{}) ([]request, error) {
	return makeUpdateOrCreateRequests(actionInsert, r, rows)
}

func makeUpdateRequests(r *rule, rows [][]interface{}) ([]request, error) {
	return makeUpdateOrCreateRequests(actionUpdate, r, rows)
}

func makeDeleteRequests(r *rule, rows [][]interface{}) ([]request, error) {
	reqs := make([]request, 0, len(rows))

	for _, row := range rows {
		keys := make([]interface{}, 0, len(r.pks))
		for _, pk := range r.pks {
			value, err := r.tableInfo.GetColumnValue(pk, row)
			if err != nil {
				return nil, err
			}
			keys = append(keys, value)
		}

		req := request{
			action: actionDelete,
			space:  r.space,
			keys:   keys,
		}

		reqs = append(reqs, req)
	}

	return reqs, nil
}
