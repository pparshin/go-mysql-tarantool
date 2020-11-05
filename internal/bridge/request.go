package bridge

import "github.com/pkg/errors"

type action string

const (
	actionInsert action = "insert"
	actionUpdate action = "update"
	actionDelete action = "delete"
)

type reqArg struct {
	field uint64
	value interface{}
}

type request struct {
	action action
	space  string
	keys   []reqArg
	args   []reqArg
}

type batch struct {
	action action
	reqs   []*request
}

func makeInsertRequest(r *rule, row []interface{}) (*request, error) {
	keys := make([]reqArg, 0, len(r.pks))
	for _, pk := range r.pks {
		value, err := pk.fetchValue(row)
		if err != nil {
			return nil, err
		}
		keys = append(keys, reqArg{
			field: pk.tupIndex,
			value: value,
		})
	}

	args := make([]reqArg, 0, len(r.attrs))
	for _, attr := range r.attrs {
		value, err := attr.fetchValue(row)
		if err != nil {
			return nil, err
		}
		args = append(args, reqArg{
			field: attr.tupIndex,
			value: value,
		})
	}

	return &request{
		action: actionInsert,
		space:  r.space,
		keys:   keys,
		args:   args,
	}, nil
}

func makeInsertBatch(r *rule, rows [][]interface{}) ([]*request, error) {
	reqs := make([]*request, 0, len(rows))

	for _, row := range rows {
		req, err := makeInsertRequest(r, row)
		if err != nil {
			return nil, err
		}

		reqs = append(reqs, req)
	}

	return reqs, nil
}

func makeUpdateRequests(r *rule, rows [][]interface{}) ([]*request, error) {
	if len(rows)%2 != 0 {
		return nil, errors.Errorf("invalid update rows event, must have 2x rows, but %d", len(rows))
	}

	reqs := make([]*request, 0, len(rows))
	for i := 0; i < len(rows); i += 2 {
		before := rows[i]
		after := rows[i+1]

		// In Tarantool it is illegal to modify a primary-key field.
		// So we make two requests: delete and insert instead of update.
		isPKChanged := false
		for _, pk := range r.pks {
			pkBefore, err := pk.fetchValue(before)
			if err != nil {
				return nil, err
			}

			pkAfter, err := pk.fetchValue(after)
			if err != nil {
				return nil, err
			}

			if pkBefore != pkAfter {
				isPKChanged = true
				break
			}
		}

		if isPKChanged {
			reqDel, err := makeDeleteRequest(r, before)
			if err != nil {
				return nil, err
			}
			reqInsert, err := makeInsertRequest(r, after)
			if err != nil {
				return nil, err
			}

			reqs = append(reqs, reqDel, reqInsert)

			continue
		}

		// Normal flow: update non-primary fields.
		keys := make([]reqArg, 0, len(r.pks))
		args := make([]reqArg, 0, len(r.attrs))

		for _, pk := range r.pks {
			value, err := pk.fetchValue(before)
			if err != nil {
				return nil, err
			}

			keys = append(keys, reqArg{
				field: pk.tupIndex,
				value: value,
			})
		}

		for _, attr := range r.attrs {
			value, err := attr.fetchValue(after)
			if err != nil {
				return nil, err
			}
			args = append(args, reqArg{
				field: attr.tupIndex,
				value: value,
			})
		}

		req := &request{
			action: actionUpdate,
			space:  r.space,
			keys:   keys,
			args:   args,
		}

		reqs = append(reqs, req)
	}

	return reqs, nil
}

func makeDeleteRequest(r *rule, row []interface{}) (*request, error) {
	keys := make([]reqArg, 0, len(r.pks))
	for _, pk := range r.pks {
		value, err := pk.fetchValue(row)
		if err != nil {
			return nil, err
		}
		keys = append(keys, reqArg{
			field: pk.tupIndex,
			value: value,
		})
	}

	return &request{
		action: actionDelete,
		space:  r.space,
		keys:   keys,
	}, nil
}

func makeDeleteBatch(r *rule, rows [][]interface{}) ([]*request, error) {
	reqs := make([]*request, 0, len(rows))

	for _, row := range rows {
		keys := make([]reqArg, 0, len(r.pks))
		for _, pk := range r.pks {
			value, err := pk.fetchValue(row)
			if err != nil {
				return nil, err
			}
			keys = append(keys, reqArg{
				field: pk.tupIndex,
				value: value,
			})
		}

		req := &request{
			action: actionDelete,
			space:  r.space,
			keys:   keys,
		}

		reqs = append(reqs, req)
	}

	return reqs, nil
}
