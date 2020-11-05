package bridge

import tnt "github.com/viciious/go-tarantool"

func makeInsertQuery(req *request) tnt.Query {
	if req.action != actionInsert {
		return nil
	}

	tuple := make([]interface{}, 0, len(req.keys)+len(req.args))
	for _, key := range req.keys {
		tuple = append(tuple, key.value)
	}
	for _, arg := range req.args {
		tuple = append(tuple, arg.value)
	}

	return &tnt.Insert{
		Space: req.space,
		Tuple: tuple,
	}
}

func makeInsertQueries(reqs []*request) []tnt.Query {
	queries := make([]tnt.Query, 0, len(reqs))
	for _, req := range reqs {
		q := makeInsertQuery(req)
		if q != nil {
			queries = append(queries, q)
		}
	}

	return queries
}

func makeUpdateQueries(reqs []*request) []tnt.Query {
	queries := make([]tnt.Query, 0, len(reqs))
	for _, req := range reqs {
		if req.action == actionDelete {
			queries = append(queries, makeDeleteQuery(req))
			continue
		}

		if req.action == actionInsert {
			queries = append(queries, makeInsertQuery(req))
			continue
		}

		keyTuple := make([]interface{}, 0, len(req.keys))
		for _, key := range req.keys {
			keyTuple = append(keyTuple, key.value)
		}

		set := make([]tnt.Operator, 0, len(req.args))
		for _, arg := range req.args {
			set = append(set, &tnt.OpAssign{
				Field:    arg.field,
				Argument: arg.value,
			})
		}

		q := &tnt.Update{
			Space:    req.space,
			KeyTuple: keyTuple,
			Set:      set,
		}
		queries = append(queries, q)
	}

	return queries
}

func makeDeleteQuery(req *request) tnt.Query {
	if req.action != actionDelete {
		return nil
	}

	tuple := make([]interface{}, 0, len(req.keys))
	for _, key := range req.keys {
		tuple = append(tuple, key.value)
	}

	return &tnt.Delete{
		Space:    req.space,
		KeyTuple: tuple,
	}
}

func makeDeleteQueries(reqs []*request) []tnt.Query {
	queries := make([]tnt.Query, 0, len(reqs))
	for _, req := range reqs {
		q := makeDeleteQuery(req)
		if q != nil {
			queries = append(queries, q)
		}
	}

	return queries
}
