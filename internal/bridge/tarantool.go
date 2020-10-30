package bridge

import tnt "github.com/viciious/go-tarantool"

func makeInsertQueries(reqs []request) []tnt.Query {
	queries := make([]tnt.Query, 0, len(reqs))
	for _, req := range reqs {
		if req.action != actionInsert {
			continue
		}

		tuple := append(req.keys, req.args...)

		q := &tnt.Insert{
			Space: req.space,
			Tuple: tuple,
		}
		queries = append(queries, q)
	}

	return queries
}

func makeUpdateQueries(reqs []request) []tnt.Query {
	queries := make([]tnt.Query, 0, len(reqs))
	for _, req := range reqs {
		if req.action != actionUpdate {
			continue
		}

		pks := len(req.keys)

		set := make([]tnt.Operator, 0, len(req.args))
		for i, arg := range req.args {
			set = append(set, &tnt.OpAssign{
				Field:    uint64(pks + i + 1),
				Argument: arg,
			})
		}

		q := &tnt.Upsert{
			Space: req.space,
			Tuple: req.keys,
			Set:   set,
		}
		queries = append(queries, q)
	}

	return queries
}

func makeDeleteQueries(reqs []request) []tnt.Query {
	queries := make([]tnt.Query, 0, len(reqs))
	for _, req := range reqs {
		if req.action != actionDelete {
			continue
		}

		q := &tnt.Delete{
			Space:    req.space,
			KeyTuple: req.keys,
		}
		queries = append(queries, q)
	}

	return queries
}
