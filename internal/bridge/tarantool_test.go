package bridge

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/viciious/go-tarantool"

	"github.com/pparshin/go-mysql-tarantool/internal/config"
	tnt "github.com/pparshin/go-mysql-tarantool/internal/tarantool"
)

type tarantoolSuite struct {
	suite.Suite

	client *tnt.Client
}

func TestOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("test requires dev env - skipping it in a short mode.")
	}

	cfgPath, err := filepath.Abs("testdata/cfg.yml")
	require.NoError(t, err)

	cfg, err := config.ReadFromFile(cfgPath)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	connCfg := cfg.Replication.ConnectionDest
	client := tnt.New(&tnt.Options{
		Addr:           connCfg.Addr,
		User:           connCfg.User,
		Password:       connCfg.Password,
		Retries:        connCfg.MaxRetries,
		ConnectTimeout: connCfg.ConnectTimeout,
		QueryTimeout:   connCfg.RequestTimeout,
	})

	suite.Run(t, &tarantoolSuite{
		client: client,
	})
}

func (s *tarantoolSuite) AfterTest(_, _ string) {
	t := s.T()

	_, err := s.client.Exec(context.Background(), &tarantool.Eval{
		Expression: "box.space.users:truncate()",
	})
	assert.NoError(t, err)

	_, err = s.client.Exec(context.Background(), &tarantool.Eval{
		Expression: "box.space.logins:truncate()",
	})
	assert.NoError(t, err)

	s.client.Close()
}

func (s *tarantoolSuite) TestInsert() {
	t := s.T()

	tests := []struct {
		name string
		req  *request
	}{
		{
			name: "SinglePK",
			req: &request{
				action: actionInsert,
				space:  "users",
				keys: []reqArg{{
					field: 0,
					value: uint64(1),
				}},
				args: []reqArg{
					{field: 1, value: "alice"},
					{field: 2, value: "12345"},
					{field: 3, value: "alice@email.com"},
				},
			},
		},
		{
			name: "MultiplePKs",
			req: &request{
				action: actionInsert,
				space:  "logins",
				keys: []reqArg{
					{field: 0, value: "alice"},
					{field: 1, value: "10.10.10.1"},
					{field: 2, value: 1604338416},
				},
				args: []reqArg{{
					field: 3,
					value: uint64(2),
				}},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			q := makeInsertQuery(tt.req)
			_, err := s.client.Exec(context.Background(), q)
			assert.NoError(t, err)
		})
	}
}

func (s *tarantoolSuite) TestUpdate() {
	t := s.T()

	tests := []struct {
		name    string
		prepare *request
		req     *request
		get     tarantool.Query
		want    []interface{}
	}{
		{
			name: "SinglePK",
			prepare: &request{
				action: actionInsert,
				space:  "users",
				keys: []reqArg{{
					field: 0,
					value: uint64(1),
				}},
				args: []reqArg{
					{field: 1, value: "alice"},
					{field: 2, value: "12345"},
					{field: 3, value: "alice@email.com"},
				},
			},
			req: &request{
				action: actionUpdate,
				space:  "users",
				keys: []reqArg{{
					field: 0,
					value: uint64(1),
				}},
				args: []reqArg{
					{field: 1, value: "alice"},
					{field: 2, value: "pwd"},
					{field: 3, value: "alice@mail.ru"},
				},
			},
			get: &tarantool.Select{
				Space: "users",
				Key:   uint64(1),
			},
			want: []interface{}{
				uint64(1),
				"alice",
				"pwd",
				"alice@mail.ru",
			},
		},
		{
			name: "MultiplePKs",
			prepare: &request{
				action: actionInsert,
				space:  "logins",
				keys: []reqArg{
					{field: 0, value: "alice"},
					{field: 1, value: "10.10.10.1"},
					{field: 2, value: 1604338416},
				},
				args: []reqArg{
					{field: 3, value: uint64(2)},
				},
			},
			req: &request{
				action: actionUpdate,
				space:  "logins",
				keys: []reqArg{
					{field: 0, value: "alice"},
					{field: 1, value: "10.10.10.1"},
					{field: 2, value: 1604338416},
				},
				args: []reqArg{
					{field: 3, value: uint64(222)},
				},
			},
			get: &tarantool.Select{
				Space:    "logins",
				KeyTuple: []interface{}{"alice", "10.10.10.1", 1604338416},
			},
			want: []interface{}{
				"alice",
				"10.10.10.1",
				1604338416,
				uint64(222),
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			prepare := makeInsertQuery(tt.prepare)
			_, err := s.client.Exec(context.Background(), prepare)
			require.NoError(t, err)

			queries := makeUpdateQueries([]*request{tt.req})
			for _, q := range queries {
				_, err := s.client.Exec(context.Background(), q)
				assert.NoError(t, err)
			}

			res, err := s.client.Exec(context.Background(), tt.get)
			require.NoError(t, err)
			require.NotNil(t, res)
			require.Len(t, res.Data, 1)

			got := res.Data[0]
			if assert.Len(t, got, len(tt.want)) {
				for i, v := range got {
					assert.EqualValues(t, tt.want[i], v)
				}
			}
		})
	}
}

func (s *tarantoolSuite) TestDelete() {
	t := s.T()

	tests := []struct {
		name    string
		prepare *request
		req     *request
		get     tarantool.Query
	}{
		{
			name: "SinglePK",
			prepare: &request{
				action: actionInsert,
				space:  "users",
				keys: []reqArg{{
					field: 0,
					value: uint64(1),
				}},
				args: []reqArg{
					{field: 1, value: "alice"},
					{field: 2, value: "12345"},
					{field: 3, value: "alice@email.com"},
				},
			},
			req: &request{
				action: actionDelete,
				space:  "users",
				keys: []reqArg{{
					field: 0,
					value: uint64(1),
				}},
			},
			get: &tarantool.Select{
				Space:    "users",
				KeyTuple: []interface{}{uint64(1)},
			},
		},
		{
			name: "MultiplePKs",
			prepare: &request{
				action: actionInsert,
				space:  "logins",
				keys: []reqArg{
					{field: 0, value: "alice"},
					{field: 1, value: "10.10.10.1"},
					{field: 2, value: 1604338416},
				},
				args: []reqArg{
					{field: 3, value: uint64(2)},
				},
			},
			req: &request{
				action: actionDelete,
				space:  "logins",
				keys: []reqArg{
					{field: 0, value: "alice"},
					{field: 1, value: "10.10.10.1"},
					{field: 2, value: 1604338416},
				},
			},
			get: &tarantool.Select{
				Space:    "logins",
				KeyTuple: []interface{}{"alice", "10.10.10.1", 1604338416},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			prepare := makeInsertQuery(tt.prepare)
			_, err := s.client.Exec(context.Background(), prepare)
			require.NoError(t, err)

			query := makeDeleteQuery(tt.req)
			_, err = s.client.Exec(context.Background(), query)
			assert.NoError(t, err)

			res, err := s.client.Exec(context.Background(), tt.get)
			require.NoError(t, err)
			require.NotNil(t, res)
			require.Empty(t, res.Data)
		})
	}
}
