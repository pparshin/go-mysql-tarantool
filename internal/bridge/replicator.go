package bridge

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/rs/zerolog"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	tnt "github.com/viciious/go-tarantool"

	"github.com/pparshin/go-mysql-tarantool/internal/config"
	"github.com/pparshin/go-mysql-tarantool/internal/tarantool"
)

var (
	ErrRuleNotExist = errors.New("rule is not exist")
)

type action string

const (
	actionInsert action = "insert"
	actionUpdate action = "update"
	actionDelete action = "delete"
)

type request struct {
	action action
	space  string
	keys   []interface{}
	args   []interface{}
}

type batch struct {
	action action
	reqs   []request
}

type Bridge struct {
	rules map[string]*rule

	canal      *canal.Canal
	tntClient  *tarantool.Client
	stateSaver stateSaver

	ctx    context.Context
	cancel context.CancelFunc
	logger zerolog.Logger

	syncCh    chan interface{}
	closeOnce *sync.Once
}

func New(cfg *config.Config, logger zerolog.Logger) (*Bridge, error) {
	b := &Bridge{
		logger:    logger,
		syncCh:    make(chan interface{}, 4096),
		closeOnce: &sync.Once{},
	}

	ctx, cancel := context.WithCancel(context.Background())
	b.ctx = ctx
	b.cancel = cancel

	if err := b.newStateSaver(cfg); err != nil {
		return nil, err
	}

	if err := b.newRules(cfg); err != nil {
		return nil, err
	}

	if err := b.newCanal(cfg); err != nil {
		return nil, err
	}

	// We must use binlog full row image.
	if err := b.canal.CheckBinlogRowImage("FULL"); err != nil {
		return nil, err
	}

	b.newTarantoolClient(cfg)

	return b, nil
}

func (b *Bridge) newStateSaver(cfg *config.Config) error {
	fs, err := newFileSaver(cfg.App.DataFile, cfg.Replication.GTIDMode)
	if err != nil {
		return err
	}

	_, err = fs.load()
	if err != nil {
		return err
	}

	b.stateSaver = fs

	return nil
}

func (b *Bridge) newRules(cfg *config.Config) error {
	rules := make(map[string]*rule, len(cfg.Replication.Mappings))
	for _, mapping := range cfg.Replication.Mappings {
		source := mapping.Source

		tableInfo, err := b.canal.GetTable(source.Schema, source.Table)
		if err != nil {
			return err
		}

		pks := make([]string, 0)
		if len(source.Pks) == 0 {
			b.logger.Info().
				Str("schema", source.Schema).
				Str("table", source.Table).
				Msg("no user defined primary columns: load them from table primary index")
			for _, pki := range tableInfo.PKColumns {
				column := tableInfo.GetPKColumn(pki)
				pks = append(pks, column.Name)
			}
		} else {
			pks = source.Pks
		}

		if len(pks) == 0 {
			return fmt.Errorf("no primary keys given or found, schema: %s, table: %s", source.Schema, source.Table)
		}

		columns := make([]string, 0, len(source.Columns))
		for _, col := range source.Columns {
			isPK := false
			for _, pk := range pks {
				if col == pk {
					isPK = true
					break
				}
			}

			if !isPK {
				columns = append(columns, col)
			}
		}

		rule := &rule{
			schema:    source.Schema,
			table:     source.Table,
			pks:       pks,
			columns:   columns,
			space:     mapping.Dest.Space,
			tableInfo: tableInfo,
		}

		key := ruleKey(rule.schema, rule.table)
		rules[key] = rule
	}

	b.rules = rules

	return nil
}

func (b *Bridge) updateRule(schema, table string) error {
	rule, ok := b.rules[ruleKey(schema, table)]
	if !ok {
		return ErrRuleNotExist
	}

	tableInfo, err := b.canal.GetTable(schema, table)
	if err != nil {
		return err
	}

	rule.tableInfo = tableInfo

	return nil
}

func (b *Bridge) newCanal(cfg *config.Config) error {
	canalCfg := canal.NewDefaultConfig()

	myCfg := cfg.Replication.ConnectionSrc
	if cfg.Replication.ServerID != nil {
		canalCfg.ServerID = *cfg.Replication.ServerID
	}
	canalCfg.Addr = myCfg.Addr
	canalCfg.User = myCfg.User
	canalCfg.Password = myCfg.Password
	canalCfg.Charset = myCfg.Charset
	canalCfg.Flavor = mysql.MySQLFlavor // TODO: support MariaDB
	canalCfg.SemiSyncEnabled = false

	canalCfg.Dump.ExecutionPath = myCfg.Dump.ExecPath
	canalCfg.Dump.DiscardErr = false
	canalCfg.Dump.SkipMasterData = myCfg.Dump.SkipMasterData

	cn, err := canal.NewCanal(canalCfg)
	if err != nil {
		return err
	}

	var db string
	dbs := map[string]struct{}{}
	tables := make([]string, 0, len(b.rules))
	for _, rule := range b.rules {
		db = rule.schema
		dbs[rule.schema] = struct{}{}
		tables = append(tables, rule.table)
	}

	if len(dbs) == 1 {
		b.canal.AddDumpTables(db, tables...)
	} else {
		keys := make([]string, 0, len(dbs))
		for key := range dbs {
			keys = append(keys, key)
		}

		b.canal.AddDumpDatabases(keys...)
	}

	eH := newEventHandler(b, cfg.Replication.GTIDMode)
	cn.SetEventHandler(eH)

	return nil
}

func (b *Bridge) newTarantoolClient(cfg *config.Config) {
	conn := cfg.Replication.ConnectionDest
	opts := &tarantool.Options{
		Addr:           conn.Addr,
		User:           conn.User,
		Password:       conn.Password,
		Retries:        conn.MaxRetries,
		ConnectTimeout: conn.ConnectTimeout,
		QueryTimeout:   conn.RequestTimeout,
	}

	b.tntClient = tarantool.New(opts)
}

// Run syncs the data from MySQL and inserts to Tarantool
// until closed or meets error.
func (b *Bridge) Run() error {
	go func() {
		err := b.syncLoop()
		if err != nil {
			b.logger.Err(err).Msg("sync error, stopping replication...")
		}
		err = b.Close()
		if err != nil {
			b.logger.Err(err).Msg("failed to stop replicator")
		}
	}()

	var err error
	pos := b.stateSaver.position()
	switch p := pos.(type) {
	case *gtidSet:
		err = b.canal.StartFromGTID(p.pos)
	case *binlogPos:
		err = b.canal.RunFrom(p.pos)
	default:
		err = errors.New("unsupported master position: expected GTID set or binlog file position")
	}
	if err != nil {
		b.logger.Err(err).Msg("broken replication")
		return err
	}

	return nil
}

func (b *Bridge) syncLoop() error {
	for {
		select {
		case got := <-b.syncCh:
			switch v := got.(type) {
			case *savePos:
				err := b.stateSaver.save(v.pos, v.force)
				if err != nil {
					return err
				}
			case *batch:
				err := b.doBatch(v)
				if err != nil {
					return err
				}
			}
		case <-b.ctx.Done():
			return nil
		}
	}
}

func (b *Bridge) doBatch(req *batch) error {
	var queries []tnt.Query
	switch req.action {
	case actionUpdate:
		queries = makeUpdateQueries(req.reqs)
	case actionInsert:
		queries = makeInsertQueries(req.reqs)
	case actionDelete:
		queries = makeDeleteQueries(req.reqs)
	}

	for _, q := range queries {
		err := b.tntClient.Exec(b.ctx, q)
		if err != nil {
			b.logger.Err(err).
				Str("query", fmt.Sprintf("%+v", q)).
				Msg("could not exec tarantool query")
			return err
		}
	}

	return nil
}

func (b *Bridge) Close() error {
	var err error

	b.closeOnce.Do(func() {
		b.canal.Close()
		err = b.stateSaver.close()
		b.cancel()
	})

	return err
}
