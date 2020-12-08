package bridge

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	tnt "github.com/viciious/go-tarantool"
	"go.uber.org/atomic"

	"github.com/pparshin/go-mysql-tarantool/internal/config"
	"github.com/pparshin/go-mysql-tarantool/internal/metrics"
	"github.com/pparshin/go-mysql-tarantool/internal/tarantool"
)

const eventsBufSize = 4096

var ErrRuleNotExist = errors.New("rule is not exist")

type Bridge struct {
	rules map[string]*rule

	canal      *canal.Canal
	tntClient  *tarantool.Client
	stateSaver stateSaver

	ctx    context.Context
	cancel context.CancelFunc
	logger zerolog.Logger

	dumping  *atomic.Bool
	running  *atomic.Bool
	syncedAt *atomic.Int64

	syncCh    chan interface{}
	closeOnce *sync.Once
}

func New(cfg *config.Config, logger zerolog.Logger) (*Bridge, error) {
	b := &Bridge{
		logger:    logger,
		dumping:   atomic.NewBool(false),
		running:   atomic.NewBool(false),
		syncedAt:  atomic.NewInt64(0),
		syncCh:    make(chan interface{}, eventsBufSize),
		closeOnce: &sync.Once{},
	}

	ctx, cancel := context.WithCancel(context.Background())
	b.ctx = ctx
	b.cancel = cancel

	if err := b.newStateSaver(cfg); err != nil {
		return nil, err
	}

	if err := b.newCanal(cfg); err != nil {
		return nil, err
	}

	if err := b.newRules(cfg); err != nil {
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
		colmap := mapping.Dest.Column

		tableInfo, err := b.canal.GetTable(source.Schema, source.Table)
		if err != nil {
			return err
		}

		pks := newAttrsFromPKs(tableInfo)
		if len(pks) == 0 {
			return fmt.Errorf("no primary keys found, schema: %s, table: %s", source.Schema, source.Table)
		}
		for _, pk := range pks {
			if m, ok := colmap[pk.name]; ok {
				pk.castTo(castTypeFromString(m.Cast))
				pk.onNull = m.OnNull
			}
		}

		attrs := make([]*attribute, 0, len(source.Columns))
		for i, name := range source.Columns {
			isPK := false
			for _, pk := range pks {
				if name == pk.name {
					isPK = true

					break
				}
			}

			if !isPK {
				tupIndex := uint64(i + len(pks))
				attr, err := newAttr(tableInfo, tupIndex, name)
				if err != nil {
					return err
				}

				if m, ok := colmap[name]; ok {
					attr.castTo(castTypeFromString(m.Cast))
					attr.onNull = m.OnNull
				}

				attrs = append(attrs, attr)
			}
		}

		rule := &rule{
			schema:    source.Schema,
			table:     source.Table,
			pks:       pks,
			attrs:     attrs,
			space:     mapping.Dest.Space,
			tableInfo: tableInfo,
		}

		key := ruleKey(rule.schema, rule.table)
		rules[key] = rule
	}

	b.rules = rules
	b.syncRulesAndCanalDump()

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
	canalCfg.Dump.ExtraOptions = myCfg.Dump.ExtraOptions

	syncOnly := make([]string, 0, len(cfg.Replication.Mappings))
	for _, mapping := range cfg.Replication.Mappings {
		source := mapping.Source
		regex := fmt.Sprintf("%s\\.%s", source.Schema, source.Table)
		syncOnly = append(syncOnly, regex)
	}
	canalCfg.IncludeTableRegex = syncOnly

	cn, err := canal.NewCanal(canalCfg)
	if err != nil {
		return err
	}

	eH := newEventHandler(b, cfg.Replication.GTIDMode)
	cn.SetEventHandler(eH)

	b.canal = cn

	return nil
}

func (b *Bridge) syncRulesAndCanalDump() {
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
// until closed or meets errors.
//
// Returns closed channel with all errors or an empty channel.
func (b *Bridge) Run() <-chan error {
	defer b.setRunning(false)

	go b.runBackgroundJobs()

	maxErrs := 3
	errCh := make(chan error, maxErrs)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		err := b.syncLoop()
		if err != nil {
			errCh <- fmt.Errorf("sync loop error: %w", err)

			err = b.Close()
			if err != nil {
				errCh <- err
			}
		}
	}()

	b.setDumping(true)
	go func() {
		<-b.canal.WaitDumpDone()
		b.setDumping(false)
		b.setRunning(true)
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
		errCh <- err
	}

	b.cancel()
	wg.Wait()
	close(errCh)

	return errCh
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
			b.syncedAt.Store(time.Now().Unix())
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
		_, err := b.tntClient.Exec(context.Background(), q)
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
		b.cancel()
		err = b.stateSaver.close()
	})

	return err
}

func (b *Bridge) Delay() uint32 {
	return b.canal.GetDelay()
}

func (b *Bridge) setRunning(v bool) {
	b.running.Store(v)
	b.setDumping(false)

	if v {
		metrics.SetReplicationState(metrics.StateRunning)
	} else {
		metrics.SetReplicationState(metrics.StateStopped)
	}
}

func (b *Bridge) Running() bool {
	return b.running.Load()
}

func (b *Bridge) setDumping(v bool) {
	b.dumping.Store(v)
	if v {
		metrics.SetReplicationState(metrics.StateDumping)
	}
}

func (b *Bridge) Dumping() bool {
	return b.dumping.Load()
}

func (b *Bridge) runBackgroundJobs() {
	go func() {
		for range time.Tick(1 * time.Second) {
			metrics.SetSecondsBehindMaster(b.Delay())
		}
	}()

	go func() {
		for range time.Tick(1 * time.Second) {
			syncedAt := b.syncedAt.Load()
			if syncedAt > 0 {
				now := time.Now().Unix()
				metrics.SetSyncedSecondsAgo(now - syncedAt)
			}
		}
	}()
}
