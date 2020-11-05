package bridge

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/viciious/go-tarantool"

	"github.com/pparshin/go-mysql-tarantool/internal/config"
	tnt "github.com/pparshin/go-mysql-tarantool/internal/tarantool"
)

type bridgeSuite struct {
	suite.Suite

	bridge  *Bridge
	sqlConn *client.Conn
	tntConn *tnt.Client
	logger  zerolog.Logger
	cfg     *config.Config
}

func (s *bridgeSuite) init(cfg *config.Config) {
	b, err := New(cfg, s.logger)
	require.NoError(s.T(), err)

	s.bridge = b
}

func (s *bridgeSuite) executeSQL(query string, args ...interface{}) (*mysql.Result, error) {
	return s.sqlConn.Execute(query, args...)
}

func (s *bridgeSuite) executeTNT(query tarantool.Query) (*tarantool.Result, error) {
	return s.tntConn.Exec(context.Background(), query)
}

func TestReplication(t *testing.T) {
	if testing.Short() {
		t.Skip("test requires dev env - skipping it in a short mode.")
	}

	cfgPath, err := filepath.Abs("testdata/cfg.yml")
	require.NoError(t, err)

	cfg, err := config.ReadFromFile(cfgPath)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	logger := zerolog.New(zerolog.NewConsoleWriter())

	connSrc := cfg.Replication.ConnectionSrc
	sqlConn, err := client.Connect(connSrc.Addr, connSrc.User, connSrc.Password, "city")
	require.NoError(t, err)

	connDest := cfg.Replication.ConnectionDest
	tntConn := tnt.New(&tnt.Options{
		Addr:           connDest.Addr,
		User:           connDest.User,
		Password:       connDest.Password,
		Retries:        connDest.MaxRetries,
		ConnectTimeout: connDest.ConnectTimeout,
		QueryTimeout:   connDest.RequestTimeout,
	})

	suite.Run(t, &bridgeSuite{
		sqlConn: sqlConn,
		tntConn: tntConn,
		logger:  logger,
		cfg:     cfg,
	})
}

func (s *bridgeSuite) AfterTest(_, _ string) {
	t := s.T()

	if s.bridge != nil {
		err := s.bridge.Close()
		assert.NoError(t, err)
	}

	_, err := s.executeTNT(&tarantool.Eval{
		Expression: "box.space.users:truncate()",
	})
	assert.NoError(t, err)

	_, err = s.executeSQL("TRUNCATE city.users")
	assert.NoError(t, err)

	_, err = s.executeSQL("RESET MASTER")
	assert.NoError(t, err)

	dataDir := path.Dir(s.cfg.App.DataFile)

	err = os.RemoveAll(dataDir)
	assert.NoError(t, err)
}

func (s *bridgeSuite) TestNewBridge() {
	b, err := New(s.cfg, s.logger)
	require.NoError(s.T(), err)

	s.bridge = b
}

func (s *bridgeSuite) TestDump() {
	t := s.T()

	// Prepare initial data.
	for i := 0; i < 200; i++ {
		_, err := s.executeSQL("INSERT INTO city.users (username, password, name, email) VALUES (?, ?, ?, ?)", "bob", "12345", "Bob", "bob@email.com")
		require.NoError(t, err)
	}

	s.init(s.cfg)

	go func() {
		errors := s.bridge.Run()
		for err := range errors {
			assert.NoError(t, err)
		}
	}()

	<-s.bridge.canal.WaitDumpDone()

	time.Sleep(50 * time.Millisecond) // ensure that all events synced

	err := s.bridge.Close()
	assert.NoError(t, err)

	got, err := s.executeTNT(&tarantool.Select{
		Space:    "users",
		Limit:    300,
		Iterator: tarantool.IterAll,
	})
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NotEmpty(t, got.Data)
	require.Len(t, got.Data, 200)
}

func (s *bridgeSuite) TestReplication() {
	t := s.T()

	s.init(s.cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	go func() {
		errors := s.bridge.Run()
		for err := range errors {
			assert.NoError(t, err)
		}
		cancel()
	}()

	<-s.bridge.canal.WaitDumpDone()

	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()

	inserted := 0

tank:
	for inserted < 200 {
		select {
		case <-tick.C:
			_, err := s.executeSQL("INSERT INTO city.users (username, password, name, email) VALUES (?, ?, ?, ?)", "bob", "12345", "Bob", "bob@email.com")
			if assert.NoError(t, err) {
				inserted++
			}
			_, err = s.executeSQL("INSERT INTO city.users (username, password, name, email) VALUES (?, ?, ?, ?)", "alice", "qwerty", "Alice", "alice@email.com")
			if assert.NoError(t, err) {
				inserted++
			}
		case <-ctx.Done():
			break tank
		}
	}

	_, err := s.executeSQL("DELETE FROM city.users where username=?", "alice")
	require.NoError(t, err)

	_, err = s.executeSQL("UPDATE city.users SET password = ?, email = ? where username = ?", "11111", "boby@gmail.com", "bob")
	require.NoError(t, err)

	err = s.bridge.canal.CatchMasterPos(500 * time.Millisecond)
	require.NoError(t, err)

	masterGTIDSet, err := s.bridge.canal.GetMasterGTIDSet()
	assert.NoError(t, err)
	bridgePos := s.bridge.stateSaver.position()
	gtidPos, ok := bridgePos.(*gtidSet)
	if assert.True(t, ok) {
		assert.True(t, masterGTIDSet.Contain(gtidPos.pos), "bridge: %s, master: %s", gtidPos.pos, masterGTIDSet)
	}

	time.Sleep(50 * time.Millisecond) // ensure that all events synced

	err = s.bridge.Close()
	assert.NoError(t, err)

	got, err := s.executeTNT(&tarantool.Select{
		Space:    "users",
		Limit:    200,
		Iterator: tarantool.IterAll,
	})
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NotEmpty(t, got.Data)
	require.Len(t, got.Data, inserted/2)
}

func (s *bridgeSuite) TestReplicationWithoutDump() {
	t := s.T()

	// Prepare initial data.
	for i := 0; i < 200; i++ {
		_, err := s.executeSQL("INSERT INTO city.users (username, password, name, email) VALUES (?, ?, ?, ?)", "bob", "12345", "Bob", "bob@email.com")
		require.NoError(t, err)
	}

	// Purge current binlog.
	_, err := s.executeSQL("RESET MASTER")
	require.NoError(t, err)

	cfg := *s.cfg
	cfg.Replication.ConnectionSrc.Dump.ExecPath = ""
	s.init(&cfg)

	go func() {
		errors := s.bridge.Run()
		for err := range errors {
			assert.NoError(t, err)
		}
	}()

	_, err = s.executeSQL("INSERT INTO city.users (username, password, name, email) VALUES (?, ?, ?, ?)", "alice", "12345", "Alice", "alice@email.com")
	require.NoError(t, err)

	err = s.bridge.canal.CatchMasterPos(500 * time.Millisecond)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond) // ensure that all events synced

	err = s.bridge.Close()
	assert.NoError(t, err)

	got, err := s.executeTNT(&tarantool.Select{
		Space:    "users",
		Iterator: tarantool.IterAll,
	})
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NotEmpty(t, got.Data)
	require.Len(t, got.Data, 1)
	require.EqualValues(t, []interface{}{uint64(201), "alice", "12345", "alice@email.com"}, got.Data[0])
}
