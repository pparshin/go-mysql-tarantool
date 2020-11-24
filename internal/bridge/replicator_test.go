package bridge

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sync"
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

func (s *bridgeSuite) hasSyncedPos() bool {
	syncedGTID := s.bridge.canal.SyncedGTIDSet()
	savedPos := s.bridge.stateSaver.position()

	return savedPos.equal(newGTIDSet(syncedGTID))
}

func (s *bridgeSuite) hasSyncedData(space string, tuples uint64) bool {
	cnt, err := s.countTuples(space)
	if assert.NoError(s.T(), err) {
		return cnt == tuples
	}

	return false
}

func (s *bridgeSuite) countTuples(space string) (uint64, error) {
	res, err := s.executeTNT(&tarantool.Eval{
		Expression: fmt.Sprintf("return box.space.%s:count()", space),
	})
	if err != nil {
		return 0, err
	}
	data := res.Data
	if len(data) == 1 && len(data[0]) == 1 {
		return toUint64(data[0][0])
	}

	return 0, fmt.Errorf("unexpected count result: %v", data)
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

	_, err = s.executeTNT(&tarantool.Eval{
		Expression: "box.space.logins:truncate()",
	})
	assert.NoError(t, err)

	_, err = s.executeSQL("TRUNCATE city.users")
	assert.NoError(t, err)

	_, err = s.executeSQL("TRUNCATE city.logins")
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
	dumpPath := "/usr/bin/mysqldump"
	if !assert.FileExists(t, dumpPath) {
		t.Skip("test requires mysqldump utility")
	}

	tuples := 200

	cfg := *s.cfg
	cfg.Replication.ConnectionSrc.Dump.ExecPath = dumpPath
	s.init(&cfg)

	// Prepare initial data.
	for i := 0; i < tuples; i++ {
		_, err := s.executeSQL("INSERT INTO city.users (username, password, name, email) VALUES (?, ?, ?, ?)", "bob", "12345", "Bob", "bob@email.com")
		require.NoError(t, err)
	}

	go func() {
		errors := s.bridge.Run()
		for err := range errors {
			assert.NoError(t, err)
		}
	}()

	<-s.bridge.canal.WaitDumpDone()

	require.Eventually(t, func() bool {
		return s.hasSyncedData("users", uint64(tuples))
	}, 500*time.Millisecond, 50*time.Millisecond)

	err := s.bridge.Close()
	assert.NoError(t, err)
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

	assert.Eventually(t, func() bool {
		return s.hasSyncedData("users", uint64(inserted/2))
	}, 500*time.Millisecond, 50*time.Millisecond)

	assert.Eventually(t,
		s.hasSyncedPos,
		500*time.Millisecond,
		50*time.Millisecond,
		"bridge: %s, master: %s", s.bridge.stateSaver.position(), s.bridge.canal.SyncedGTIDSet(),
	)

	err = s.bridge.Close()
	assert.NoError(t, err)
}

func (s *bridgeSuite) TestUpdatePrimaryKeys() {
	t := s.T()
	s.init(s.cfg)

	go func() {
		errors := s.bridge.Run()
		for err := range errors {
			assert.NoError(t, err)
		}
	}()

	_, err := s.executeSQL("INSERT INTO city.logins (username, ip, date, attempts, longitude, latitude) VALUES (?, ?, ?, ?, ?, ?)", "alice", "192.168.1.1", 1604571708, 4, 73.98, 40.74)
	require.NoError(t, err)

	_, err = s.executeSQL("UPDATE city.logins SET ip = ?, date = ?, attempts = ? where username = ?", "192.168.1.167", 1604571910, 14, "alice")
	require.NoError(t, err)

	err = s.bridge.canal.CatchMasterPos(500 * time.Millisecond)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return s.hasSyncedData("logins", 1)
	}, 500*time.Millisecond, 50*time.Millisecond)

	err = s.bridge.Close()
	assert.NoError(t, err)

	got, err := s.executeTNT(&tarantool.Select{
		Space:    "logins",
		Iterator: tarantool.IterAll,
	})
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NotEmpty(t, got.Data)
	require.Len(t, got.Data, 1)
	want := []interface{}{"alice", "192.168.1.167", 1604571910, 14, 73.98, 40.74}
	gotTuple := got.Data[0]
	require.Len(t, gotTuple, len(want))
	for i, v := range want {
		require.EqualValues(t, v, gotTuple[i])
	}
}

func (s *bridgeSuite) TestForceCast() {
	t := s.T()

	for step := 1; step <= 2; step++ {
		wantErr := step == 2

		cfg := *s.cfg
		if wantErr {
			for i := range cfg.Replication.Mappings {
				cfg.Replication.Mappings[i].Dest.Cast = nil
			}
		}

		s.init(&cfg)

		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()

			errors := s.bridge.Run()
			if wantErr {
				assert.Len(t, errors, 1)
			} else {
				for err := range errors {
					assert.NoError(t, err)
				}
			}
		}()

		name := fmt.Sprintf("alice_%d", step)
		_, err := s.executeSQL("INSERT INTO city.logins (username, ip, date, attempts, longitude, latitude) VALUES (?, ?, ?, ?, ?, ?)", name, "192.168.1.1", 1604571708, 404, 73.98, 40.74)
		require.NoError(t, err)

		if wantErr {
			wg.Wait()
		} else {
			err = s.bridge.canal.CatchMasterPos(500 * time.Millisecond)
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				return s.hasSyncedData("logins", 1)
			}, 500*time.Millisecond, 50*time.Millisecond)
		}

		err = s.bridge.Close()
		assert.NoError(t, err)
	}
}

func (s *bridgeSuite) TestReconnect() {
	t := s.T()

	for i := 1; i < 5; i++ {
		s.init(s.cfg)

		go func() {
			errors := s.bridge.Run()
			for err := range errors {
				assert.NoError(t, err)
			}
		}()

		name := fmt.Sprintf("robot_%d", i)
		_, err := s.executeSQL("INSERT INTO city.users (username, password, name, email) VALUES (?, ?, ?, ?)", name, "12345", name, "robot@email.com")
		require.NoError(t, err)

		err = s.bridge.canal.CatchMasterPos(500 * time.Millisecond)
		require.NoError(t, err)

		wantTuples := uint64(i)
		require.Eventually(t, func() bool {
			return s.hasSyncedData("users", wantTuples)
		}, 500*time.Millisecond, 50*time.Millisecond)

		err = s.bridge.Close()
		assert.NoError(t, err)
	}
}

func (s *bridgeSuite) TestRenameColumn() {
	t := s.T()

	s.init(s.cfg)

	go func() {
		errors := s.bridge.Run()
		for err := range errors {
			assert.NoError(t, err)
		}
	}()

	<-s.bridge.canal.WaitDumpDone()

	_, err := s.executeSQL("INSERT INTO city.users (username, password, name, email) VALUES (?, ?, ?, ?)", "bob", "12345", "Bob", "bob@email.com")
	require.NoError(t, err)

	_, err = s.executeSQL("ALTER TABLE city.users CHANGE `name` `new_name` varchar(50)")
	require.NoError(t, err)

	defer func() {
		_, err = s.executeSQL("ALTER TABLE city.users CHANGE `new_name` `name` varchar(50)")
		require.NoError(t, err)
	}()

	_, err = s.executeSQL("INSERT INTO city.users (id, username, password, new_name, email) VALUES (?, ?, ?, ?, ?)", 2, "alice", "123", "Alice", "alice@email.com")
	require.NoError(t, err)

	err = s.bridge.canal.CatchMasterPos(500 * time.Millisecond)
	require.NoError(t, err)

	wantRows := uint64(2)
	require.Eventually(t, func() bool {
		return s.hasSyncedData("users", wantRows)
	}, 500*time.Millisecond, 50*time.Millisecond)

	got, err := s.executeTNT(&tarantool.Select{
		Space: "users",
		Key:   2,
	})
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NotEmpty(t, got.Data)
	require.Len(t, got.Data, 1)
	want := []interface{}{2, "alice", "123", "alice@email.com"}
	gotTuple := got.Data[0]
	require.Len(t, gotTuple, len(want))
	for i, v := range want {
		require.EqualValues(t, v, gotTuple[i])
	}

	err = s.bridge.Close()
	assert.NoError(t, err)
}
