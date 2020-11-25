package config

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadFromFile_InvalidPath(t *testing.T) {
	cfg, err := ReadFromFile("invalid_path")
	assert.NotNil(t, err)
	assert.Nil(t, cfg)
}

func TestReadFromFile_ValidPath(t *testing.T) {
	testConfigPath, err := filepath.Abs("testdata/replicator.conf.yml")
	require.NoError(t, err)

	cfg, err := ReadFromFile(testConfigPath)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, ":8081", cfg.App.ListenAddr)
	assert.Equal(t, "/etc/mysql-tarantool-replicator/state.info", cfg.App.DataFile)

	healthCfg := cfg.App.Health
	assert.Equal(t, 5, healthCfg.SecondsBehindMaster)

	loggingCfg := cfg.App.Logging
	assert.Equal(t, "debug", loggingCfg.Level)
	assert.True(t, loggingCfg.SysLogEnabled)
	assert.True(t, loggingCfg.FileLoggingEnabled)
	assert.Equal(t, "/var/log/mysql-tarantool-repl.log", loggingCfg.Filename)
	assert.Equal(t, 256, loggingCfg.MaxSize)
	assert.Equal(t, 3, loggingCfg.MaxBackups)
	assert.Equal(t, 5, loggingCfg.MaxAge)

	require.NotNil(t, cfg.Replication.ServerID)
	assert.EqualValues(t, 100, *cfg.Replication.ServerID)
	assert.True(t, cfg.Replication.GTIDMode)

	connSrc := cfg.Replication.ConnectionSrc
	assert.Equal(t, "/usr/bin/mysqldump", connSrc.Dump.ExecPath)
	assert.False(t, connSrc.Dump.SkipMasterData)
	assert.Equal(t, []string{"--column-statistics=0"}, connSrc.Dump.ExtraOptions)
	assert.Equal(t, "127.0.0.1:3306", connSrc.Addr)
	assert.Equal(t, "repl", connSrc.User)
	assert.Equal(t, "repl", connSrc.Password)
	assert.Equal(t, "utf8", connSrc.Charset)

	destSrc := cfg.Replication.ConnectionDest
	assert.Equal(t, "127.0.0.1:3301", destSrc.Addr)
	assert.Equal(t, "repl", destSrc.User)
	assert.Equal(t, "repl", destSrc.Password)
	assert.Equal(t, 3, destSrc.MaxRetries)
	assert.Equal(t, 500*time.Millisecond, destSrc.ConnectTimeout)
	assert.Equal(t, 500*time.Millisecond, destSrc.RequestTimeout)

	mappings := cfg.Replication.Mappings
	require.Len(t, mappings, 1)

	mapping := mappings[0]
	assert.Equal(t, "city", mapping.Source.Schema)
	assert.Equal(t, "users", mapping.Source.Table)
	assert.Equal(t, []string{"username", "password", "email"}, mapping.Source.Columns)
	assert.Equal(t, "users", mapping.Dest.Space)
	assert.Len(t, mapping.Dest.Column, 3)
	columnMapping, ok := mapping.Dest.Column["attempts"]
	if assert.True(t, ok) {
		assert.Equal(t, "unsigned", columnMapping.Cast)
		assert.Equal(t, 0, columnMapping.OnNull)
	}
	columnMapping, ok = mapping.Dest.Column["email"]
	if assert.True(t, ok) {
		assert.Equal(t, "", columnMapping.Cast)
		assert.Equal(t, "", columnMapping.OnNull)
	}
	columnMapping, ok = mapping.Dest.Column["client_id"]
	if assert.True(t, ok) {
		assert.Equal(t, "unsigned", columnMapping.Cast)
		assert.Nil(t, columnMapping.OnNull)
	}
}
