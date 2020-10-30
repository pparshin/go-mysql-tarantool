package config

import (
	"io/ioutil"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	defaultDataFile           = "/etc/mysql-tarantool-replicator/state.info"
	defaultLogLevel           = "debug"
	defaultSysLogEnabled      = false
	defaultFileLoggingEnabled = false
	defaultLogFilename        = "/var/log/mysql-tarantool-repl.log"
	defaultLogFileMaxSize     = 256
	defaultLogFileMaxBackups  = 3
	defaultLogFileMaxAge      = 5
	defaultGTIDMode           = true
	defaultDumpExecPath       = "/usr/bin/mysqldump"
	defaultCharset            = "utf8mb4_unicode_ci"
	defaultConnectTimeout     = 500 * time.Millisecond
	defaultRequestTimeout     = 1 * time.Second
)

type Config struct {
	App         AppConfig `yaml:"app"`
	Replication struct {
		// ServerID is the unique ID of the replica in MySQL cluster.
		// Omit this option if you'd like to auto generate ID.
		ServerID *uint32 `yaml:"server_id"`
		// GTIDMode indicates when to use GTID-based replication
		// or binlog file position.
		GTIDMode bool `yaml:"gtid_mode"`
		// ConnectionSrc is the options to connect to MySQL.
		ConnectionSrc SourceConnectConfig `yaml:"mysql"`
		// ConnectionDest is the options to connect to Tarantool.
		ConnectionDest DestConnectConfig `yaml:"tarantool"`
		// Mappings contains rules to map data from MySQL to Tarantool.
		Mappings []Mapping `yaml:"mappings"`
	} `yaml:"replication"`
}

type AppConfig struct {
	DataFile string  `yaml:"data_file"`
	Logging  Logging `yaml:"logging"`
}

type Logging struct {
	Level              string `yaml:"level"`
	SysLogEnabled      bool   `yaml:"syslog_enabled"`
	FileLoggingEnabled bool   `yaml:"file_enabled"`
	Filename           string `yaml:"file_name"`
	MaxSize            int    `yaml:"file_max_size"`    // megabytes
	MaxBackups         int    `yaml:"file_max_backups"` // files
	MaxAge             int    `yaml:"file_max_age"`     // days
}

func (c *AppConfig) withDefaults() {
	if c == nil {
		return
	}

	c.DataFile = defaultDataFile

	c.Logging.Level = defaultLogLevel
	c.Logging.SysLogEnabled = defaultSysLogEnabled
	c.Logging.FileLoggingEnabled = defaultFileLoggingEnabled
	c.Logging.Filename = defaultLogFilename
	c.Logging.MaxSize = defaultLogFileMaxSize
	c.Logging.MaxBackups = defaultLogFileMaxBackups
	c.Logging.MaxAge = defaultLogFileMaxAge
}

type SourceConnectConfig struct {
	Dump struct {
		// ExecPath is absolute path to mysqldump binary.
		ExecPath string `yaml:"dump_exec_path"`
		// SkipMasterData set true if you have no privilege to use `--master-data`.
		SkipMasterData bool `yaml:"skip_master_data"`
	} `yaml:"skip_master_data"`
	Addr     string `yaml:"addr"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Charset  string `yaml:"charset"`
}

func (c *SourceConnectConfig) withDefaults() {
	if c == nil {
		return
	}

	c.Dump.ExecPath = defaultDumpExecPath
	c.Charset = defaultCharset
}

type DestConnectConfig struct {
	Addr           string        `yaml:"addr"`
	User           string        `yaml:"user"`
	Password       string        `yaml:"password"`
	MaxRetries     int           `yaml:"max_retries"`
	ConnectTimeout time.Duration `yaml:"connect_timeout"`
	RequestTimeout time.Duration `yaml:"request_timeout"`
}

func (c *DestConnectConfig) withDefaults() {
	if c == nil {
		return
	}

	c.ConnectTimeout = defaultConnectTimeout
	c.RequestTimeout = defaultRequestTimeout
}

type Mapping struct {
	Source struct {
		Schema  string   `yaml:"schema"`
		Table   string   `yaml:"table"`
		Pks     []string `yaml:"pks"`
		Columns []string `yaml:"columns"`
	} `yaml:"source"`

	Dest struct {
		Space string `yaml:"space"`
	} `yaml:"dest"`
}

func ReadFromFile(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = file.Close()
	}()

	data, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var cfg Config
	cfg.withDefaults()
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (c *Config) withDefaults() {
	if c == nil {
		return
	}

	app := &c.App
	app.withDefaults()

	c.Replication.GTIDMode = defaultGTIDMode

	srcConn := &c.Replication.ConnectionSrc
	srcConn.withDefaults()

	destConn := &c.Replication.ConnectionDest
	destConn.withDefaults()
}
