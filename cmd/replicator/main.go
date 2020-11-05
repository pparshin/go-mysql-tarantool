package main

import (
	"flag"
	"fmt"
	"io"
	"log/syslog"
	"os"
	"os/signal"
	"path"
	"syscall"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	sidlog "github.com/siddontang/go-log/log"
	"golang.org/x/sys/unix"
	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/pparshin/go-mysql-tarantool/internal/adapter"
	"github.com/pparshin/go-mysql-tarantool/internal/bridge"
	"github.com/pparshin/go-mysql-tarantool/internal/config"
)

var (
	version   = "dev"
	commit    = "none"
	buildDate = "unknown"
)

var (
	configPath = flag.String("config", "", "Config file path")
)

func main() {
	flag.Parse()
	cfg, err := config.ReadFromFile(*configPath)
	if err != nil {
		log.Fatal().Err(err).Msgf("failed to read config")
	}

	logger := initLogger(cfg)
	logger.Info().Msgf("starting replicator %s, commit %s, built at %s", version, commit, buildDate)

	b, err := bridge.New(cfg, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("could not establish bridge from MySQL to Tarantool")
	}

	go func() {
		errors := b.Run()
		for errRun := range errors {
			logger.Err(errRun).Msg("got sync error")
		}

		errClose := b.Close()
		if errClose != nil {
			logger.Err(errClose).Msg("failed to stop replicator")
		}
	}()

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
	sig := <-interrupt

	logger.Info().Msgf("received system signal: %s. Shutting down replicator", sig)

	err = b.Close()
	if err != nil {
		logger.Err(err).Msg("failed to stop replicator")
	}
}

func initLogger(cfg *config.Config) zerolog.Logger {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	loggingCfg := cfg.App.Logging

	logLevel, err := zerolog.ParseLevel(loggingCfg.Level)
	if err != nil {
		log.Warn().Msgf("unknown Level string: '%s', defaulting to DebugLevel", loggingCfg.Level)
		logLevel = zerolog.DebugLevel
	}

	writers := make([]io.Writer, 0, 1)
	writers = append(writers, os.Stdout)

	if loggingCfg.SysLogEnabled {
		w, err := syslog.New(syslog.LOG_INFO, "mysql-tarantool-replicator")
		if err != nil {
			log.Warn().Err(err).Msg("unable to connect to the system log daemon")
		} else {
			writers = append(writers, zerolog.SyslogLevelWriter(w))
		}
	}

	if loggingCfg.FileLoggingEnabled {
		w, err := newRollingLogFile(&loggingCfg)
		if err != nil {
			log.Warn().Err(err).Msg("unable to init file logger")
		} else {
			writers = append(writers, w)
		}
	}

	var baseLogger zerolog.Logger
	if len(writers) == 1 {
		baseLogger = zerolog.New(writers[0])
	} else {
		baseLogger = zerolog.New(zerolog.MultiLevelWriter(writers...))
	}

	logger := baseLogger.Level(logLevel).With().Timestamp().Logger()

	// Redirect siddontang/go-log messages to our logger.
	handler := adapter.NewZeroLogHandler(logger)
	sidlog.SetDefaultLogger(sidlog.New(handler, sidlog.Llevel))
	sidlog.SetLevelByName(logLevel.String())

	return logger
}

func newRollingLogFile(cfg *config.Logging) (io.Writer, error) {
	dir := path.Dir(cfg.Filename)
	if unix.Access(dir, unix.W_OK) != nil {
		return nil, fmt.Errorf("no permissions to write logs to dir: %s", dir)
	}

	return &lumberjack.Logger{
		Filename:   cfg.Filename,
		MaxBackups: cfg.MaxBackups,
		MaxSize:    cfg.MaxSize,
		MaxAge:     cfg.MaxAge,
	}, nil
}
