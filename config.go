package kineticaotelexporter // import

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"

	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"gopkg.in/yaml.v3"
)

const (
	// The value of "type" key in configuration.
	typeStr = "kinetica"
	// The stability level of the exporter.
	stability = component.StabilityLevelAlpha

	maxSize    = 10
	maxBackups = 5
	maxAge     = 10
)

var (
	kineticaLogger *zap.Logger
)

// Config defines configuration for the Kinetica exporter.
type Config struct {
	Host               string `mapstructure:"host"`
	Schema             string `mapstructure:"schema"`
	Username           string `mapstructure:"username"`
	Password           string `mapstructure:"password"`
	BypassSslCertCheck bool   `mapstructure:"bypasssslcertcheck"`
}

// Validate the config
//
//	@receiver cfg
//	@return error
func (cfg *Config) Validate() error {
	kineticaHost, err := url.ParseRequestURI(cfg.Host)
	if err != nil {
		return err
	}
	if kineticaHost.Scheme != "http" && kineticaHost.Scheme != "https" {
		return errors.New("Protocol must be either `http` or `https`")
	}

	return nil
}

func parseNumber(s string, fallback int) int {
	v, err := strconv.Atoi(s)
	if err == nil {
		return v
	}
	return fallback
}

func (cfg *Config) createLogger() *zap.Logger {

	if kineticaLogger != nil {
		return kineticaLogger
	}

	var logConfig zap.Config

	yamlFile, _ := os.ReadFile("./config_log_zap.yaml")
	if err := yaml.Unmarshal(yamlFile, &logConfig); err != nil {
		return cfg.createDefaultLogger()
	}

	var (
		stdout      zapcore.WriteSyncer
		file        zapcore.WriteSyncer
		logFilePath *url.URL
	)

	for _, path := range logConfig.OutputPaths {

		if path == "stdout" || path == "stderror" {
			stdout = zapcore.AddSync(os.Stdout)

		} else if strings.HasPrefix(path, "lumberjack://") {
			var err error
			logFilePath, err = url.Parse(path)
			if err == nil {
				filename := strings.TrimLeft(logFilePath.Path, "/")
				if filename != "" {
					q := logFilePath.Query()
					l := &lumberjack.Logger{
						Filename:   filename,
						MaxSize:    parseNumber(q.Get("maxSize"), maxSize),
						MaxAge:     parseNumber(q.Get("maxAge"), maxAge),
						MaxBackups: parseNumber(q.Get("maxBackups"), maxBackups),
						LocalTime:  false,
						Compress:   false,
					}

					file = zapcore.AddSync(l)
				}
			}
		} else {
			// Unknown output format
			fmt.Println("Invalid output path specified in config ...")
		}
	}

	if stdout == nil && file == nil {
		return cfg.createDefaultLogger()
	}

	level := zap.NewAtomicLevelAt(logConfig.Level.Level())

	consoleEncoder := zapcore.NewConsoleEncoder(logConfig.EncoderConfig)
	fileEncoder := zapcore.NewJSONEncoder(logConfig.EncoderConfig)

	core := zapcore.NewTee(
		zapcore.NewCore(consoleEncoder, stdout, level),
		zapcore.NewCore(fileEncoder, file, level),
	)

	return zap.New(core)
}

// createDefaultLogger
//
//	@receiver cfg
//	@return *zap.Logger
func (cfg *Config) createDefaultLogger() *zap.Logger {
	stdout := zapcore.AddSync(os.Stdout)

	file := zapcore.AddSync(&lumberjack.Logger{
		Filename:   "./logs/kinetica-exporter.log",
		MaxSize:    maxSize, // megabytes
		MaxBackups: maxBackups,
		MaxAge:     maxAge, // days
	})

	level := zap.NewAtomicLevelAt(zap.InfoLevel)

	productionCfg := zap.NewProductionEncoderConfig()
	productionCfg.NameKey = "logger"
	productionCfg.TimeKey = "ts"
	productionCfg.MessageKey = "msg"
	productionCfg.StacktraceKey = "stacktrace"
	productionCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	developmentCfg := zap.NewDevelopmentEncoderConfig()
	developmentCfg.NameKey = "logger"
	developmentCfg.TimeKey = "ts"
	developmentCfg.MessageKey = "msg"
	developmentCfg.StacktraceKey = "stacktrace"
	developmentCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	developmentCfg.EncodeLevel = zapcore.CapitalColorLevelEncoder

	consoleEncoder := zapcore.NewConsoleEncoder(developmentCfg)
	fileEncoder := zapcore.NewJSONEncoder(productionCfg)

	core := zapcore.NewTee(
		zapcore.NewCore(consoleEncoder, stdout, level),
		zapcore.NewCore(fileEncoder, file, level),
	)

	return zap.New(core)
}

var _ component.Config = (*Config)(nil)
