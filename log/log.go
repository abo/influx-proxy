package log

import (
	"io"
	golog "log"
	"os"
	"sync/atomic"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// // log level from kubernetes/klog: The practical default level is V(2). Developers and QE environments may wish to run at V(3) or V(4).
// const (
// 	// VError - Generally useful for this to ALWAYS be visible to an operator
// 	// * Programmer errors
// 	// * Logging extra info about a panic
// 	// * CLI argument handling
// 	VError = 1

// 	// VWarn - A reasonable default log level if you don't want verbosity.
// 	// * Information about config (listening on X, watching Y)
// 	// * Errors that repeat frequently that relate to conditions that can be corrected (pod detected as unhealthy)
// 	VWarn = 2

// 	// VInfo - Useful steady state information about the service and important log messages that may correlate to
// 	// significant changes in the system.  This is the recommended default log level for most systems.
// 	// * Logging HTTP requests and their exit code
// 	// * System state changing (killing pod)
// 	// * Controller state change events (starting pods)
// 	// * Scheduler log messages
// 	VInfo = 3

// 	// VExtInfo - Extended information about changes
// 	// * More info about system state changes
// 	VExtInfo = 4

// 	// VDebug - Debug level verbosity
// 	// * Logging in particularly thorny parts of code where you may want to come back later and check it
// 	VDebug = 5

// 	// VTrace - Trace level verbosity
// 	// * Context to understand the steps leading up to errors and warnings
// 	// * More information for troubleshooting reported issues
// 	VTrace = 6
// )

// Options for logging
type Options struct {
	Level      string `yaml:"level"`
	File       string `yaml:"file"` // if empty, output to stdout
	MaxSizeMB  int    `yaml:"file-max-size"`
	MaxAgeDays int    `yaml:"file-max-age"`
}

type Logger interface {
	Debug(v ...interface{})
	Debugf(format string, v ...interface{})
	Info(v ...interface{})
	Infof(format string, v ...interface{})
	Warn(v ...interface{})
	Warnf(format string, v ...interface{})
	Error(v ...interface{})
	Errorf(format string, v ...interface{})
	GoLogger() *golog.Logger
	Flush()
}

func init() {
	Init(Options{File: "", Level: "debug"})
}

var globalL atomic.Value
var zaplog func(name string) *zap.Logger

// Init initialize logger with options, and redirect stdlog to debug
func Init(opts Options) {
	var w io.Writer = os.Stdout
	if opts.File != "" {
		w = &lumberjack.Logger{
			Filename:  opts.File,
			MaxSize:   opts.MaxSizeMB,
			MaxAge:    opts.MaxAgeDays,
			LocalTime: true,
			Compress:  true,
		}
	}

	var level zapcore.Level
	if err := level.Set(opts.Level); err != nil {
		panic(err)
	}

	zaplog = func(name string) *zap.Logger {
		cfg := zap.NewProductionEncoderConfig()
		cfg.EncodeTime = zapcore.TimeEncoderOfLayout("2006-01-02T15:04:05")
		cfg.ConsoleSeparator = " "
		cfg.EncodeLevel = zapcore.CapitalLevelEncoder
		core := zapcore.NewCore(zapcore.NewConsoleEncoder(cfg), zapcore.AddSync(w), level)
		if name != "" {
			name += ":"
		}
		return zap.New(core).Named(name)
	}

	l := zaplog("")
	zap.RedirectStdLogAt(l, zap.DebugLevel)
	globalL.Store(l)
}

func Debugf(format string, v ...interface{}) { s().Debugf(format, v...) }
func Infof(format string, v ...interface{})  { s().Infof(format, v...) }
func Warnf(format string, v ...interface{})  { s().Warnf(format, v...) }
func Errorf(format string, v ...interface{}) { s().Errorf(format, v...) }
func Debug(v ...interface{})                 { s().Debug(v...) }
func Info(v ...interface{})                  { s().Info(v...) }
func Warn(v ...interface{})                  { s().Warn(v...) }
func Error(v ...interface{})                 { s().Error(v...) }
func Flush()                                 { l().Sync() }

func l() *zap.Logger {
	return globalL.Load().(*zap.Logger)
}

func s() *zap.SugaredLogger {
	return l().Sugar()
}

type logger struct {
	inner *zap.SugaredLogger
}

func New(name string) Logger {
	return &logger{zaplog(name).Sugar()}
}
func (l *logger) Debugf(format string, vals ...interface{}) { l.inner.Debugf(format, vals...) }
func (l *logger) Infof(format string, vals ...interface{})  { l.inner.Infof(format, vals...) }
func (l *logger) Warnf(format string, vals ...interface{})  { l.inner.Warnf(format, vals...) }
func (l *logger) Errorf(format string, vals ...interface{}) { l.inner.Errorf(format, vals...) }
func (l *logger) Debug(vals ...interface{})                 { l.inner.Debug(vals...) }
func (l *logger) Info(vals ...interface{})                  { l.inner.Info(vals...) }
func (l *logger) Warn(vals ...interface{})                  { l.inner.Warn(vals...) }
func (l *logger) Error(vals ...interface{})                 { l.inner.Error(vals...) }
func (l *logger) GoLogger() *golog.Logger                   { return golog.New(&levelPrefixWriter{l}, "", 0) }
func (l *logger) Flush()                                    { l.inner.Sync() }

type levelPrefixWriter struct{ l *logger }

func (o *levelPrefixWriter) Write(p []byte) (n int, err error) {
	switch string(p[:2]) {
	case "[I": // [INFO]
		o.l.Info(string(p[7:]))
	case "[W": // [WARN]
		o.l.Warn(string(p[7:]))
	case "[E": // [ERR]
		o.l.Error(string(p[6:]))
	case "[D": // [DEBUG]
		o.l.Debug(string(p[8:]))
	case "[T": // [TRACE]
		o.l.Debug(string(p[8:]))
	default:
		o.l.Info(string(p))
	}
	return len(p), nil
}

// func With(fields ...zap.Field) *zap.Logger { return l().With(fields...) }
