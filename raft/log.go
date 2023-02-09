package raft

import (
	"go.uber.org/zap"
)

type logAdapter struct {
	inner *zap.SugaredLogger
}

func newRaftLogger(l *zap.Logger) *logAdapter {
	return &logAdapter{l.Sugar()}
}

func (rl *logAdapter) Debug(v ...interface{}) {
	rl.inner.Debug(v...)

}
func (rl *logAdapter) Debugf(format string, v ...interface{}) {
	rl.inner.Debugf(format, v...)
}

func (rl *logAdapter) Error(v ...interface{}) {
	rl.inner.Error(v...)
}
func (rl *logAdapter) Errorf(format string, v ...interface{}) {
	rl.inner.Errorf(format, v...)
}

func (rl *logAdapter) Info(v ...interface{}) {
	rl.inner.Info(v...)
}
func (rl *logAdapter) Infof(format string, v ...interface{}) {
	rl.inner.Infof(format, v...)
}

func (rl *logAdapter) Warning(v ...interface{}) {
	rl.inner.Warn(v...)
}
func (rl *logAdapter) Warningf(format string, v ...interface{}) {
	rl.inner.Warnf(format, v...)
}

func (rl *logAdapter) Fatal(v ...interface{}) {
	rl.inner.Fatal(v...)
}
func (rl *logAdapter) Fatalf(format string, v ...interface{}) {
	rl.inner.Fatalf(format, v...)
}

func (rl *logAdapter) Panic(v ...interface{}) {
	rl.inner.Panic(v...)
}
func (rl *logAdapter) Panicf(format string, v ...interface{}) {
	rl.inner.Panicf(format, v...)
}
