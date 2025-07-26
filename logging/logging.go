// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package logging

import "fmt"

// Logger defines the logging interface used by database packages
type Logger interface {
	Debug(format string, args ...interface{})
	Info(format string, args ...interface{})
	Warn(format string, args ...interface{})
	Error(format string, args ...interface{})
	Fatal(format string, args ...interface{})
}

// NoLog is a no-op logger implementation
type NoLog struct{}

func (NoLog) Debug(format string, args ...interface{}) {}
func (NoLog) Info(format string, args ...interface{})  {}
func (NoLog) Warn(format string, args ...interface{})  {}
func (NoLog) Error(format string, args ...interface{}) {}
func (NoLog) Fatal(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

// NoLogger is a default no-op logger instance
var NoLogger = NoLog{}
