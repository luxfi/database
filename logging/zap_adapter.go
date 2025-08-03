// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package logging

import (
	"fmt"
	"go.uber.org/zap"
)

// ZapAdapter adapts zap.Logger to the database Logger interface
type ZapAdapter struct {
	logger *zap.Logger
}

// NewZapAdapter creates a new ZapAdapter
func NewZapAdapter(logger *zap.Logger) Logger {
	return &ZapAdapter{logger: logger}
}

func (z *ZapAdapter) Debug(format string, args ...interface{}) {
	z.logger.Debug(fmt.Sprintf(format, args...))
}

func (z *ZapAdapter) Info(format string, args ...interface{}) {
	z.logger.Info(fmt.Sprintf(format, args...))
}

func (z *ZapAdapter) Warn(format string, args ...interface{}) {
	z.logger.Warn(fmt.Sprintf(format, args...))
}

func (z *ZapAdapter) Error(format string, args ...interface{}) {
	z.logger.Error(fmt.Sprintf(format, args...))
}

func (z *ZapAdapter) Fatal(format string, args ...interface{}) {
	z.logger.Fatal(fmt.Sprintf(format, args...))
}
