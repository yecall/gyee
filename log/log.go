// Copyright (C) 2019 gyee authors
//
// This file is part of the gyee library.
//
// The gyee library is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The gyee library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with the gyee library.  If not, see <http://www.gnu.org/licenses/>.

package log

import (
	"github.com/yeeco/gyee/utils/logging"
)

//
// logging API for geth code
//

// Trace is a convenient alias for Root().Trace
func Trace(msg string, ctx ...interface{}) {
	logging.Logger.Trace(msg, ctx)
}

// Debug is a convenient alias for Root().Debug
func Debug(msg string, ctx ...interface{}) {
	logging.Logger.Debug(msg, ctx)
}

// Info is a convenient alias for Root().Info
func Info(msg string, ctx ...interface{}) {
	logging.Logger.Info(msg, ctx)
}

// Warn is a convenient alias for Root().Warn
func Warn(msg string, ctx ...interface{}) {
	logging.Logger.Warn(msg, ctx)
}

// Error is a convenient alias for Root().Error
func Error(msg string, ctx ...interface{}) {
	logging.Logger.Error(msg, ctx)
}

// Crit is a convenient alias for Root().Crit
func Crit(msg string, ctx ...interface{}) {
	logging.Logger.Fatal(msg, ctx)
}

//
// logging API with printf format
//

func Debugf(fmt string, args ...interface{}) {
	logging.Logger.Debugf(fmt, args)
}

func Infof(fmt string, args ...interface{}) {
	logging.Logger.Infof(fmt, args)
}

func Warnf(fmt string, args ...interface{}) {
	logging.Logger.Warnf(fmt, args)
}

func Errorf(fmt string, args ...interface{}) {
	logging.Logger.Errorf(fmt, args)
}
