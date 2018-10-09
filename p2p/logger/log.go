/*
 *  Copyright (C) 2017 gyee authors
 *
 *  This file is part of the gyee library.
 *
 *  the gyee library is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  the gyee library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with the gyee library.  If not, see <http://www.gnu.org/licenses/>.
 *
 */


package logger

import (
	"os"
	"time"
	"fmt"
	"log"
	"runtime"
	"path/filepath"
	"github.com/lestrrat-go/file-rotatelogs"
	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
)

type P2pLogger struct {
	logger				*logrus.Logger	// global or local logger
	Module				string			// module name
	Level				uint32			// logger level
	Global				bool			// if global logger applied
	LogPosition			bool			// if caller's file and line needed
}

var (
	globalLogger *logrus.Logger			// the global logger
)

func init() {
	globalLogger = logrus.New()
	globalLogger.Out = os.Stdout
	globalLogger.Formatter = &logrus.TextFormatter{FullTimestamp: true}
	globalLogger.Level = logrus.InfoLevel
}

func NewP2pLogger(module string, level uint32, isGlobal bool, isPosition bool) *P2pLogger {
	logger := P2pLogger {
		Module: module,
		Level: level,
		Global:	isGlobal,
		LogPosition: isPosition,
	}
	if isGlobal {
		logger.logger = globalLogger
	} else {
		logger.logger = logrus.New()
		logger.logger.Out = os.Stdout
		logger.logger.Formatter = &logrus.TextFormatter{FullTimestamp: true}
		logger.logger.Level = logrus.Level(level)
	}
	return &logger
}

func (p2pLog *P2pLogger)getCallerFileLine() (string, int) {
	_, file, line, _ := runtime.Caller(1)
	return file, line
}

func LogCallerFileLine(format string, args ... interface{}) {
	_, file, line, _ := runtime.Caller(1)
	text := fmt.Sprintf(format, args...)
	fileLine := fmt.Sprintf("file: %s, line: %d", file, line)
	textAndFileLine := fmt.Sprintf("%s\n%s", text, fileLine)

	// Seems globalLogger.Printf not work with "\n" or "\r\n" to return and get a new line,
	// but log.Printf does work.

	// globalLogger.Printf("%s", textAndFileLine)
	log.Printf("%s", textAndFileLine)
}

func (p2pLog *P2pLogger)SetFileRotationHooker(path string, count uint) {
	frHook := p2pLog.newFileRotateHooker(path, count)
	p2pLog.logger.Hooks.Add(frHook)
}

func (p2pLog *P2pLogger)newFileRotateHooker(path string, count uint) logrus.Hook {
	if len(path) == 0 {
		panic("Failed to parse logger folder:" + path + ".")
	}
	if !filepath.IsAbs(path) {
		path, _ = filepath.Abs(path)
	}
	if err := os.MkdirAll(path, 0700); err != nil {
		panic("Failed to create logger folder:" + path + ". err:" + err.Error())
	}
	filePath := path + "/yee-%Y%m%d-%H.log"
	linkPath := path + "/yee.log"
	writer, err := rotatelogs.New(
		filePath,
		rotatelogs.WithLinkName(linkPath),
		rotatelogs.WithRotationTime(time.Duration(24)*time.Hour),
		rotatelogs.WithRotationCount(count),
	)

	if err != nil {
		panic("Failed to create rotate logs. err:" + err.Error())
	}

	hook := lfshook.NewHook(lfshook.WriterMap{
		logrus.DebugLevel: writer,
		logrus.InfoLevel:  writer,
		logrus.WarnLevel:  writer,
		logrus.ErrorLevel: writer,
		logrus.FatalLevel: writer,
	}, nil)
	return hook
}
