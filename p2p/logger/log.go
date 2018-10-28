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
	gylog "github.com/yeeco/gyee/utils/logging"
)

type P2pLogger struct {
	logger				*logrus.Logger	// global or local logger
	Module				string			// module name
	Level				uint32			// logger level
	Global				bool			// if global logger applied
	LogPosition			bool			// if caller's file and line needed
}

var (
	Debug__ = true						// debug stage flag
	GyeeProject = true					// is playing in github.com/yeeco/gyee project
	GlobalLogger *logrus.Logger	= nil	// the global logger
)

func init() {
	if GyeeProject {
		// Notice: in the debug stage, the global debug level is forced to be DebugLevel
		// to output messages as most as possible(the default level is InfoLevel). This
		// might be conflicted with other module preference.
		GlobalLogger = gylog.Logger
		if Debug__ {
			GlobalLogger.Level = logrus.DebugLevel
		}
	} else {
		// If it's not playing in the gyee project, we create a new global logger than
		// that created in gyee logger package, see it please.
		GlobalLogger = logrus.New()
		GlobalLogger.Out = os.Stdout
		GlobalLogger.Formatter = &logrus.TextFormatter{FullTimestamp: true}
		GlobalLogger.Level = logrus.InfoLevel
	}
}

func NewP2pLogger(module string, level uint32, isGlobal bool, isPosition bool) *P2pLogger {
	logger := P2pLogger {
		Module: module,
		Level: level,
		Global:	isGlobal,
		LogPosition: isPosition,
	}
	if isGlobal {
		logger.logger = GlobalLogger
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

func Debug(format string, args ... interface{}) {
	// Notice: only applied in project DEBUG stage to output the caller file and line.
	// Use the real logger debug interface in normal cases please.
	if !Debug__ {
		msg := fmt.Sprintf(format, args...)
		GlobalLogger.Info(msg)
	} else {
		_, file, line, _ := runtime.Caller(1)
		text := fmt.Sprintf(format, args...)
		fileLine := fmt.Sprintf("file: %s, line: %d", file, line)
		textAndFileLine := fmt.Sprintf("%s\n%s", text, fileLine)
		// Seems GlobalLogger.Printf does not work with "\n" or "\r\n" to return and
		// get a new line, but log.Printf does.
		// GlobalLogger.Printf("%s", textAndFileLine)
		log.Printf("%s", textAndFileLine)
	}
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
