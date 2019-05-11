/*
 *  Copyright (C) 2017 gyee authors
 *
 *  This file is part of the gyee library.
 *
 *  The gyee library is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  The gyee library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with the gyee library.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package logging

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/lestrrat-go/file-rotatelogs"
	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
)

var Logger *logrus.Logger

func init() {
	Logger = logrus.New()
	Logger.SetOutput(os.Stdout)
	Logger.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
	Logger.SetLevel(logrus.InfoLevel)
}

func SetRotationFileLogger(logPath string) {
	if len(Logger.Hooks) > 0 {
		panic("Logger hooks exceeded")
	}
	rotateWriter, err := newFileRotateWriter(logPath)
	if err != nil {
		panic("failed to create rotate logs" + err.Error())
	}
	Logger.Out = rotateWriter
	Logger.Hooks.Add(newConsoleHooker())
	Logger.SetLevel(logrus.DebugLevel)
}

func newConsoleHooker() logrus.Hook {
	return lfshook.NewHook(lfshook.WriterMap{
		logrus.WarnLevel:  os.Stdout,
		logrus.ErrorLevel: os.Stdout,
		logrus.FatalLevel: os.Stdout,
	}, nil)
}

func newFileRotateWriter(logPath string) (io.Writer, error) {
	var err error
	if len(logPath) == 0 {
		return nil, fmt.Errorf("failed to parse logger folder: %s", logPath)
	}
	if !filepath.IsAbs(logPath) {
		logPath, err = filepath.Abs(logPath)
		if err != nil {
			return nil, err
		}
	}
	if err = os.MkdirAll(logPath, 0700); err != nil {
		return nil, fmt.Errorf("failed to create logger folder: %v", err)
	}
	return newRotateLogs(logPath, "yee")
}

func newRotateLogs(logpath, prefix string) (*rotatelogs.RotateLogs, error) {
	filePath := path.Join(logpath, prefix+"-%Y%m%d-%H%M.log")
	linkPath := path.Join(logpath, prefix+".log")
	return rotatelogs.New(
		filePath,
		rotatelogs.WithLinkName(linkPath),
		rotatelogs.WithMaxAge(30*24*time.Hour), // max age for clean up
		rotatelogs.WithRotationTime(time.Hour),
	)
}
