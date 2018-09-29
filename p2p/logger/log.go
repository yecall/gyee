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
	"runtime"
	golog "log"
	logg "github.com/yeeco/gyee/utils/logging"
)


//
// Init logger switchs
//

var (
	Log_DisableAll			bool
	Log_DisableGoLog		bool
	Log_DisableFileLine		bool
	Log_DisableGyeeLog		bool
)

func init() {
	Log_DisableAll		= false
	Log_DisableGoLog	= false
	Log_DisableFileLine = false
	Log_DisableGyeeLog	= true
}

//
// Log the caller's file and line when this function called
//
func LogCallerFileLine(format string, args ... interface{}) {

	if Log_DisableAll { return }

	_, file, line, _ := runtime.Caller(1)

	if !Log_DisableGoLog {

		golog.Printf(format, args...)

		if !Log_DisableFileLine {
			golog.Printf("file: %s, line: %d", file, line)
		}

	} else if !Log_DisableGyeeLog {

		logg.Logger.Infof(format, args...)

		if !Log_DisableFileLine {
			logg.Logger.Infof("file: %s, line: %d", file, line)
		}
	}
}
