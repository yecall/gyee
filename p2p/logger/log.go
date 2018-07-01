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
	golog "log"
	"runtime"
)


//
// Init logger switchs
//

var (
	Log_DisableFileLine	bool
)

func init() {
	Log_DisableFileLine = false
}

//
// Log the caller's file and line when this function called
//
func LogCallerFileLine(format string, args ... interface{}) {

	if !Log_DisableFileLine {

		_, file, line, _ := runtime.Caller(1)
		golog.Printf("yclog: " + format, args...)
		golog.Printf("yclog: file: %s, line: %d", file, line)
	}
}
