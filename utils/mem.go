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

package utils

import (
	"fmt"
	"runtime"
)

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tMallocs = %v MiB", bToMb(m.Mallocs))
	fmt.Printf("\tFrees = %v MiB", bToMb(m.Frees))
	fmt.Printf("\tHeapObjects = %v", m.HeapObjects)
	fmt.Printf("\tPauseTotalNs = %v", m.PauseTotalNs)
	fmt.Printf("\tNumGC = %v", m.NumGC)
	fmt.Printf("\tNumGoroutine = %v\n", runtime.NumGoroutine())
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
