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


package chunker

import (
	sch 	"github.com/yeeco/gyee/p2p/scheduler"
	log		"github.com/yeeco/gyee/p2p/logger"
)

//
// Chunker manager
//
const DhtchMgrName = "DhtchMgr"

type DhtChunkerManager struct {
	sdl		*sch.Scheduler		// pointer to scheduler
	name	string				// name
	tep		sch.SchUserTaskEp	// entry
}

//
// Create chunker manager
//
func NewDhtchMgr() *DhtChunkerManager {
	var dhtchMgr = DhtChunkerManager {
		name: DhtchMgrName,
	}
	dhtchMgr.tep = dhtchMgr.dhtchMgrProc
	return &dhtchMgr
}

//
// Entry point exported to shceduler
//
func (dhtchMgr *DhtChunkerManager)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return dhtchMgr.tep(ptn, msg)
}

//
// Table manager entry
//
func (dhtchMgr *DhtChunkerManager)dhtchMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	var eno = sch.SchEnoUnknown

	switch msg.Id {
	case sch.EvSchPoweron:
		eno = dhtchMgr.dhtchMgrPoweron(ptn)
	case sch.EvSchPoweroff:
		eno = dhtchMgr.dhtchMgrPoweroff(ptn)
	default:
	}

	return eno
}

//
//
// Poweron handler
func (dhtchMgr *DhtChunkerManager)dhtchMgrPoweron(ptn interface{}) sch.SchErrno {
	return sch.SchEnoNone
}

//
// poweroff handler
//
func (dhtchMgr *DhtChunkerManager)dhtchMgrPoweroff(ptn interface{}) sch.SchErrno {
	log.LogCallerFileLine("dhtchMgrPoweroff: task will be done")
	sdl := sch.SchGetScheduler(ptn)
	return sdl.SchTaskDone(ptn, sch.SchEnoKilled)
}



