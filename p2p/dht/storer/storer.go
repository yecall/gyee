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


package storer

import (
	sch 	"github.com/yeeco/gyee/p2p/scheduler"
	log		"github.com/yeeco/gyee/p2p/logger"
)

//
// store manager
//
const DhtstMgrName = "DhtstMgr"

type DhtStorerManager struct {
	sdl		*sch.Scheduler		// pointer to scheduler
	name	string				// name
	tep		sch.SchUserTaskEp	// entry
}

//
// Create storer manager
//
func NewDhtstMgr() *DhtStorerManager {
	var dhtstMgr = DhtStorerManager{
		name: DhtstMgrName,
	}
	dhtstMgr.tep = dhtstMgr.dhtstMgrProc
	return &dhtstMgr
}

//
// Entry point exported to shceduler
//
func (dhtstMgr *DhtStorerManager)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return dhtstMgr.tep(ptn, msg)
}

//
// store manager entry
//
func (dhtstMgr *DhtStorerManager)dhtstMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	var eno = sch.SchEnoUnknown

	switch msg.Id {
	case sch.EvSchPoweron:
		eno = dhtstMgr.dhtstMgrPoweron(ptn)
	case sch.EvSchPoweroff:
		eno = dhtstMgr.dhtstMgrPoweroff(ptn)
	default:
	}

	return eno
}

//
//
// Poweron handler
func (dhtstMgr *DhtStorerManager)dhtstMgrPoweron(ptn interface{}) sch.SchErrno {
	return sch.SchEnoNone
}

//
// poweroff handler
//
func (dhtstMgr *DhtStorerManager)dhtstMgrPoweroff(ptn interface{}) sch.SchErrno {
	log.LogCallerFileLine("dhtstMgrPoweroff: task will be done")
	sdl := sch.SchGetScheduler(ptn)
	return sdl.SchTaskDone(ptn, sch.SchEnoKilled)
}



