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


package retriver

import (
	sch 	"github.com/yeeco/gyee/p2p/scheduler"
	log		"github.com/yeeco/gyee/p2p/logger"
)

//
// retrive manager
//
const DhtreMgrName = "DhtreMgr"

type DhtRetriverManager struct {
	sdl		*sch.Scheduler		// pointer to scheduler
	name	string				// name
	tep		sch.SchUserTaskEp	// entry
}

//
// Create retriver manager
//
func NewDhtreMgr() *DhtRetriverManager {
	var dhtreMgr = DhtRetriverManager{
		name: DhtreMgrName,
	}
	dhtreMgr.tep = dhtreMgr.dhtreMgrProc
	return &dhtreMgr
}

//
// Entry point exported to shceduler
//
func (dhtreMgr *DhtRetriverManager)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return dhtreMgr.tep(ptn, msg)
}

//
// retrive manager entry
//
func (dhtreMgr *DhtRetriverManager)dhtreMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	var eno = sch.SchEnoUnknown

	switch msg.Id {
	case sch.EvSchPoweron:
		eno = dhtreMgr.dhtreMgrPoweron(ptn)
	case sch.EvSchPoweroff:
		eno = dhtreMgr.dhtreMgrPoweroff(ptn)
	default:
	}

	return eno
}

//
//
// Poweron handler
func (dhtreMgr *DhtRetriverManager)dhtreMgrPoweron(ptn interface{}) sch.SchErrno {
	return sch.SchEnoNone
}

//
// poweroff handler
//
func (dhtreMgr *DhtRetriverManager)dhtreMgrPoweroff(ptn interface{}) sch.SchErrno {
	log.LogCallerFileLine("dhtreMgrPoweroff: task will be done")
	sdl := sch.SchGetScheduler(ptn)
	return sdl.SchTaskDone(ptn, sch.SchEnoKilled)
}
