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


package dht

import (
	sch 	"github.com/yeeco/gyee/p2p/scheduler"
	log		"github.com/yeeco/gyee/p2p/logger"
)

//
// DHT manager
//
const DhtMgrName = "DhtMgr"

type DhtManager struct {
	sdl		*sch.Scheduler		// pointer to scheduler
	name	string				// name
	tep		sch.SchUserTaskEp	// entry
}

//
// Create DHT manager
//
func NewDhtMgr() *DhtManager {
	var dhtMgr = DhtManager{
		name: DhtMgrName,
	}

	dhtMgr.tep = dhtMgr.dhtMgrProc
	return &dhtMgr
}

//
// Entry point exported to shceduler
//
func (dhtMgr *DhtManager)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return dhtMgr.tep(ptn, msg)
}

//
// Table manager entry
//
func (dhtMgr *DhtManager)dhtMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	var eno = sch.SchEnoUnknown

	switch msg.Id {
	case sch.EvSchPoweron:
		eno = dhtMgr.dhtMgrPoweron(ptn)
	case sch.EvSchPoweroff:
		eno = dhtMgr.dhtMgrPoweroff(ptn)
	default:
	}

	return eno
}

//
//
// Poweron handler
func (dhtMgr *DhtManager)dhtMgrPoweron(ptn interface{}) sch.SchErrno {
	return sch.SchEnoNone
}

//
// poweroff handler
//
func (dhtMgr *DhtManager)dhtMgrPoweroff(ptn interface{}) sch.SchErrno {
	log.LogCallerFileLine("dhtMgrPoweroff: task will be done")
	sdl := sch.SchGetScheduler(ptn)
	return sdl.SchTaskDone(ptn, sch.SchEnoKilled)
}


