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

package shell


import (
	log "github.com/ethereum/go-ethereum/log"
	sch "github.com/yeeco/gyee/p2p/scheduler"
)


const dhtShMgrName = sch.DhtShMgrName

type dhtShellManager struct {
	sdl				*sch.Scheduler					// pointer to scheduler
	name			string							// my name
	tep				sch.SchUserTaskEp				// task entry
	ptnMe			interface{}						// pointer to task node of myself
}

//
// Create dht shell manager
//
func NewDhtShellMgr() *dhtShellManager {
	shMgr := dhtShellManager {
		name: dhtShMgrName,
	}
	shMgr.tep = nil
	return &shMgr
}

//
// Entry point exported to scheduler
//
func (shMgr *dhtShellManager)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return shMgr.tep(ptn, msg)
}

//
// Shell manager entry
//
func (shMgr *dhtShellManager)shMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	eno := sch.SchEnoUnknown
	switch msg.Id {
	case sch.EvSchPoweron:
	case sch.EvSchPoweroff:
	default:
		log.Debug("shMgrProc: unknown event: %d", msg.Id)
		eno = sch.SchEnoParameter
	}
	return eno
}


