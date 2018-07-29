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
	sch	"github.com/yeeco/gyee/p2p/scheduler"
)

const LsnMgrName = sch.DhtLsnMgrName

type LsnMgr struct {
	name	string				// my name
	tep		sch.SchUserTaskEp	// task entry
	ptnMe	interface{}			// pointer to task node of myself
}

//
// Create listener manager
//
func NewLsnMgr() *LsnMgr {

	lsnMgr := LsnMgr {
		name:	LsnMgrName,
		tep:	nil,
		ptnMe:	nil,
	}

	lsnMgr.tep = lsnMgr.lsnMgrProc

	return &lsnMgr
}

//
// Entry point exported to shceduler
//
func (lsnMgr *LsnMgr)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return lsnMgr.tep(ptn, msg)
}

//
// Discover manager entry
//
func (lsnMgr *LsnMgr)lsnMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	eno := sch.SchEnoUnknown

	switch msg.Id {
	case sch.EvSchPoweron:
	case sch.EvSchPoweroff:
	case sch.EvDhtLsnMgrStartReq:
	case sch.EvDhtLsnMgrStopReq:
	case sch.EvDhtLsnMgrPauseReq:
	default:
		eno = sch.SchEnoParameter
	}

	return eno
}
