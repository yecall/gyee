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

//
// Provider manager name registered in scheduler
//
const PrdMgrName = sch.DhtPrdMgrName

//
// Provider manager
//
type PrdMgr struct {
	name	string				// my name
	tep		sch.SchUserTaskEp	// task entry
	ptnMe	interface{}			// pointer to task node of myself
}

//
// Create provider manager
//
func NewPrdMgr() *PrdMgr {

	prdMgr := PrdMgr{
		name:	PrdMgrName,
		tep:	nil,
		ptnMe:	nil,
	}

	prdMgr.tep = prdMgr.prdMgrProc

	return &prdMgr
}

//
// Entry point exported to shceduler
//
func (prdMgr *PrdMgr)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return prdMgr.tep(ptn, msg)
}

//
// Provider manager entry
//
func (prdMgr *PrdMgr)prdMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	eno := sch.SchEnoUnknown

	switch msg.Id {
	case sch.EvSchPoweron:
	case sch.EvSchPoweroff:
	case sch.EvDhtPrdMgrAddProviderReq:
	case sch.EvDhtPrdMgrGetProviderReq:
	case sch.EvDhtPrdMgrGetProviderRsp:
	case sch.EvDhtPrdMgrUpdateReq:
	default:
		eno = sch.SchEnoParameter
	}

	return eno
}