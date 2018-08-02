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
// Dht manager name registered in scheduler
//
const DhtMgrName = sch.DhtMgrName

//
// errno for route manager
//
type DhtErrno int

const (
	DhtEnoNone	= iota
	DhtEnoParameter
	DhtEnoScheduler
	DhtEnoNotFound
	DhtEnoResource
	DhtEnoUnknown
)

//
// Dht manager control block
//
type DhtMgr struct {
	name	string				// my name
	tep		sch.SchUserTaskEp	// task entry
	ptnMe	interface{}			// pointer to task node of myself
}

//
// Create DHT manager
//
func NewDhtMgr() *DhtMgr {

	dhtMgr := DhtMgr{
		name:	DhtMgrName,
		tep:	nil,
		ptnMe:	nil,
	}

	dhtMgr.tep = dhtMgr.dhtMgrProc

	return &dhtMgr
}

//
// Entry point exported to shceduler
//
func (dhtMgr *DhtMgr)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return dhtMgr.tep(ptn, msg)
}

//
// DHT manager entry
//
func (dhtMgr *DhtMgr)dhtMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	var eno = sch.SchEnoUnknown

	switch msg.Id {
	case sch.EvSchPoweron:
	case sch.EvSchPoweroff:
	case sch.EvDhtMgrFindPeerReq:
	case sch.EvDhtMgrPutProviderReq:
	case sch.EvDhtMgrGetProviderReq:
	case sch.EvDhtMgrPutValueReq:
	case sch.EvDhtMgrGetValueReq:
	case sch.EvDhtRutMgrNearestRsp:
	case sch.EvDhtQryMgrQueryStartRsp:
	case sch.EvDhtQryMgrQueryStopRsp:
	case sch.EvDhtQryMgrQueryResultInd:
	case sch.EvDhtPrdMgrGetProviderRsp:
	case sch.EvDhtPrdMgrAddProviderRsp:
	case sch.EvDhtConMgrConnectRsp:
	case sch.EvDhtConMgrSendRsp:
	case sch.EvDhtConMgrCloseRsp:
	default:
		eno = sch.SchEnoParameter
	}

	return eno
}