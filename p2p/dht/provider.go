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
	log "github.com/yeeco/gyee/p2p/logger"
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
	name		string					// my name
	tep			sch.SchUserTaskEp		// task entry
	ptnMe		interface{}				// pointer to task node of myself
	ptnDhtMgr	interface{}				// pointer to dht manager task node
	ptnQryMgr	interface{}				// pointer to query manager task node
	ds			Datastore				// data store
}

//
// Create provider manager
//
func NewPrdMgr() *PrdMgr {

	prdMgr := PrdMgr{
		name:		PrdMgrName,
		tep:		nil,
		ptnMe:		nil,
		ptnDhtMgr:	nil,
		ptnQryMgr:	nil,
		ds:			nil,
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

	if ptn == nil || msg == nil {
		log.LogCallerFileLine("prdMgrProc: invalid parameters")
		return DhtEnoParameter
	}

	eno := sch.SchEnoUnknown

	switch msg.Id {

	case sch.EvSchPoweron:
		eno = prdMgr.poweron(ptn)

	case sch.EvSchPoweroff:
		eno = prdMgr.poweroff(ptn)

	case sch.EvDhtPrdMgrCleanupTimer:
		eno = prdMgr.cleanupTimer()

	case sch.EvDhtPrdMgrAddProviderReq:
		eno = prdMgr.addProviderReq(msg.Body.(*sch.MsgDhtPrdMgrAddProviderReq))

	case sch.EvDhtQryMgrQueryResultInd:
		eno = prdMgr.qryMgrQueryResultInd(msg.Body.(*sch.MsgDhtQryMgrQueryResultInd))

	case sch.EvDhtPrdMgrPutProviderReq:
		eno = prdMgr.putProviderReq(msg.Body.(*sch.MsgDhtPrdMgrPutProviderReq))

	case sch.EvDhtPrdMgrGetProviderReq:
		eno = prdMgr.getProviderReq(msg.Body.(*sch.MsgDhtPrdMgrGetProviderReq))

	case sch.EvDhtPrdMgrGetProviderRsp:
		eno = prdMgr.getProviderRsp(msg.Body.(*sch.MsgDhtPrdMgrGetProviderRsp))

	default:
		eno = sch.SchEnoParameter
		log.LogCallerFileLine("prdMgrProc: unknown message: %d", msg.Id)
	}

	return eno
}

//
// power on handler
//
func (prdMgr *PrdMgr)poweron(ptn interface{}) sch.SchErrno {
	return sch.SchEnoNone
}

//
// power off handler
//
func (prdMgr *PrdMgr)poweroff(ptn interface{}) sch.SchErrno {
	return sch.SchEnoNone
}

//
// cleanup timer handler
//
func (prdMgr *PrdMgr)cleanupTimer() sch.SchErrno {
	return sch.SchEnoNone
}

//
// add provider request handler
//
func (prdMgr *PrdMgr)addProviderReq(msg *sch.MsgDhtPrdMgrAddProviderReq) sch.SchErrno {
	return sch.SchEnoNone
}

//
// qryMgr query result indication handler
//
func (prdMgr *PrdMgr)qryMgrQueryResultInd(msg *sch.MsgDhtQryMgrQueryResultInd) sch.SchErrno {
	return sch.SchEnoNone
}

//
// put provider request handler
//
func (prdMgr *PrdMgr)putProviderReq(msg *sch.MsgDhtPrdMgrPutProviderReq) sch.SchErrno {
	return sch.SchEnoNone
}

//
// get provider handler
//
func (prdMgr *PrdMgr)getProviderReq(msg *sch.MsgDhtPrdMgrGetProviderReq) sch.SchErrno {
	return sch.SchEnoNone
}

//
// get provder response handler
//
func (prdMgr *PrdMgr)getProviderRsp(msg *sch.MsgDhtPrdMgrGetProviderRsp) sch.SchErrno {
	return sch.SchEnoNone
}
