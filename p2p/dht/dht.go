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
// Dht manager name registered in scheduler
//
const DhtMgrName = sch.DhtMgrName

//
// errno for route manager
//
type DhtErrno int

const (
	DhtEnoNone	= iota			// none of errors
	DhtEnoParameter				// invalid parameters
	DhtEnoScheduler				// scheduler errors
	DhtEnoNotFound				// something not found
	DhtEnoDuplicated			// something duplicated
	DhtEnoMismatched			// status mismatched
	DhtEnoResource				// no more resource
	DhtEnoRoute					// route errors
	DhtEnoTimeout				// timeout
	DhtEnoInternal				// internal logical errors
	DhtEnoOs					// underlying operating system errors
	DhtEnoSerialization			// serialization errors
	DhtEnoProtocol				// protocol errors
	DhtEnoNotSup				// not supported
	DhtEnoUnknown				// unknown
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

	if ptn == nil || msg == nil {
		log.LogCallerFileLine("dhtMgrProc: invalid parameters")
		return DhtEnoParameter
	}

	var eno = sch.SchEnoUnknown

	switch msg.Id {

	case sch.EvSchPoweron:
		eno = dhtMgr.poweron(ptn)

	case sch.EvSchPoweroff:
		eno = dhtMgr.poweroff(ptn)

	case sch.EvDhtMgrFindPeerReq:
		eno = dhtMgr.findPeerReq(msg.Body.(*sch.MsgDhtQryMgrQueryStartReq))

	case sch.EvDhtMgrFindPeerRsp:
		eno = dhtMgr.findPeerRsp(msg.Body.(*sch.MsgDhtQryMgrQueryResultInd))

	case sch.EvDhtMgrPutProviderReq:
		eno = dhtMgr.putProviderReq(msg.Body.(*sch.MsgDhtPrdMgrAddProviderReq))

	case sch.EvDhtMgrPutProviderRsp:
		eno = dhtMgr.putProviderRsp(msg.Body.(*sch.MsgDhtPrdMgrAddProviderRsp))

	case sch.EvDhtMgrGetProviderReq:
		eno = dhtMgr.getProviderReq(msg.Body.(*sch.MsgDhtMgrGetProviderReq))

	case sch.EvDhtMgrGetProviderRsp:
		eno = dhtMgr.getProviderRsp(msg.Body.(*sch.MsgDhtMgrGetProviderRsp))

	case sch.EvDhtMgrPutValueReq:
		eno = dhtMgr.putValueReq(msg.Body.(*sch.MsgDhtMgrPutValueReq))

	case sch.EvDhtMgrPutValueRsp:
		eno = dhtMgr.putValueRsp(msg.Body.(*sch.MsgDhtMgrPutValueRsp))

	case sch.EvDhtMgrGetValueReq:
		eno = dhtMgr.getValueReq(msg.Body.(*sch.MsgDhtMgrGetValueReq))

	case sch.EvDhtMgrGetValueRsp:
		eno = dhtMgr.getValueRsp(msg.Body.(*sch.MsgDhtMgrGetValueRsp))

	case sch.EvDhtQryMgrQueryStartRsp:
		eno = dhtMgr.qryMgrQueryStartRsp(msg.Body.(*sch.MsgDhtQryMgrQueryStartRsp))

	case sch.EvDhtMgrQueryStopReq:
		eno = dhtMgr.qryMgrqueryStopReq(msg.Body.(*sch.MsgDhtQryMgrQueryStopReq))

	case sch.EvDhtQryMgrQueryStopRsp:
		eno = dhtMgr.qryMgrQueryStopRsp(msg.Body.(*sch.MsgDhtQryMgrQueryStopRsp))

	case sch.EvDhtConMgrSendReq:
		eno = dhtMgr.conMgrSendReq(msg.Body.(*sch.MsgDhtConMgrSendReq))

	case sch.EvDhtConMgrSendCfm:

	case sch.EvDhtConMgrCloseReq:
		eno = dhtMgr.conMgrCloseReq(msg.Body.(*sch.MsgDhtConMgrCloseReq))

	case sch.EvDhtConMgrCloseRsp:
		eno = dhtMgr.conMgrCloseRsp(msg.Body.(*sch.MsgDhtConMgrCloseRsp))

	default:
		eno = sch.SchEnoParameter
		log.LogCallerFileLine("")
	}

	return eno
}

//
// power on handler
//
func (dhtMgr *DhtMgr)poweron(ptn interface{}) sch.SchErrno {
	return sch.SchEnoNone
}

//
// power off handler
//
func (dhtMgr *DhtMgr)poweroff(ptn interface{}) sch.SchErrno {
	return sch.SchEnoNone
}

//
// find peer request handler
//
func (dhtMgr *DhtMgr)findPeerReq(msg *sch.MsgDhtQryMgrQueryStartReq) sch.SchErrno {
	return sch.SchEnoNone
}

//
// qryMgr query start response handler
//
func (dhtMgr *DhtMgr)qryMgrQueryStartRsp(msg *sch.MsgDhtQryMgrQueryStartRsp) sch.SchErrno {
	return sch.SchEnoNone
}

//
// qryMgr query stop response handler
//
func (dhtMgr *DhtMgr)qryMgrQueryStopRsp(msg *sch.MsgDhtQryMgrQueryStopRsp) sch.SchErrno {
	return sch.SchEnoNone
}

//
// put provider request handler
//
func (dhtMgr *DhtMgr)putProviderReq(msg *sch.MsgDhtPrdMgrAddProviderReq) sch.SchErrno {
	return sch.SchEnoNone
}

//
// put provider response handler
//
func (dhtMgr *DhtMgr)putProviderRsp(msg *sch.MsgDhtPrdMgrAddProviderRsp) sch.SchErrno {
	return sch.SchEnoNone
}

//
// get provider request handler
//
func (dhtMgr *DhtMgr)getProviderReq(msg *sch.MsgDhtMgrGetProviderReq) sch.SchErrno {
	return sch.SchEnoNone
}

//
// get provider response handler
//
func (dhtMgr *DhtMgr)getProviderRsp(msg *sch.MsgDhtMgrGetProviderRsp) sch.SchErrno {
	return sch.SchEnoNone
}

//
// put value request handler
//
func (dhtMgr *DhtMgr)putValueReq(msg *sch.MsgDhtMgrPutValueReq) sch.SchErrno {
	return sch.SchEnoNone
}

//
// put value response handler
//
func (dhtMgr *DhtMgr)putValueRsp(msg *sch.MsgDhtMgrPutValueRsp) sch.SchErrno {
	return sch.SchEnoNone
}

//
// get value request handler
//
func (dhtMgr *DhtMgr)getValueReq(msg *sch.MsgDhtMgrGetValueReq) sch.SchErrno {
	return sch.SchEnoNone
}

//
// get value response handler
//
func (dhtMgr *DhtMgr)getValueRsp(msg *sch.MsgDhtMgrGetValueRsp) sch.SchErrno {
	return sch.SchEnoNone
}

//
// conMgr connection close response handler
//
func (dhtMgr *DhtMgr)conMgrCloseRsp(msg *sch.MsgDhtConMgrCloseRsp) sch.SchErrno {
	return sch.SchEnoNone
}
