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
	"sync"
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
	DhtEnoDatastore				// data store errors
	DhtEnoUnknown				// unknown
)

//
// Dht manager control block
//
type DhtMgr struct {
	sdl			*sch.Scheduler		// pointer to scheduler
	name		string				// my name
	tep			sch.SchUserTaskEp	// task entry
	ptnMe		interface{}			// pointer to task node of myself
	ptnQryMgr	interface{}			// pointer to task node of query manager
	ptnConMgr	interface{}			// pointer to task node of connection manager
	ptnRutMgr	interface{}			// pointer to task node of route manager
	ptnPrdMgr	interface{}			// pointer to task node of provider manager
	ptnDsMgr	interface{}			// pointer to task node of data store manager
	cbLock		sync.Mutex			// lock for callback to be installed/removed
	cbf			DhtCallback			// callback entry
}

//
// Callback type
//
type DhtCallback func(mid int, msg interface{})int

//
// Create DHT manager
//
func NewDhtMgr() *DhtMgr {

	dhtMgr := DhtMgr{
		sdl:		nil,
		name:		DhtMgrName,
		tep:		nil,
		ptnMe:		nil,
		ptnQryMgr:	nil,
		ptnConMgr:	nil,
		ptnRutMgr:	nil,
		ptnPrdMgr:	nil,
		ptnDsMgr:	nil,
		cbf:		nil,
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
		eno = dhtMgr.conMgrSendCfm(msg.Body.(*sch.MsgDhtConMgrSendCfm))

	case sch.EvDhtConMgrCloseReq:
		eno = dhtMgr.conMgrCloseReq(msg.Body.(*sch.MsgDhtConMgrCloseReq))

	case sch.EvDhtConMgrCloseRsp:
		eno = dhtMgr.conMgrCloseRsp(msg.Body.(*sch.MsgDhtConMgrCloseRsp))

	default:
		eno = sch.SchEnoParameter
		log.LogCallerFileLine("dhtMgrProc: invalid event, id: %d", msg.Id)
	}

	return eno
}

//
// power on handler
//
func (dhtMgr *DhtMgr)poweron(ptn interface{}) sch.SchErrno {

	sdl := sch.SchGetScheduler(ptn)
	dhtMgr.sdl = sdl

	dhtMgr.ptnMe = ptn
	_, dhtMgr.ptnQryMgr = sdl.SchGetTaskNodeByName(QryMgrName)
	_, dhtMgr.ptnConMgr = sdl.SchGetTaskNodeByName(ConMgrName)
	_, dhtMgr.ptnRutMgr = sdl.SchGetTaskNodeByName(RutMgrName)
	_, dhtMgr.ptnPrdMgr = sdl.SchGetTaskNodeByName(PrdMgrName)
	_, dhtMgr.ptnDsMgr = sdl.SchGetTaskNodeByName(DsMgrName)

	if dhtMgr.ptnQryMgr == nil ||
		dhtMgr.ptnConMgr == nil ||
		dhtMgr.ptnRutMgr == nil ||
		dhtMgr.ptnPrdMgr == nil ||
		dhtMgr.ptnDsMgr == nil {
		log.LogCallerFileLine("poweron: nil task pointers")
		return sch.SchEnoInternal
	}

	return sch.SchEnoNone
}

//
// power off handler
//
func (dhtMgr *DhtMgr)poweroff(ptn interface{}) sch.SchErrno {
	log.LogCallerFileLine("poweroff: task will be done ...")
	return dhtMgr.sdl.SchTaskDone(dhtMgr.ptnMe, sch.SchEnoKilled)
}

//
// find peer request handler
//
func (dhtMgr *DhtMgr)findPeerReq(msg *sch.MsgDhtQryMgrQueryStartReq) sch.SchErrno {
	if msg.ForWhat != MID_FINDNODE {
		log.LogCallerFileLine("findPeerReq: unknown what's for: %d", msg.ForWhat)
		return sch.SchEnoParameter
	}
	return dhtMgr.dispMsg(dhtMgr.ptnQryMgr, sch.EvDhtQryMgrQueryStartReq, msg)
}

//
// find peer response handler
//
func (dhtMgr *DhtMgr)findPeerRsp(msg *sch.MsgDhtQryMgrQueryResultInd) sch.SchErrno {
	if dhtMgr.cbf != nil {
		rc := dhtMgr.cbf(sch.EvDhtMgrFindPeerRsp, msg)
		log.LogCallerFileLine("qryMgrQueryStartRsp: callback return: %d", rc)
	}
	return sch.SchEnoNone
}

//
// qryMgr query start response handler
//
func (dhtMgr *DhtMgr)qryMgrQueryStartRsp(msg *sch.MsgDhtQryMgrQueryStartRsp) sch.SchErrno {
	if dhtMgr.cbf != nil {
		rc := dhtMgr.cbf(sch.EvDhtQryMgrQueryStartRsp, msg)
		log.LogCallerFileLine("qryMgrQueryStartRsp: callback return: %d", rc)
	}
	return sch.SchEnoNone
}

//
// qryMgr query stop request handler
//
func (dhtMgr *DhtMgr)qryMgrqueryStopReq(msg *sch.MsgDhtQryMgrQueryStopReq) sch.SchErrno {
	return dhtMgr.dispMsg(dhtMgr.ptnQryMgr, sch.EvDhtQryMgrQueryStopReq, msg)
}

//
// qryMgr query stop response handler
//
func (dhtMgr *DhtMgr)qryMgrQueryStopRsp(msg *sch.MsgDhtQryMgrQueryStopRsp) sch.SchErrno {
	if dhtMgr.cbf != nil {
		rc := dhtMgr.cbf(sch.EvDhtQryMgrQueryStopRsp, msg)
		log.LogCallerFileLine("qryMgrQueryStopRsp: callback return: %d", rc)
	}
	return sch.SchEnoNone
}

//
// conMgr send request handler
//
func (dhtMgr *DhtMgr)conMgrSendReq(msg *sch.MsgDhtConMgrSendReq) sch.SchErrno {
	return dhtMgr.dispMsg(dhtMgr.ptnConMgr, sch.EvDhtConMgrSendReq, msg)
}

//
// conMgr send confirm handler
//
func (dhtMgr *DhtMgr)conMgrSendCfm(msg *sch.MsgDhtConMgrSendCfm) sch.SchErrno {
	if dhtMgr.cbf != nil {
		rc := dhtMgr.cbf(sch.EvDhtConMgrSendCfm, msg)
		log.LogCallerFileLine("conMgrSendCfm: callback return: %d", rc)
	}
	return sch.SchEnoNone
}

//
// put provider request handler
//
func (dhtMgr *DhtMgr)putProviderReq(msg *sch.MsgDhtPrdMgrAddProviderReq) sch.SchErrno {
	return dhtMgr.dispMsg(dhtMgr.ptnPrdMgr, sch.EvDhtPrdMgrAddProviderReq, msg)
}

//
// put provider response handler
//
func (dhtMgr *DhtMgr)putProviderRsp(msg *sch.MsgDhtPrdMgrAddProviderRsp) sch.SchErrno {
	if dhtMgr.cbf != nil {
		rc := dhtMgr.cbf(sch.EvDhtMgrPutProviderRsp, msg)
		log.LogCallerFileLine("putProviderRsp: callback return: %d", rc)
	}
	return sch.SchEnoNone
}

//
// get provider request handler
//
func (dhtMgr *DhtMgr)getProviderReq(msg *sch.MsgDhtMgrGetProviderReq) sch.SchErrno {
	return dhtMgr.dispMsg(dhtMgr.ptnPrdMgr, sch.EvDhtMgrGetProviderReq, msg)
}

//
// get provider response handler
//
func (dhtMgr *DhtMgr)getProviderRsp(msg *sch.MsgDhtMgrGetProviderRsp) sch.SchErrno {
	if dhtMgr.cbf != nil {
		rc := dhtMgr.cbf(sch.EvDhtMgrGetProviderRsp, msg)
		log.LogCallerFileLine("getProviderRsp: callback return: %d", rc)
	}
	return sch.SchEnoNone
}

//
// put value request handler
//
func (dhtMgr *DhtMgr)putValueReq(msg *sch.MsgDhtMgrPutValueReq) sch.SchErrno {
	return dhtMgr.dispMsg(dhtMgr.ptnDsMgr, sch.EvDhtMgrPutValueReq, msg)
}

//
// put value response handler
//
func (dhtMgr *DhtMgr)putValueRsp(msg *sch.MsgDhtMgrPutValueRsp) sch.SchErrno {
	if dhtMgr.cbf != nil {
		rc := dhtMgr.cbf(sch.EvDhtMgrPutValueRsp, msg)
		log.LogCallerFileLine("putValueRsp: callback return: %d", rc)
	}
	return sch.SchEnoNone
}

//
// get value request handler
//
func (dhtMgr *DhtMgr)getValueReq(msg *sch.MsgDhtMgrGetValueReq) sch.SchErrno {
	return dhtMgr.dispMsg(dhtMgr.ptnDsMgr, sch.EvDhtMgrGetValueReq, msg)
}

//
// get value response handler
//
func (dhtMgr *DhtMgr)getValueRsp(msg *sch.MsgDhtMgrGetValueRsp) sch.SchErrno {
	if dhtMgr.cbf != nil {
		rc := dhtMgr.cbf(sch.EvDhtMgrGetValueRsp, msg)
		log.LogCallerFileLine("getValueRsp: callback return: %d", rc)
	}
	return sch.SchEnoNone
}

//
// conMgr connection close request handler
//
func (dhtMgr *DhtMgr)conMgrCloseReq(msg *sch.MsgDhtConMgrCloseReq) sch.SchErrno {
	return dhtMgr.dispMsg(dhtMgr.ptnConMgr, sch.EvDhtConMgrCloseReq, msg)
}

//
// conMgr connection close response handler
//
func (dhtMgr *DhtMgr)conMgrCloseRsp(msg *sch.MsgDhtConMgrCloseRsp) sch.SchErrno {
	if dhtMgr.cbf != nil {
		rc := dhtMgr.cbf(sch.EvDhtConMgrCloseRsp, msg)
		log.LogCallerFileLine("conMgrCloseRsp: callback return: %d", rc)
	}
	return sch.SchEnoNone
}

//
// install callback
//
func (dhtMgr *DhtMgr)InstallCallback(cbf DhtCallback) DhtErrno {

	dhtMgr.cbLock.Lock()
	defer dhtMgr.cbLock.Unlock()

	if dhtMgr.cbf != nil {
		log.LogCallerFileLine("DhtInstallCallback: " +
			"callback is not nil: %p, it will be overlapped",
			dhtMgr.cbf)
	}

	dhtMgr.cbf = cbf

	if dhtMgr.cbf == nil {
		log.LogCallerFileLine("DhtInstallCallback: it's a nil callback, old is removed")
	}

	return DhtEnoNone
}

//
// dispatch message to specific task
//
func (dhtMgr *DhtMgr)dispMsg(dstTask interface{}, event int, msg interface{}) sch.SchErrno {
	schMsg := sch.SchMessage{}
	dhtMgr.sdl.SchMakeMessage(&schMsg, dhtMgr.ptnMe, dstTask, event, msg)
	return dhtMgr.sdl.SchSendMessage(&schMsg)
}

//
// dht command
//
func (dhtMgr *DhtMgr)DhtCommand(cmd int, msg interface{}) sch.SchErrno {
	schMsg := sch.SchMessage{}
	dhtMgr.sdl.SchMakeMessage(&schMsg, &sch.RawSchTask, dhtMgr.ptnMe, cmd, msg)
	return dhtMgr.sdl.SchSendMessage(&schMsg)
}

