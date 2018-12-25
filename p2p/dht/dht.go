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
	"fmt"
	"sync"
	config	"github.com/yeeco/gyee/p2p/config"
	sch		"github.com/yeeco/gyee/p2p/scheduler"
	p2plog	"github.com/yeeco/gyee/p2p/logger"
)


//
// debug
//
type dhtLogger struct {
	debug__		bool
}

var dhtLog = dhtLogger {
	debug__:	false,
}

func (log dhtLogger)Debug(fmt string, args ... interface{}) {
	if log.debug__ {
		p2plog.Debug(fmt, args ...)
	}
}

//
// stand alone for TEST when it's true
//
const _TEST_ = false

//
// Dht manager name registered in scheduler
//
const DhtMgrName = sch.DhtMgrName

//
// errno for route manager
//
type DhtErrno int

const (
	DhtEnoNone	DhtErrno = iota	// none of errors
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
	DhtEnoTimer					// timer errors
	DhtEnoUnknown				// unknown
)

func (eno DhtErrno)Error() string {
	return fmt.Sprintf("eno: %d", eno)
}

func (eno DhtErrno)GetEno() int {
	return int(eno)
}

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
	ptnShMgr	interface{}			// pointer to task node of dht shell manager
	cbLock		sync.Mutex			// lock for callback to be installed/removed
	cbf			DhtCallback			// callback entry
}

//
// Callback type
//
type DhtCallback func(mgr interface{}, mid int, msg interface{})int

//
// Create DHT manager
//
func NewDhtMgr() *DhtMgr {
	dhtMgr := DhtMgr{
		name:		DhtMgrName,
	}
	dhtMgr.tep = dhtMgr.dhtMgrProc
	return &dhtMgr
}

//
// Entry point exported to scheduler
//
func (dhtMgr *DhtMgr)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return dhtMgr.tep(ptn, msg)
}

//
// DHT manager entry
//
func (dhtMgr *DhtMgr)dhtMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	if ptn == nil || msg == nil {
		dhtLog.Debug("dhtMgrProc: invalid parameters")
		return sch.SchEnoParameter
	}

	var eno = sch.SchEnoUnknown

	switch msg.Id {

	case sch.EvSchPoweron:
		eno = dhtMgr.poweron(ptn)

	case sch.EvSchPoweroff:
		eno = dhtMgr.poweroff(ptn)

	case sch.EvDhtBlindConnectReq:
		eno = dhtMgr.blindConnectReq(msg.Body.(*sch.MsgDhtBlindConnectReq))

	case sch.EvDhtBlindConnectRsp:
		eno = dhtMgr.blindConnectRsp(msg.Body.(*sch.MsgDhtBlindConnectRsp))

	case sch.EvDhtRutRefreshReq:
		eno = dhtMgr.rutRefreshReq()

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

	case sch.EvDhtConInstStatusInd:
		eno = dhtMgr.conInstStatusInd(msg.Body.(*sch.MsgDhtConInstStatusInd))

	default:
		eno = sch.SchEnoParameter
		dhtLog.Debug("dhtMgrProc: invalid event, id: %d", msg.Id)
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
	_, dhtMgr.ptnQryMgr = sdl.SchGetUserTaskNode(QryMgrName)
	_, dhtMgr.ptnConMgr = sdl.SchGetUserTaskNode(ConMgrName)
	_, dhtMgr.ptnRutMgr = sdl.SchGetUserTaskNode(RutMgrName)
	_, dhtMgr.ptnPrdMgr = sdl.SchGetUserTaskNode(PrdMgrName)
	_, dhtMgr.ptnDsMgr = sdl.SchGetUserTaskNode(DsMgrName)

	if _TEST_ == false {
		var eno sch.SchErrno
		eno, dhtMgr.ptnShMgr = sdl.SchGetUserTaskNode(sch.DhtShMgrName)
		if eno != sch.SchEnoNone || dhtMgr.ptnShMgr == nil {
			dhtLog.Debug("poweron: shell not found")
			return sch.SchEnoNotFound
		}
	}

	if dhtMgr.ptnQryMgr == nil ||
		dhtMgr.ptnConMgr == nil ||
		dhtMgr.ptnRutMgr == nil ||
		dhtMgr.ptnPrdMgr == nil ||
		dhtMgr.ptnDsMgr == nil {
		dhtLog.Debug("poweron: nil task pointers")
		return sch.SchEnoInternal
	}

	return sch.SchEnoNone
}

//
// power off handler
//
func (dhtMgr *DhtMgr)poweroff(ptn interface{}) sch.SchErrno {
	dhtLog.Debug("poweroff: task will be done ...")
	return dhtMgr.sdl.SchTaskDone(dhtMgr.ptnMe, sch.SchEnoKilled)
}

//
// blind connect request
//
func (dhtMgr *DhtMgr)blindConnectReq(msg *sch.MsgDhtBlindConnectReq) sch.SchErrno {

	//
	// for blind connect request, no queries started, send connect request to
	// connection manager directly.
	//
	req := sch.MsgDhtConMgrConnectReq {
		Task:		dhtMgr.ptnMe,
		Peer:		msg.Peer,
		IsBlind:	true,
	}
	schMsg := sch.SchMessage{}
	dhtMgr.sdl.SchMakeMessage(&schMsg, dhtMgr.ptnMe, dhtMgr.ptnConMgr, sch.EvDhtConMgrConnectReq, &req)
	return dhtMgr.sdl.SchSendMessage(&schMsg)
}

//
// blind connect response
//
func (dhtMgr *DhtMgr)blindConnectRsp(msg *sch.MsgDhtBlindConnectRsp) sch.SchErrno {
	if dhtMgr.ptnShMgr != nil {
		ind := sch.MsgDhtShEventInd {
			Evt: sch.EvDhtBlindConnectRsp,
			Msg: msg,
		}
		schMsg := sch.SchMessage{}
		dhtMgr.sdl.SchMakeMessage(&schMsg, dhtMgr.ptnMe, dhtMgr.ptnShMgr, sch.EvDhtShEventInd, &ind)
		dhtMgr.sdl.SchSendMessage(&schMsg)
	} else if dhtMgr.cbf != nil {
		rc := dhtMgr.cbf(dhtMgr, sch.EvDhtBlindConnectRsp, msg)
		dhtLog.Debug("blindConnectRsp: callback return: %d", rc)
	}
	return sch.SchEnoNone
}

//
// request to resfresh route table
//
func (dhtMgr *DhtMgr)rutRefreshReq() sch.SchErrno {
	msg := sch.SchMessage{}
	dhtMgr.sdl.SchMakeMessage(&msg, dhtMgr.ptnMe, dhtMgr.ptnRutMgr, sch.EvDhtRutRefreshReq, nil)
	return dhtMgr.sdl.SchSendMessage(&msg)
}

//
// find peer request handler
//
func (dhtMgr *DhtMgr)findPeerReq(msg *sch.MsgDhtQryMgrQueryStartReq) sch.SchErrno {
	if msg.ForWhat != MID_FINDNODE {
		dhtLog.Debug("findPeerReq: unknown what's for: %d", msg.ForWhat)
		return sch.SchEnoParameter
	}
	return dhtMgr.dispMsg(dhtMgr.ptnQryMgr, sch.EvDhtQryMgrQueryStartReq, msg)
}

//
// find peer response handler
//
func (dhtMgr *DhtMgr)findPeerRsp(msg *sch.MsgDhtQryMgrQueryResultInd) sch.SchErrno {
	if dhtMgr.ptnShMgr != nil {
		ind := sch.MsgDhtShEventInd{
			Evt: sch.EvDhtMgrFindPeerRsp,
			Msg: msg,
		}
		schMsg := sch.SchMessage{}
		dhtMgr.sdl.SchMakeMessage(&schMsg, dhtMgr.ptnMe, dhtMgr.ptnShMgr, sch.EvDhtShEventInd, &ind)
		dhtMgr.sdl.SchSendMessage(&schMsg)
	} else if dhtMgr.cbf != nil {
		rc := dhtMgr.cbf(dhtMgr, sch.EvDhtMgrFindPeerRsp, msg)
		dhtLog.Debug("qryMgrQueryStartRsp: callback return: %d", rc)
	}
	return sch.SchEnoNone
}

//
// qryMgr query start response handler
//
func (dhtMgr *DhtMgr)qryMgrQueryStartRsp(msg *sch.MsgDhtQryMgrQueryStartRsp) sch.SchErrno {
	if dhtMgr.ptnShMgr != nil {
		ind := sch.MsgDhtShEventInd{
			Evt: sch.EvDhtQryMgrQueryStartRsp,
			Msg: msg,
		}
		schMsg := sch.SchMessage{}
		dhtMgr.sdl.SchMakeMessage(&schMsg, dhtMgr.ptnMe, dhtMgr.ptnShMgr, sch.EvDhtShEventInd, &ind)
		dhtMgr.sdl.SchSendMessage(&schMsg)
	} else if dhtMgr.cbf != nil {
		rc := dhtMgr.cbf(dhtMgr, sch.EvDhtQryMgrQueryStartRsp, msg)
		dhtLog.Debug("qryMgrQueryStartRsp: callback return: %d", rc)
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
	if dhtMgr.ptnShMgr != nil {
		ind := sch.MsgDhtShEventInd{
			Evt: sch.EvDhtQryMgrQueryStopRsp,
			Msg: msg,
		}
		schMsg := sch.SchMessage{}
		dhtMgr.sdl.SchMakeMessage(&schMsg, dhtMgr.ptnMe, dhtMgr.ptnShMgr, sch.EvDhtShEventInd, &ind)
		dhtMgr.sdl.SchSendMessage(&schMsg)
	} else if dhtMgr.cbf != nil {
		rc := dhtMgr.cbf(dhtMgr, sch.EvDhtQryMgrQueryStopRsp, msg)
		dhtLog.Debug("qryMgrQueryStopRsp: callback return: %d", rc)
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
	if dhtMgr.ptnShMgr != nil {
		ind := sch.MsgDhtShEventInd{
			Evt: sch.EvDhtConMgrSendCfm,
			Msg: msg,
		}
		schMsg := sch.SchMessage{}
		dhtMgr.sdl.SchMakeMessage(&schMsg, dhtMgr.ptnMe, dhtMgr.ptnShMgr, sch.EvDhtShEventInd, &ind)
		dhtMgr.sdl.SchSendMessage(&schMsg)
	} else if dhtMgr.cbf != nil {
		rc := dhtMgr.cbf(dhtMgr, sch.EvDhtConMgrSendCfm, msg)
		dhtLog.Debug("conMgrSendCfm: callback return: %d", rc)
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
	if dhtMgr.ptnShMgr != nil {
		ind := sch.MsgDhtShEventInd{
			Evt: sch.EvDhtMgrPutProviderRsp,
			Msg: msg,
		}
		schMsg := sch.SchMessage{}
		dhtMgr.sdl.SchMakeMessage(&schMsg, dhtMgr.ptnMe, dhtMgr.ptnShMgr, sch.EvDhtShEventInd, &ind)
		dhtMgr.sdl.SchSendMessage(&schMsg)
	} else if dhtMgr.cbf != nil {
		rc := dhtMgr.cbf(dhtMgr, sch.EvDhtMgrPutProviderRsp, msg)
		dhtLog.Debug("putProviderRsp: callback return: %d", rc)
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
	if dhtMgr.ptnShMgr != nil {
		ind := sch.MsgDhtShEventInd{
			Evt: sch.EvDhtMgrGetProviderRsp,
			Msg: msg,
		}
		schMsg := sch.SchMessage{}
		dhtMgr.sdl.SchMakeMessage(&schMsg, dhtMgr.ptnMe, dhtMgr.ptnShMgr, sch.EvDhtShEventInd, &ind)
		dhtMgr.sdl.SchSendMessage(&schMsg)
	} else if dhtMgr.cbf != nil {
		rc := dhtMgr.cbf(dhtMgr, sch.EvDhtMgrGetProviderRsp, msg)
		dhtLog.Debug("getProviderRsp: callback return: %d", rc)
	}
	return sch.SchEnoNone
}

//
// put value request handler
//
func (dhtMgr *DhtMgr)putValueReq(msg *sch.MsgDhtMgrPutValueReq) sch.SchErrno {
	req := sch.MsgDhtDsMgrAddValReq{
		Key: msg.Key,
		Val: msg.Val,
		KT: msg.KeepTime,
	}
	return dhtMgr.dispMsg(dhtMgr.ptnDsMgr, sch.EvDhtDsMgrAddValReq, &req)
}

//
// put value response handler
//
func (dhtMgr *DhtMgr)putValueRsp(msg *sch.MsgDhtMgrPutValueRsp) sch.SchErrno {
	if dhtMgr.ptnShMgr != nil {
		ind := sch.MsgDhtShEventInd{
			Evt: sch.EvDhtMgrPutValueRsp,
			Msg: msg,
		}
		schMsg := sch.SchMessage{}
		dhtMgr.sdl.SchMakeMessage(&schMsg, dhtMgr.ptnMe, dhtMgr.ptnShMgr, sch.EvDhtShEventInd, &ind)
		dhtMgr.sdl.SchSendMessage(&schMsg)
	} else if dhtMgr.cbf != nil {
		rc := dhtMgr.cbf(dhtMgr, sch.EvDhtMgrPutValueRsp, msg)
		dhtLog.Debug("putValueRsp: callback return: %d", rc)
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
	if dhtMgr.ptnShMgr != nil {
		ind := sch.MsgDhtShEventInd{
			Evt: sch.EvDhtMgrGetValueRsp,
			Msg: msg,
		}
		schMsg := sch.SchMessage{}
		dhtMgr.sdl.SchMakeMessage(&schMsg, dhtMgr.ptnMe, dhtMgr.ptnShMgr, sch.EvDhtShEventInd, &ind)
		dhtMgr.sdl.SchSendMessage(&schMsg)
	} else if dhtMgr.cbf != nil {
		rc := dhtMgr.cbf(dhtMgr, sch.EvDhtMgrGetValueRsp, msg)
		dhtLog.Debug("getValueRsp: callback return: %d", rc)
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
	if dhtMgr.ptnShMgr != nil {
		ind := sch.MsgDhtShEventInd{
			Evt: sch.EvDhtConMgrCloseRsp,
			Msg: msg,
		}
		schMsg := sch.SchMessage{}
		dhtMgr.sdl.SchMakeMessage(&schMsg, dhtMgr.ptnMe, dhtMgr.ptnShMgr, sch.EvDhtShEventInd, &ind)
		dhtMgr.sdl.SchSendMessage(&schMsg)
	} else if dhtMgr.cbf != nil {
		rc := dhtMgr.cbf(dhtMgr, sch.EvDhtConMgrCloseRsp, msg)
		dhtLog.Debug("conMgrCloseRsp: callback return: %d", rc)
	}
	return sch.SchEnoNone
}

//
// conInst status indication handler
//
func (dhtMgr *DhtMgr)conInstStatusInd(msg *sch.MsgDhtConInstStatusInd) sch.SchErrno {
	if dhtMgr.ptnShMgr != nil {
		ind := sch.MsgDhtShEventInd{
			Evt: sch.EvDhtConInstStatusInd,
			Msg: msg,
		}
		schMsg := sch.SchMessage{}
		dhtMgr.sdl.SchMakeMessage(&schMsg, dhtMgr.ptnMe, dhtMgr.ptnShMgr, sch.EvDhtShEventInd, &ind)
		dhtMgr.sdl.SchSendMessage(&schMsg)
	} else if dhtMgr.cbf != nil {
		rc := dhtMgr.cbf(dhtMgr, sch.EvDhtConInstStatusInd, msg)
		dhtLog.Debug("conInstStatusInd: callback return: %d", rc)
	}
	return sch.SchEnoNone
}

//
// install callback
//
func (dhtMgr *DhtMgr)InstallEventCallback(cbf DhtCallback) DhtErrno {

	if dhtMgr.ptnShMgr != nil {
		dhtLog.Debug("DhtInstallCallback: failed for shell presented")
		return DhtEnoMismatched
	}

	dhtMgr.cbLock.Lock()
	defer dhtMgr.cbLock.Unlock()

	if dhtMgr.cbf != nil {
		dhtLog.Debug("DhtInstallCallback: " +
			"callback is not nil: %p, it will be overlapped",
			dhtMgr.cbf)
	}

	dhtMgr.cbf = cbf

	if dhtMgr.cbf == nil {
		dhtLog.Debug("DhtInstallCallback: it's a nil callback, old is removed")
	}

	return DhtEnoNone
}

//
// get scheduler of manager
//
func (dhtMgr *DhtMgr)GetScheduler() *sch.Scheduler {
	return dhtMgr.sdl
}

//
// install rx data callback
//
func (dhtMgr *DhtMgr)InstallRxDataCallback(cbf ConInstRxDataCallback, peer *config.NodeID, dir ConInstDir) DhtErrno {

	//
	// this function exported for user to install callback for data received by connection
	// instance. to do this, one should check connection instance status in his callback
	// for event sch.EvDhtConInstStatusInd, for example, when "CisConnected" is reported.
	//

	conMgr, ok := dhtMgr.sdl.SchGetTaskObject(ConMgrName).(*ConMgr)
	if !ok {
		dhtLog.Debug("InstallRxDataCallback: connection manager not found")
		return DhtEnoMismatched
	}

	cid := conInstIdentity {
		nid:	*peer,
		dir:	dir,
	}
	cis := conMgr.lookupConInst(&cid)
	if len(cis) == 0 {
		dhtLog.Debug("InstallRxDataCallback: none of instances found")
		return DhtEnoNotFound
	}

	for idx, ci := range cis {
		if ci != nil {
			if eno := ci.InstallRxDataCallback(cbf); eno != DhtEnoNone {
				dhtLog.Debug("InstallRxDataCallback: "+
					"failed, idx: %d, eno: %d, name: %s",
					idx, eno, ci.name)
				return eno
			}
		}
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
