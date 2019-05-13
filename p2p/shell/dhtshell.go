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
	dht "github.com/yeeco/gyee/p2p/dht"
	p2plog "github.com/yeeco/gyee/p2p/logger"
	sch "github.com/yeeco/gyee/p2p/scheduler"
	log "github.com/yeeco/gyee/log"
)

//
// debug
//
type dhtShellLogger struct {
	debug__ bool
}

var dhtLog = dhtShellLogger{
	debug__: false,
}

func (log dhtShellLogger) Debug(fmt string, args ...interface{}) {
	if log.debug__ {
		p2plog.Debug(fmt, args...)
	}
}

const (
	dhtShMgrName     = sch.DhtShMgrName // name registered in scheduler
	ShMgrMailboxSize = 1024 * 8			// mailbox size
	dhtShEvQueueSize = 64               // event indication queue size
	dhtShCsQueueSize = 64               // connection status indication queue size
)

type DhtShellManager struct {
	sdl       *sch.Scheduler                   // pointer to scheduler
	name      string                           // my name
	tep       sch.SchUserTaskEp                // task entry
	ptnMe     interface{}                      // pointer to task node of myself
	ptnDhtMgr interface{}                      // pointer to dht manager task node
	evChan    chan *sch.MsgDhtShEventInd       // event indication channel
	csChan    chan *sch.MsgDhtConInstStatusInd // connection status indication channel
}

//
// Create dht shell manager
//
func NewDhtShellMgr() *DhtShellManager {
	shMgr := DhtShellManager{
		name:   dhtShMgrName,
		evChan: make(chan *sch.MsgDhtShEventInd, dhtShEvQueueSize),
		csChan: make(chan *sch.MsgDhtConInstStatusInd, dhtShCsQueueSize),
	}
	shMgr.tep = shMgr.shMgrProc
	return &shMgr
}

//
// Entry point exported to scheduler
//
func (shMgr *DhtShellManager) TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return shMgr.tep(ptn, msg)
}

//
// Shell manager entry
//
func (shMgr *DhtShellManager) shMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	dhtLog.Debug("shMgrProc: name: %s, msg.Id: %d", shMgr.name, msg.Id)

	eno := sch.SchEnoUnknown

	switch msg.Id {
	case sch.EvSchPoweron:
		eno = shMgr.poweron(ptn)

	case sch.EvSchPoweroff:
		eno = shMgr.poweroff(ptn)

	case sch.EvDhtShEventInd:
		eno = shMgr.dhtShEventInd(msg.Body.(*sch.MsgDhtShEventInd))

	case sch.EvDhtRutRefreshReq:
		eno = shMgr.dhtRutRefreshReq()

	case sch.EvDhtMgrFindPeerReq:
		eno = shMgr.dhtShFindPeerReq(msg.Body.(*sch.MsgDhtQryMgrQueryStartReq))

	case sch.EvDhtBlindConnectReq:
		eno = shMgr.dhtShBlindConnectReq(msg.Body.(*sch.MsgDhtBlindConnectReq))

	case sch.EvDhtMgrGetValueReq:
		eno = shMgr.dhtShGetValueReq(msg.Body.(*sch.MsgDhtMgrGetValueReq))

	case sch.EvDhtMgrPutValueReq:
		eno = shMgr.dhtShPutValueReq(msg.Body.(*sch.MsgDhtMgrPutValueReq))

	case sch.EvDhtMgrGetProviderReq:
		eno = shMgr.dhtShGetProviderReq(msg.Body.(*sch.MsgDhtMgrGetProviderReq))

	case sch.EvDhtMgrPutProviderReq:
		eno = shMgr.dhtShPutProviderReq(msg.Body.(*sch.MsgDhtPrdMgrAddProviderReq))

	default:
		dhtLog.Debug("shMgrProc: unknown event: %d", msg.Id)
		eno = sch.SchEnoParameter
	}

	dhtLog.Debug("shMgrProc: get out, name: %s, msg.Id: %d", shMgr.name, msg.Id)

	return eno
}

func (shMgr *DhtShellManager) poweron(ptn interface{}) sch.SchErrno {
	var eno sch.SchErrno
	shMgr.ptnMe = ptn
	shMgr.sdl = sch.SchGetScheduler(ptn)
	if eno, shMgr.ptnDhtMgr = shMgr.sdl.SchGetUserTaskNode(sch.DhtMgrName); eno != sch.SchEnoNone {
		dhtLog.Debug("poweron: dht manager task not found")
		return eno
	}
	return sch.SchEnoNone
}

func (shMgr *DhtShellManager) poweroff(ptn interface{}) sch.SchErrno {
	dhtLog.Debug("poweroff: task will be done...")
	close(shMgr.evChan)
	close(shMgr.csChan)
	return shMgr.sdl.SchTaskDone(shMgr.ptnMe, shMgr.name, sch.SchEnoPowerOff)
}

func (shMgr *DhtShellManager) dhtShEventInd(ind *sch.MsgDhtShEventInd) sch.SchErrno {

	dhtLog.Debug("dhtShEventInd: event type: %d", ind.Evt)

	evt := ind.Evt
	msg := ind.Msg
	eno := sch.SchEnoUnknown

	switch evt {

	case sch.EvDhtBlindConnectRsp:
		eno = shMgr.dhtBlindConnectRsp(msg.(*sch.MsgDhtBlindConnectRsp))

	case sch.EvDhtMgrFindPeerRsp:
		eno = shMgr.dhtMgrFindPeerRsp(msg.(*sch.MsgDhtQryMgrQueryResultInd))

	case sch.EvDhtQryMgrQueryStartRsp:
		eno = shMgr.dhtQryMgrQueryStartRsp(msg.(*sch.MsgDhtQryMgrQueryStartRsp))

	case sch.EvDhtQryMgrQueryStopRsp:
		eno = shMgr.dhtQryMgrQueryStopRsp(msg.(*sch.MsgDhtQryMgrQueryStopRsp))

	case sch.EvDhtConMgrSendCfm:
		eno = shMgr.dhtConMgrSendCfm(msg.(*sch.MsgDhtConMgrSendCfm))

	case sch.EvDhtMgrPutProviderRsp:
		eno = shMgr.dhtMgrPutProviderRsp(msg.(*sch.MsgDhtPrdMgrAddProviderRsp))

	case sch.EvDhtMgrGetProviderRsp:
		eno = shMgr.dhtMgrGetProviderRsp(msg.(*sch.MsgDhtMgrGetProviderRsp))

	case sch.EvDhtMgrPutValueLocalRsp:
		eno = shMgr.dhtMgrPutValueLocalRsp(msg.(*sch.MsgDhtMgrPutValueLocalRsp))

	case sch.EvDhtMgrPutValueRsp:
		eno = shMgr.dhtMgrPutValueRsp(msg.(*sch.MsgDhtMgrPutValueRsp))

	case sch.EvDhtMgrGetValueRsp:
		eno = shMgr.dhtMgrGetValueRsp(msg.(*sch.MsgDhtMgrGetValueRsp))

	case sch.EvDhtConMgrCloseRsp:
		eno = shMgr.dhtConMgrCloseRsp(msg.(*sch.MsgDhtConMgrCloseRsp))

	case sch.EvDhtConInstStatusInd:
		return shMgr.dhtConInstStatusInd(msg.(*sch.MsgDhtConInstStatusInd))

	default:
		dhtLog.Debug("dhtTestEventCallback: unknown event type: %d", evt)
		return sch.SchEnoParameter
	}

	dhtLog.Debug("dhtTestEventCallback: event put to channel, type: %d", evt)

	if eno == sch.SchEnoNone {
		shMgr.evChan <- ind
	}

	return eno
}

func (shMgr *DhtShellManager) dhtBlindConnectRsp(msg *sch.MsgDhtBlindConnectRsp) sch.SchErrno {
	dhtLog.Debug("dhtBlindConnectRsp: eno: %d", msg.Eno)
	return sch.SchEnoNone
}

func (shMgr *DhtShellManager) dhtMgrFindPeerRsp(msg *sch.MsgDhtQryMgrQueryResultInd) sch.SchErrno {
	dhtLog.Debug("dhtMgrFindPeerRsp: eno: %d", msg.Eno)
	return sch.SchEnoNone
}

func (shMgr *DhtShellManager) dhtQryMgrQueryStartRsp(msg *sch.MsgDhtQryMgrQueryStartRsp) sch.SchErrno {
	dhtLog.Debug("dhtQryMgrQueryStartRsp: eno: %d", msg.Eno)
	return sch.SchEnoNone
}

func (shMgr *DhtShellManager) dhtQryMgrQueryStopRsp(msg *sch.MsgDhtQryMgrQueryStopRsp) sch.SchErrno {
	dhtLog.Debug("dhtQryMgrQueryStopRsp: eno: %d", msg.Eno)
	return sch.SchEnoNone
}

func (shMgr *DhtShellManager) dhtConMgrSendCfm(msg *sch.MsgDhtConMgrSendCfm) sch.SchErrno {
	dhtLog.Debug("dhtConMgrSendCfm: eno: %d", msg.Eno)
	return sch.SchEnoNone
}

func (shMgr *DhtShellManager) dhtMgrPutProviderRsp(msg *sch.MsgDhtPrdMgrAddProviderRsp) sch.SchErrno {
	dhtLog.Debug("dhtMgrPutProviderRsp: eno: %d", msg.Eno)
	return sch.SchEnoNone
}

func (shMgr *DhtShellManager) dhtMgrGetProviderRsp(msg *sch.MsgDhtMgrGetProviderRsp) sch.SchErrno {
	dhtLog.Debug("dhtMgrGetProviderRsp: eno: %d", msg.Eno)
	return sch.SchEnoNone
}

func (shMgr *DhtShellManager)dhtMgrPutValueLocalRsp(msg *sch.MsgDhtMgrPutValueLocalRsp) sch.SchErrno {
	dhtLog.Debug("dhtMgrPutValueLocalRsp: eno: %d", msg.Eno)
	return sch.SchEnoNone
}

func (shMgr *DhtShellManager) dhtMgrPutValueRsp(msg *sch.MsgDhtMgrPutValueRsp) sch.SchErrno {
	dhtLog.Debug("dhtMgrPutValueRsp: eno: %d", msg.Eno)
	return sch.SchEnoNone
}

func (shMgr *DhtShellManager) dhtMgrGetValueRsp(msg *sch.MsgDhtMgrGetValueRsp) sch.SchErrno {
	dhtLog.Debug("dhtMgrGetValueRsp: eno: %d", msg.Eno)
	return sch.SchEnoNone
}

func (shMgr *DhtShellManager) dhtConMgrCloseRsp(msg *sch.MsgDhtConMgrCloseRsp) sch.SchErrno {
	dhtLog.Debug("dhtConMgrCloseRsp: eno: %d", msg.Eno)
	return sch.SchEnoNone
}

func (shMgr *DhtShellManager) dhtConInstStatusInd(msg *sch.MsgDhtConInstStatusInd) sch.SchErrno {

	switch msg.Status {

	case dht.CisNull:
		dhtLog.Debug("dhtConInstStatusInd: CisNull")

	case dht.CisConnecting:
		dhtLog.Debug("dhtConInstStatusInd: CisConnecting")

	case dht.CisConnected:
		dhtLog.Debug("dhtConInstStatusInd: CisConnected")

	case dht.CisAccepted:
		dhtLog.Debug("dhtTestConInstStatusInd: CisAccepted")

	case dht.CisInHandshaking:
		dhtLog.Debug("dhtTestConInstStatusInd: CisInHandshaking")

	case dht.CisHandshook:
		dhtLog.Debug("dhtTestConInstStatusInd: CisHandshook")

	case dht.CisInService:
		dhtLog.Debug("dhtTestConInstStatusInd: CisInService")

	case dht.CisOutOfService:
		dhtLog.Debug("dhtTestConInstStatusInd: CisOutOfService")

	case dht.CisClosed:
		dhtLog.Debug("dhtTestConInstStatusInd: CisClosed")

	default:
		dhtLog.Debug("dhtTestConInstStatusInd: unknown status: %d", msg.Status)
		return sch.SchEnoParameter
	}

	shMgr.csChan <- msg
	return sch.SchEnoNone
}

func (shMgr *DhtShellManager) dhtRutRefreshReq() sch.SchErrno {
	msg := sch.SchMessage{}
	shMgr.sdl.SchMakeMessage(&msg, shMgr.ptnMe, shMgr.ptnDhtMgr, sch.EvDhtRutRefreshReq, nil)
	return shMgr.sdl.SchSendMessage(&msg)
}

func (shMgr *DhtShellManager) dhtShFindPeerReq(req *sch.MsgDhtQryMgrQueryStartReq) sch.SchErrno {
	msg := sch.SchMessage{}
	shMgr.sdl.SchMakeMessage(&msg, shMgr.ptnMe, shMgr.ptnDhtMgr, sch.EvDhtMgrFindPeerReq, req)
	return shMgr.sdl.SchSendMessage(&msg)
}

func (shMgr *DhtShellManager) dhtShBlindConnectReq(req *sch.MsgDhtBlindConnectReq) sch.SchErrno {
	msg := sch.SchMessage{}
	shMgr.sdl.SchMakeMessage(&msg, shMgr.ptnMe, shMgr.ptnDhtMgr, sch.EvDhtBlindConnectReq, req)
	return shMgr.sdl.SchSendMessage(&msg)
}

func (shMgr *DhtShellManager) dhtShGetValueReq(req *sch.MsgDhtMgrGetValueReq) sch.SchErrno {
	log.Infof("dhtShGetValueReq: going to dispath EvDhtMgrGetValueReq received")
	msg := sch.SchMessage{}
	shMgr.sdl.SchMakeMessage(&msg, shMgr.ptnMe, shMgr.ptnDhtMgr, sch.EvDhtMgrGetValueReq, req)
	if eno := shMgr.sdl.SchSendMessage(&msg); eno != sch.SchEnoNone {
		log.Errorf("dhtShGetValueReq: send message failed, eno: %d", eno)
		return eno
	}
	return sch.SchEnoNone
}

func (shMgr *DhtShellManager) dhtShPutValueReq(req *sch.MsgDhtMgrPutValueReq) sch.SchErrno {
	msg := sch.SchMessage{}
	shMgr.sdl.SchMakeMessage(&msg, shMgr.ptnMe, shMgr.ptnDhtMgr, sch.EvDhtMgrPutValueReq, req)
	return shMgr.sdl.SchSendMessage(&msg)
}

func (shMgr *DhtShellManager) dhtShGetProviderReq(req *sch.MsgDhtMgrGetProviderReq) sch.SchErrno {
	msg := sch.SchMessage{}
	shMgr.sdl.SchMakeMessage(&msg, shMgr.ptnMe, shMgr.ptnDhtMgr, sch.EvDhtMgrGetProviderReq, req)
	return shMgr.sdl.SchSendMessage(&msg)
}

func (shMgr *DhtShellManager) dhtShPutProviderReq(req *sch.MsgDhtPrdMgrAddProviderReq) sch.SchErrno {
	msg := sch.SchMessage{}
	shMgr.sdl.SchMakeMessage(&msg, shMgr.ptnMe, shMgr.ptnDhtMgr, sch.EvDhtMgrPutProviderReq, req)
	return shMgr.sdl.SchSendMessage(&msg)
}

func (shMgr *DhtShellManager) GetEventChan() chan *sch.MsgDhtShEventInd {
	return shMgr.evChan
}

func (shMgr *DhtShellManager) GetConnStatusChan() chan *sch.MsgDhtConInstStatusInd {
	return shMgr.csChan
}
