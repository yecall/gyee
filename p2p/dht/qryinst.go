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
	"time"
	log "github.com/yeeco/gyee/p2p/logger"
	sch	"github.com/yeeco/gyee/p2p/scheduler"
	config "github.com/yeeco/gyee/p2p/config"
)


//
// timeout value
//
const (
	qiWaitConnectTimeout = time.Second * 8
	qiWaitResponseTimeout = time.Second * 8
)


//
// Query instance
//
type QryInst struct {
	tep		sch.SchUserTaskEp			// task entry
	icb		*qryInstCtrlBlock			// instance control block
}

//
// Create query instance
//
func NewQryInst() *QryInst {

	qryInst := QryInst{
		tep:	nil,
		icb:	nil,
	}

	qryInst.tep = qryInst.qryInstProc

	return &qryInst
}

//
// Entry point exported to shceduler
//
func (qryInst *QryInst)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return qryInst.tep(ptn, msg)
}

//
// Query instance entry
//
func (qryInst *QryInst)qryInstProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	if ptn == nil || msg == nil {
		log.LogCallerFileLine("qryInstProc: " +
			"invalid parameters, ptn: %p, msg: %p",
			ptn, msg)
		return sch.SchEnoParameter
	}

	var eno = sch.SchEnoUnknown

	switch msg.Id {

	case sch.EvSchPoweron:
		eno = qryInst.powerOn(ptn)

	case sch.EvSchPoweroff:
		eno = qryInst.powerOff(ptn)

	case sch.EvDhtQryInstStartReq:
		eno = qryInst.startReq()

	case sch.EvDhtQryInstStopReq:
		eno = qryInst.stopReq(msg.Body.(*sch.MsgDhtQryInstStopReq))

	case sch.EvDhtQryMgrIcbTimer:
		eno = qryInst.icbTimerHandler(msg.Body.(*QryInst))

	case sch.EvDhtConMgrConnectRsp:
		eno = qryInst.connectRsp(msg.Body.(*sch.MsgDhtConMgrConnectRsp))

	case sch.EvDhtQryInstProtoDatInd:
		eno = qryInst.protoDatInd(msg.Body.(*sch.MsgDhtQryInstProtoDatInd))

	default:
		log.LogCallerFileLine("qryInstProc: unknown event: %d", msg.Id)
		return sch.SchEnoParameter
	}

	return eno
}

//
// Power on handler
//
func (qryInst *QryInst)powerOn(ptn interface{}) sch.SchErrno {

	var sdl = sch.SchGetScheduler(ptn)
	var ptnConMgr interface{}
	var ptnRutMgr interface{}
	var icb *qryInstCtrlBlock

	if sdl == nil {
		log.LogCallerFileLine("powerOn: SchGetScheduler failed")
		return sch.SchEnoInternal
	}

	if icb = sdl.SchGetUserDataArea(ptn).(*qryInstCtrlBlock); icb == nil {
		log.LogCallerFileLine("powerOn: impossible nil instance control block")
		return sch.SchEnoInternal
	}

	if _, ptnConMgr = sdl.SchGetTaskNodeByName(ConMgrName); ptnConMgr == nil {
		log.LogCallerFileLine("powerOn: nil connection manager")
		return sch.SchEnoInternal
	}

	if _, ptnRutMgr = sdl.SchGetTaskNodeByName(RutMgrName); ptnRutMgr == nil {
		log.LogCallerFileLine("powerOn: nil route manager")
		return sch.SchEnoInternal
	}

	icb.status = qisInited
	icb.ptnConMgr = ptnConMgr
	icb.ptnRutMgr = ptnRutMgr
	qryInst.icb = icb

	ind := sch.MsgDhtQryInstStatusInd {
		Peer:	icb.to.ID,
		Target:	icb.target,
		Status:	qisInited,
	}

	schMsg := sch.SchMessage{}
	sdl.SchMakeMessage(&schMsg, icb.ptnInst, icb.ptnQryMgr, sch.EvDhtQryInstStatusInd, &ind)
	sdl.SchSendMessage(&schMsg)

	return sch.SchEnoNone
}

//
// Power off handler
//
func (qryInst *QryInst)powerOff(ptn interface{}) sch.SchErrno {
	log.LogCallerFileLine("powerOff: task will be done ...")
	return qryInst.icb.sdl.SchTaskDone(qryInst.icb.ptnInst, sch.SchEnoKilled)
}

//
// Start instance handler
//
func (qryInst *QryInst)startReq() sch.SchErrno {

	icb := qryInst.icb

	if icb.status != qisInited {
		log.LogCallerFileLine("startReq: state mismatched: %d", icb.status)
		return sch.SchEnoUserTask
	}

	msg := sch.SchMessage{}
	req := sch.MsgDhtConMgrConnectReq{
		Task:	icb.ptnInst,
		Peer:	&icb.to,
	}

	icb.sdl.SchMakeMessage(&msg, icb.ptnInst, icb.ptnConMgr, sch.EvDhtConMgrConnectReq, &req)
	icb.sdl.SchSendMessage(&msg)

	icb.conBegTime = time.Now()
	td := sch.TimerDescription{
		Name:	"qiConnTimer" + fmt.Sprintf("%d", icb.seq),
		Utid:	sch.DhtQryMgrIcbTimerId,
		Tmt:	sch.SchTmTypeAbsolute,
		Dur:	qiWaitConnectTimeout,
		Extra:	qryInst,
	}

	var eno sch.SchErrno
	var tid int

	ind := sch.MsgDhtQryInstStatusInd {
		Peer:	icb.to.ID,
		Target:	icb.target,
		Status:	qisNull,
	}

	eno, tid = icb.sdl.SchSetTimer(icb.ptnInst, &td)

	if eno != sch.SchEnoNone || tid == sch.SchInvalidTid {

		log.LogCallerFileLine("startReq: SchSetTimer failed, eno: %d", eno)

		ind.Status = qisDone
		icb.status = qisDone

		icb.sdl.SchMakeMessage(&msg, icb.ptnInst, icb.ptnQryMgr, sch.EvDhtQryInstStatusInd, &ind)
		icb.sdl.SchSendMessage(&msg)
		icb.sdl.SchTaskDone(icb.ptnInst, sch.SchEnoInternal)

		return eno
	}

	icb.qTid = tid
	icb.status = qisWaitConnect

	ind.Status = qisWaitConnect
	icb.sdl.SchMakeMessage(&msg, icb.ptnInst, icb.ptnQryMgr, sch.EvDhtQryInstStatusInd, &ind)
	icb.sdl.SchSendMessage(&msg)

	return sch.SchEnoNone
}

//
// Stop instance handler
//
func (qryInst *QryInst)stopReq(msg *sch.MsgDhtQryInstStopReq) sch.SchErrno {

	icb := qryInst.icb
	sdl := icb.sdl
	schMsg := sch.SchMessage{}

	if msg.Target != icb.target || msg.Peer != icb.to.ID {
		log.LogCallerFileLine("")
		return sch.SchEnoMismatched
	}

	log.LogCallerFileLine("stopReq: stopped for eno: %d, target: %x, peer: %x",
		msg.Eno, msg.Target, msg.Peer)

	if icb.status == qisWaitConnect {

		req := sch.MsgDhtConMgrCloseReq {
			Task:	icb.ptnInst,
			Peer:	&icb.to,
		}
		sdl.SchMakeMessage(&schMsg, icb.ptnInst, icb.ptnConMgr, sch.EvDhtConMgrCloseReq, &req)
		sdl.SchSendMessage(&schMsg)

		if icb.qTid != sch.SchInvalidTid {
			sdl.SchKillTimer(icb.ptnInst, icb.qTid)
			icb.qTid = sch.SchInvalidTid
		}
	}

	rsp := sch.MsgDhtQryInstStopRsp {
		To:		icb.to,
		Target:	icb.target,
	}

	sdl.SchMakeMessage(&schMsg, icb.ptnInst, icb.ptnQryMgr, sch.EvDhtQryInstStopRsp, &rsp)
	sdl.SchSendMessage(&schMsg)

	return sch.SchEnoNone
}

//
// instance timer handler
//
func (qryInst *QryInst)icbTimerHandler(msg *QryInst) sch.SchErrno {

	if qryInst != msg {
		log.LogCallerFileLine("icbTimerHandler: instance pointer mismatched")
		return sch.SchEnoMismatched
	}

	icb := qryInst.icb
	sdl := icb.sdl
	schMsg := sch.SchMessage{}

	//
	// this timer might for waiting connection establishment response or waiting
	// response from peer fro a query.
	//

	if (icb.status != qisWaitConnect && icb.status != qisWaitResponse) || icb.qTid == sch.SchInvalidTid {
		log.LogCallerFileLine("icbTimerHandler:" +
			"mismatched, status: %d, qTid: %d",
			icb.status, icb.qTid)
		return sch.SchEnoMismatched
	}

	//
	// if we are waiting connection to be established, we request the connection manager
	// to abandon the connect-procedure. when this request received, the connection manager
	// should check if the connection had been established and route talbe updated, if ture,
	// then do not care this request, else it should close the connection and free all
	// resources had been allocated to the connection instance.
	//

	if icb.status == qisWaitConnect {
		req := sch.MsgDhtConMgrCloseReq{
			Task:	icb.ptnInst,
			Peer:	&icb.to,
		}
		sdl.SchMakeMessage(&schMsg, icb.ptnInst, icb.ptnConMgr, sch.EvDhtConMgrCloseReq, &req)
		sdl.SchSendMessage(&schMsg)
	}

	//
	// update route manager
	//

	var updateReq = sch.MsgDhtRutMgrUpdateReq{
		Why:	rutMgrUpdate4Query,
		Eno:	DhtEnoTimeout,
		Seens:	[]config.Node{icb.to},
		Duras:	nil,
	}
	sdl.SchMakeMessage(&schMsg, icb.ptnInst, icb.ptnRutMgr, sch.EvDhtRutMgrUpdateReq, &updateReq)
	sdl.SchSendMessage(&schMsg)

	//
	// done this instance task and tell query manager task about instance done,
	// need not to close the connection.
	//

	ind := sch.MsgDhtQryInstStatusInd {
		Peer:	icb.to.ID,
		Target:	icb.target,
		Status:	qisDone,
	}

	icb.status = qisDone
	sdl.SchMakeMessage(&schMsg, icb.ptnInst, icb.ptnQryMgr, sch.EvDhtQryInstStatusInd, &ind)
	sdl.SchSendMessage(&schMsg)
	sdl.SchTaskDone(icb.ptnInst, sch.SchEnoKilled)

	return sch.SchEnoNone
}

//
// Connect response handler
//
func (qryInst *QryInst)connectRsp(msg *sch.MsgDhtConMgrConnectRsp) sch.SchErrno {

	icb := qryInst.icb
	sdl := icb.sdl

	schMsg := sch.SchMessage{}
	ind := sch.MsgDhtQryInstStatusInd {
		Peer:	icb.to.ID,
		Target:	icb.target,
		Status:	qisNull,
	}
	sendReq := sch.MsgDhtConMgrSendReq{}

	//
	// here "DhtEnoDuplicated" means the connection had been exist, so it's not
	// an error for connection establishment. if failed, done the instance.
	//

	if msg.Eno != DhtEnoNone && msg.Eno != DhtEnoDuplicated {

		log.LogCallerFileLine("connectRsp:" +
			"connect failed, eno: %d, peer: %+V",
			msg.Eno, *msg.Peer)

		if icb.qTid != sch.SchInvalidTid {

			sdl.SchKillTimer(icb.ptnInst, icb.qTid)
			icb.qTid = sch.SchInvalidTid
		}

		ind.Status = qisDone
		icb.status = qisDone

		sdl.SchMakeMessage(&schMsg, icb.ptnInst, icb.ptnQryMgr, sch.EvDhtQryInstStatusInd, &ind)
		sdl.SchSendMessage(&schMsg)
		sdl.SchTaskDone(icb.ptnInst, sch.SchEnoKilled)

		return sch.SchEnoNone
	}

	icb.conEndTime = time.Now()

	//
	// send query to peer since connection is ok here now
	//

	var pkg = []byte{}

	if eno := qryInst.setupQryPkg(pkg); eno != DhtEnoNone {

		log.LogCallerFileLine("connectRsp: setupQryPkg failed, eno: %d", eno)

		ind.Status = qisDone
		icb.status = qisDone

		sdl.SchMakeMessage(&schMsg, icb.ptnInst, icb.ptnQryMgr, sch.EvDhtQryInstStatusInd, &ind)
		sdl.SchSendMessage(&schMsg)
		sdl.SchTaskDone(icb.ptnInst, sch.SchEnoKilled)

		return sch.SchEnoUserTask
	}

	sendReq.Data = pkg
	sendReq.Task = icb.ptnInst
	sendReq.Peer = &icb.to

	sdl.SchMakeMessage(&schMsg, icb.ptnInst, icb.ptnConMgr, sch.EvDhtConMgrSendReq, &sendReq)
	sdl.SchSendMessage(&schMsg)

	//
	// update instance status and tell query manager
	//

	ind.Status = qisWaitResponse
	icb.status = qisWaitResponse
	icb.begTime = time.Now()

	sdl.SchMakeMessage(&schMsg, icb.ptnInst, icb.ptnQryMgr, sch.EvDhtQryInstStatusInd, &ind)
	sdl.SchSendMessage(&schMsg)

	//
	// start timer to wait query response from peer
	//

	td := sch.TimerDescription {
		Name:	"qiQryTimer" + fmt.Sprintf("%d", icb.seq),
		Utid:	sch.DhtQryMgrIcbTimerId,
		Tmt:	sch.SchTmTypeAbsolute,
		Dur:	qiWaitResponseTimeout,
		Extra:	qryInst,
	}

	eno, tid := sdl.SchSetTimer(icb.ptnInst, &td)

	if eno != sch.SchEnoNone || tid == sch.SchInvalidTid {
		log.LogCallerFileLine("connectRsp: SchSetTimer failed, eno: %d, tid: %d", eno, tid)
		return eno
	}

	icb.qTid = tid

	return sch.SchEnoNone
}

//
// Incoming DHT messages handler
//
func (qryInst *QryInst)protoDatInd(msg *sch.MsgDhtQryInstProtoDatInd) sch.SchErrno {
	return sch.SchEnoNone
}

//
// Setup the package for query by protobuf schema
//
func (qryInst *QryInst)setupQryPkg(pkg []byte) DhtErrno {
	return DhtEnoNone
}
