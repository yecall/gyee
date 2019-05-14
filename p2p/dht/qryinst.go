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
	"bytes"
	"fmt"
	"time"

	config "github.com/yeeco/gyee/p2p/config"
	p2plog "github.com/yeeco/gyee/p2p/logger"
	sch "github.com/yeeco/gyee/p2p/scheduler"
	log "github.com/yeeco/gyee/log"
)

//
// debug
//
type qiMgrLogger struct {
	debug__      bool
	debugForce__ bool
}

var qiLog = qiMgrLogger{
	debug__:      false,
	debugForce__: false,
}

func (log qiMgrLogger) Debug(fmt string, args ...interface{}) {
	if log.debug__ {
		p2plog.Debug(fmt, args...)
	}
}

func (log qiMgrLogger) ForceDebug(fmt string, args ...interface{}) {
	if log.debugForce__ {
		p2plog.Debug(fmt, args...)
	}
}

//
// timeout value
//
const (
	qiWaitConnectTimeout  = time.Second * 8
	qiWaitResponseTimeout = time.Second * 8
)

//
// Query instance
//
type QryInst struct {
	tep sch.SchUserTaskEp // task entry
	icb *qryInstCtrlBlock // instance control block
}

//
// Create query instance
//
func NewQryInst() *QryInst {

	qryInst := QryInst{
		tep: nil,
		icb: nil,
	}

	qryInst.tep = qryInst.qryInstProc

	return &qryInst
}

//
// Entry point exported to shceduler
//
func (qryInst *QryInst) TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return qryInst.tep(ptn, msg)
}

//
// Query instance entry
//
func (qryInst *QryInst) qryInstProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	qiLog.Debug("qryInstProc: ptn: %p, msg.Id: %d", ptn, msg.Id)
	var eno = sch.SchEnoUnknown

	switch msg.Id {

	case sch.EvSchPoweron:
		eno = qryInst.powerOn(ptn)

	case sch.EvSchPoweroff:
		eno = qryInst.powerOff(ptn)

	case sch.EvDhtQryInstStartReq:
		eno = qryInst.startReq()

	case sch.EvDhtQryMgrIcbTimer:
		eno = qryInst.icbTimerHandler(msg.Body.(*QryInst))

	case sch.EvDhtConMgrConnectRsp:
		eno = qryInst.connectRsp(msg.Body.(*sch.MsgDhtConMgrConnectRsp))

	case sch.EvDhtQryInstProtoMsgInd:
		eno = qryInst.protoMsgInd(msg.Body.(*sch.MsgDhtQryInstProtoMsgInd))

	case sch.EvDhtConInstTxInd:
		eno = qryInst.conInstTxInd(msg.Body.(*sch.MsgDhtConInstTxInd))

	default:
		qiLog.Debug("qryInstProc: unknown event: %d", msg.Id)
		eno = sch.SchEnoParameter
	}

	qiLog.Debug("qryInstProc: get out, ptn: %p, msg.Id: %d", ptn, msg.Id)

	return eno
}

//
// Power on handler
//
func (qryInst *QryInst) powerOn(ptn interface{}) sch.SchErrno {

	var sdl = sch.SchGetScheduler(ptn)
	var ptnQryMgr interface{}
	var ptnConMgr interface{}
	var ptnRutMgr interface{}
	var icb *qryInstCtrlBlock

	if sdl == nil {
		qiLog.Debug("powerOn: SchGetScheduler failed")
		return sch.SchEnoInternal
	}

	if icb = sdl.SchGetUserDataArea(ptn).(*qryInstCtrlBlock); icb == nil {
		qiLog.Debug("powerOn: impossible nil instance control block, " +
			"sdl: %s, inst: %s",
			icb.sdlName, icb.name)
		return sch.SchEnoInternal
	}

	if _, ptnQryMgr = sdl.SchGetUserTaskNode(QryMgrName); ptnQryMgr == nil {
		qiLog.Debug("powerOn: nil query manager, " +
			"sdl: %s, inst: %s",
			icb.sdlName, icb.name)
		return sch.SchEnoInternal
	}

	if _, ptnConMgr = sdl.SchGetUserTaskNode(ConMgrName); ptnConMgr == nil {
		qiLog.Debug("powerOn: nil connection manager, " +
			"sdl: %s, inst: %s",
			icb.sdlName, icb.name)
		return sch.SchEnoInternal
	}

	if _, ptnRutMgr = sdl.SchGetUserTaskNode(RutMgrName); ptnRutMgr == nil {
		qiLog.Debug("powerOn: nil route manager, " +
			"sdl: %s, inst: %s",
			icb.sdlName, icb.name)
		return sch.SchEnoInternal
	}

	icb.sdlName = sdl.SchGetP2pCfgName()
	icb.status = qisInited
	icb.ptnQryMgr = ptnQryMgr
	icb.ptnConMgr = ptnConMgr
	icb.ptnRutMgr = ptnRutMgr
	qryInst.icb = icb

	ind := sch.MsgDhtQryInstStatusInd{
		Peer:   icb.to.ID,
		Target: icb.target,
		Status: qisInited,
	}

	schMsg := sch.SchMessage{}
	sdl.SchMakeMessage(&schMsg, icb.ptnInst, icb.ptnQryMgr, sch.EvDhtQryInstStatusInd, &ind)
	sdl.SchSendMessage(&schMsg)

	return sch.SchEnoNone
}

//
// Power off handler
//
func (qryInst *QryInst) powerOff(ptn interface{}) sch.SchErrno {
	qiLog.ForceDebug("powerOff: task will be done, " +
		"sdl: %s, inst: %s",
		qryInst.icb.sdlName, qryInst.icb.name)
	return qryInst.icb.sdl.SchTaskDone(qryInst.icb.ptnInst, qryInst.icb.name, sch.SchEnoKilled)
}

//
// Start instance handler
//
func (qryInst *QryInst) startReq() sch.SchErrno {

	icb := qryInst.icb
	if icb.status != qisInited {
		log.Errorf("startReq: state mismatched, " + "" +
			"sdl: %s, inst: %s, status: %d",
			qryInst.icb.sdlName, qryInst.icb.name, icb.status)
		return sch.SchEnoUserTask
	}

	log.Infof("startReq: ask connection manager for peer, " +
		"sdl: %s, inst: %s, ForWhat: %d",
		icb.sdlName, icb.name, icb.qryReq.ForWhat)

	req := sch.MsgDhtConMgrConnectReq{
		Task:    icb.ptnInst,
		Name:    icb.name,
		Peer:    &icb.to,
		IsBlind: false,
	}
	eno := sch.SchEnoUnknown
	tid := sch.SchInvalidTid
	ind := sch.MsgDhtQryInstStatusInd{
		Peer:   icb.to.ID,
		Target: icb.target,
		Status: qisNull,
	}

	msg := sch.SchMessage{}
	icb.sdl.SchMakeMessage(&msg, icb.ptnInst, icb.ptnConMgr, sch.EvDhtConMgrConnectReq, &req)
	if eno = icb.sdl.SchSendMessage(&msg); eno != sch.SchEnoNone {
		log.Errorf("startReq: send EvDhtConMgrConnectReq failed, " +
			"sdl: %s, inst: %s, ForWhat: %d, eno: %d",
			icb.sdlName, icb.name, icb.qryReq.ForWhat, eno)
		ind.Status = qisDone
		icb.status = qisDone
		msg = sch.SchMessage{}
		icb.sdl.SchMakeMessage(&msg, icb.ptnInst, icb.ptnQryMgr, sch.EvDhtQryInstStatusInd, &ind)
		icb.sdl.SchSendMessage(&msg)
		icb.sdl.SchTaskDone(icb.ptnInst, icb.name, sch.SchEnoInternal)
		return eno
	}
	icb.conBegTime = time.Now()

	td := sch.TimerDescription{
		Name:  "qiConnTimer" + fmt.Sprintf("%d", icb.seq),
		Utid:  sch.DhtQryMgrIcbTimerId,
		Tmt:   sch.SchTmTypeAbsolute,
		Dur:   qiWaitConnectTimeout,
		Extra: qryInst,
	}

	eno, tid = icb.sdl.SchSetTimer(icb.ptnInst, &td)
	if eno != sch.SchEnoNone || tid == sch.SchInvalidTid {
		log.Errorf("startReq: done for SchSetTimer failed, " +
			"sdl: %s, inst: %s, eno: %d, ForWhat: %d",
			icb.sdlName, icb.name, eno, icb.qryReq.ForWhat)
		ind.Status = qisDone
		icb.status = qisDone
		msg = sch.SchMessage{}
		icb.sdl.SchMakeMessage(&msg, icb.ptnInst, icb.ptnQryMgr, sch.EvDhtQryInstStatusInd, &ind)
		icb.sdl.SchSendMessage(&msg)
		icb.sdl.SchTaskDone(icb.ptnInst, icb.name, sch.SchEnoInternal)
		return eno
	}

	icb.qTid = tid
	icb.status = qisWaitConnect
	ind.Status = qisWaitConnect
	msg = sch.SchMessage{}
	icb.sdl.SchMakeMessage(&msg, icb.ptnInst, icb.ptnQryMgr, sch.EvDhtQryInstStatusInd, &ind)
	if eno := icb.sdl.SchSendMessage(&msg); eno != sch.SchEnoNone {
		log.Errorf("startReq: send EvDhtConMgrConnectReq failed, " +
			"sdl: %s, inst: %s, ForWhat: %d",
			icb.sdlName, icb.name, icb.qryReq.ForWhat)
		return eno
	}
	return sch.SchEnoNone
}

//
// instance timer handler
//
func (qryInst *QryInst) icbTimerHandler(msg *QryInst) sch.SchErrno {
	if msg == nil {
		log.Errorf("icbTimerHandler: invalid parameter, " +
			"sdl: %s, inst: %s, forWhat: %d",
			qryInst.icb.sdlName, qryInst.icb.name, qryInst.icb.qryReq.ForWhat)
		return sch.SchEnoParameter
	}

	log.Warnf("icbTimerHandler: query instance timer expired, " +
		"sdl: %s, inst: %s, forWhat: %d",
		qryInst.icb.sdlName, qryInst.icb.name, qryInst.icb.qryReq.ForWhat)

	if qryInst != msg {
		log.Errorf("icbTimerHandler: instance pointer mismatched, " +
			"sdl: %s, inst: %s, forWhat: %d",
			qryInst.icb.sdlName, qryInst.icb.name, qryInst.icb.qryReq.ForWhat)
		return sch.SchEnoMismatched
	}

	icb := qryInst.icb
	sdl := icb.sdl

	//
	// this timer might for waiting connection establishment response or waiting
	// response from peer for a query.
	//

	if (icb.status != qisWaitConnect && icb.status != qisWaitResponse) || icb.qTid == sch.SchInvalidTid {
		log.Errorf("icbTimerHandler: mismatched, " +
			"sdl: %s, inst: %s, status: %d, qTid: %d, forWhat: %d",
			icb.sdlName, icb.name, icb.status, icb.qTid, icb.qryReq.ForWhat)
		return sch.SchEnoMismatched
	}

	//
	// do not send EvDhtConMgrCloseReq when the instance is in qisWaitConnect status, this
	// case, we do not know the "dir" about the connection instance, since we had never got
	// a "connect-response"(so the timer expired). if it's really want to kill the connection
	// instance, the "dir" should be set to "outbound", a "inbound" direction is impossible.
	//

	if icb.status == qisWaitResponse {
		log.Infof("icbTimerHandler: send EvDhtConMgrCloseReq, " +
			"sdl: %s, inst: %s, dir: %d, forWhat: %d",
			icb.sdlName, icb.name, icb.dir, icb.qryReq.ForWhat)
		req := sch.MsgDhtConMgrCloseReq{
			Task: icb.sdl.SchGetTaskName(icb.ptnInst),
			Peer: &icb.to,
			Dir:  icb.dir,
		}
		schMsg := sch.SchMessage{}
		sdl.SchMakeMessage(&schMsg, icb.ptnInst, icb.ptnConMgr, sch.EvDhtConMgrCloseReq, &req)
		if eno := sdl.SchSendMessage(&schMsg); eno != sch.SchEnoNone {
			log.Infof("icbTimerHandler: send EvDhtConMgrCloseReq failed, " +
				"sdl: %s, inst: %s, dir: %d, forWhat: %d",
				icb.sdlName, icb.name, icb.dir, icb.qryReq.ForWhat)
		}
	}

	//
	// update route manager
	//

	var updateReq = sch.MsgDhtRutMgrUpdateReq{
		Why:   rutMgrUpdate4Query,
		Eno:   DhtEnoTimeout.GetEno(),
		Seens: []config.Node{icb.to},
		Duras: []time.Duration{-1},
	}
	schMsg := sch.SchMessage{}
	sdl.SchMakeMessage(&schMsg, icb.ptnInst, icb.ptnRutMgr, sch.EvDhtRutMgrUpdateReq, &updateReq)
	if eno := sdl.SchSendMessage(&schMsg); eno != sch.SchEnoNone {
		if eno := sdl.SchSendMessage(&schMsg); eno != sch.SchEnoNone {
			log.Infof("icbTimerHandler: send EvDhtRutMgrUpdateReq failed, " +
				"sdl: %s, inst: %s, dir: %d, forWhat: %d",
				icb.sdlName, icb.name, icb.dir, icb.qryReq.ForWhat)
		}
	}

	//
	// done this instance task and tell query manager task about instance done,
	// need not to close the connection.
	//

	ind := sch.MsgDhtQryInstStatusInd{
		Peer:   icb.to.ID,
		Target: icb.target,
		Status: qisDone,
	}

	icb.status = qisDone
	schMsg = sch.SchMessage{}
	sdl.SchMakeMessage(&schMsg, icb.ptnInst, icb.ptnQryMgr, sch.EvDhtQryInstStatusInd, &ind)
	if eno := sdl.SchSendMessage(&schMsg); eno != sch.SchEnoNone {
		log.Infof("icbTimerHandler: send EvDhtRutMgrUpdateReq failed, " +
			"sdl: %s, inst: %s, dir: %d, forWhat: %d",
			icb.sdlName, icb.name, icb.dir, icb.qryReq.ForWhat)
	}

	log.Infof("icbTimerHandler: done instance, " +
		"sdl: %s, inst: %s, dir: %d, forWhat: %d",
		icb.sdlName, icb.name, icb.dir, icb.qryReq.ForWhat)

	return sdl.SchTaskDone(icb.ptnInst, icb.name, sch.SchEnoTimeout)
}

//
// Connect response handler
//
func (qryInst *QryInst) connectRsp(msg *sch.MsgDhtConMgrConnectRsp) sch.SchErrno {

	icb := qryInst.icb
	sdl := icb.sdl

	log.Infof("connectRsp: " +
		"sdl: %s, inst: %s, dir: %d, status: %d, ForWhat: %d, eno: %d",
		icb.sdlName, icb.name, icb.dir, icb.status, icb.qryReq.ForWhat, msg.Eno)

	if icb.status != qisWaitConnect {
		log.Warnf("connectRsp: mismatched, " +
			"sdl: %s, inst: %s, dir: %d, status: %d, ForWhat: %d, eno: %d",
			icb.sdlName, icb.name, icb.dir, icb.status, icb.qryReq.ForWhat, msg.Eno)
		return sch.SchEnoMismatched
	}

	ind := sch.MsgDhtQryInstStatusInd{
		Peer:   icb.to.ID,
		Target: icb.target,
		Status: qisNull,
	}
	sendReq := sch.MsgDhtConMgrSendReq{}

	//
	// here "DhtEnoDuplicated" means the connection had been exist, so it's not
	// an error for connection establishment. if failed, done the instance.
	//

	if icb.qTid != sch.SchInvalidTid {
		sdl.SchKillTimer(icb.ptnInst, icb.qTid)
		icb.qTid = sch.SchInvalidTid
	}

	if msg.Eno != DhtEnoNone.GetEno() && msg.Eno != DhtEnoDuplicated.GetEno() {

		log.Warnf("connectRsp: done for connect failed, " +
			"sdl: %s, inst: %s, dir: %d, status: %d, ForWhat: %d, eno: %d",
			icb.sdlName, icb.name, icb.dir, icb.status, icb.qryReq.ForWhat, msg.Eno)

		ind.Status = qisDone
		icb.status = qisDone

		schMsg := sch.SchMessage{}
		sdl.SchMakeMessage(&schMsg, icb.ptnInst, icb.ptnQryMgr, sch.EvDhtQryInstStatusInd, &ind)
		sdl.SchSendMessage(&schMsg)
		sdl.SchTaskDone(icb.ptnInst, icb.name, sch.SchEnoKilled)
		return sch.SchEnoNone
	}

	icb.conEndTime = time.Now()
	icb.dir = msg.Dir

	//
	// send query to peer since connection is ok here now
	//

	eno, pkg := qryInst.setupQryPkg()
	if eno != DhtEnoNone {

		log.Warnf("connectRsp: done for setupQryPkg failed, " +
			"sdl: %s, inst: %s, dir: %d, status: %d, ForWhat: %d, eno: %d",
			icb.sdlName, icb.name, icb.dir, icb.status, icb.qryReq.ForWhat, msg.Eno)

		ind.Status = qisDone
		icb.status = qisDone
		schMsg := sch.SchMessage{}
		sdl.SchMakeMessage(&schMsg, icb.ptnInst, icb.ptnQryMgr, sch.EvDhtQryInstStatusInd, &ind)
		sdl.SchSendMessage(&schMsg)
		sdl.SchTaskDone(icb.ptnInst, icb.name, sch.SchEnoKilled)
		return sch.SchEnoUserTask
	}

	log.Infof("connectRsp: setupQryPkg ok, " +
		"sdl: %s, inst: %s, dir: %d, status: %d, ForWhat: %d, eno: %d",
		icb.sdlName, icb.name, icb.dir, icb.status, icb.qryReq.ForWhat, msg.Eno)

	sendReq.Task = icb.ptnInst
	sendReq.Peer = &icb.to
	sendReq.Data = pkg

	var waitMid = map[int]int{
		MID_FINDNODE:        MID_NEIGHBORS,
		MID_GETPROVIDER_REQ: MID_GETPROVIDER_RSP,
		MID_GETVALUE_REQ:    MID_GETVALUE_RSP,
	}

	if icb.qryReq.ForWhat == MID_FINDNODE ||
		icb.qryReq.ForWhat == MID_GETPROVIDER_REQ ||
		icb.qryReq.ForWhat == MID_GETVALUE_REQ {
		sendReq.WaitRsp = true
		sendReq.WaitMid = waitMid[icb.qryReq.ForWhat]
		sendReq.WaitSeq = icb.qryReq.Seq
	} else {
		sendReq.WaitRsp = false
		sendReq.WaitMid = -1
		sendReq.WaitSeq = -1
	}

	schMsg := sch.SchMessage{}
	sdl.SchMakeMessage(&schMsg, icb.ptnInst, icb.ptnConMgr, sch.EvDhtConMgrSendReq, &sendReq)
	if eno := sdl.SchSendMessage(&schMsg); eno != sch.SchEnoNone {
		log.Warnf("connectRsp: send EvDhtConMgrSendReq failed, " +
			"sdl: %s, inst: %s, dir: %d, status: %d, ForWhat: %d, eno: %d",
			icb.sdlName, icb.name, icb.dir, icb.status, icb.qryReq.ForWhat, eno)
	}

	//
	// for "put-value" or "put-provider", we should send indication to query manager
	// as following, since no responses are expected from peer in these cases.
	// notice: the dht package might still not be sent at this moment, firstly it will
	// be put into pending queue of a connection instance.
	//

	if icb.qryReq.ForWhat == MID_PUTVALUE || icb.qryReq.ForWhat == MID_PUTPROVIDER {

		//
		// tell query manager the result
		//

		fwMap := map[int]int{
			MID_PUTVALUE:    sch.EvDhtMgrPutValueReq,
			MID_PUTPROVIDER: sch.EvDhtMgrPutProviderReq,
		}
		fw := fwMap[icb.qryReq.ForWhat]
		indResult := sch.MsgDhtQryInstResultInd{
			From:     icb.to,
			Target:   icb.target,
			Latency:  icb.conEndTime.Sub(icb.conBegTime),
			ForWhat:  fw,
			Peers:    []*config.Node{&icb.to},
			Provider: nil,
			Value:    nil,
			Pcs:      []int{pcsConnYes},
		}
		schMsg = sch.SchMessage{}
		sdl.SchMakeMessage(&schMsg, icb.ptnInst, icb.ptnQryMgr, sch.EvDhtQryInstResultInd, &indResult)
		if eno := sdl.SchSendMessage(&schMsg); eno != sch.SchEnoNone {
			log.Warnf("connectRsp: send EvDhtConMgrSendReq failed, " +
				"sdl: %s, inst: %s, dir: %d, status: %d, ForWhat: %d, eno: %d",
				icb.sdlName, icb.name, icb.dir, icb.status, icb.qryReq.ForWhat, eno)
			return eno
		}
		return sch.SchEnoNone
	}

	//
	// update instance status and tell query manager
	//

	ind.Status = qisWaitResponse
	icb.status = qisWaitResponse
	icb.begTime = time.Now()

	schMsg = sch.SchMessage{}
	sdl.SchMakeMessage(&schMsg, icb.ptnInst, icb.ptnQryMgr, sch.EvDhtQryInstStatusInd, &ind)
	if eno := sdl.SchSendMessage(&schMsg); eno != sch.SchEnoNone {
		log.Warnf("connectRsp: send EvDhtConMgrSendReq failed, " +
			"sdl: %s, inst: %s, dir: %d, status: %d, ForWhat: %d, eno: %d",
			icb.sdlName, icb.name, icb.dir, icb.status, icb.qryReq.ForWhat, eno)
	}

	//
	// start timer to wait query response from peer
	//

	td := sch.TimerDescription{
		Name:  "qiQryTimer" + fmt.Sprintf("%d", icb.seq),
		Utid:  sch.DhtQryMgrIcbTimerId,
		Tmt:   sch.SchTmTypeAbsolute,
		Dur:   qiWaitResponseTimeout,
		Extra: qryInst,
	}

	schEno, tid := sdl.SchSetTimer(icb.ptnInst, &td)
	if schEno != sch.SchEnoNone || tid == sch.SchInvalidTid {
		log.Warnf("connectRsp: SchSetTimer failed, " +
			"sdl: %s, inst: %s, dir: %d, status: %d, ForWhat: %d, eno: %d",
			icb.sdlName, icb.name, icb.dir, icb.status, icb.qryReq.ForWhat, msg.Eno)
		return schEno
	}

	icb.qTid = tid

	return sch.SchEnoNone
}

//
// Incoming DHT messages handler
//
func (qryInst *QryInst) protoMsgInd(msg *sch.MsgDhtQryInstProtoMsgInd) sch.SchErrno {

	//
	// notice: here response from peer got, means result of query instance obtained,
	// we send the result to query manager and update the query instance to "qisDoneOk"
	//

	icb := qryInst.icb
	icb.endTime = time.Now()
	msgResult := sch.SchMessage{}

	switch msg.ForWhat {

	case sch.EvDhtConInstNeighbors:

		nbs, ok := msg.Msg.(*Neighbors)
		if !ok {
			qiLog.Debug("protoMsgInd: mismatched type Neighbors, " +
				"sdl: %s, inst: %s",
				icb.sdlName, icb.name)
			return sch.SchEnoMismatched
		}

		ind := sch.MsgDhtQryInstResultInd{
			From:     nbs.From,
			Target:   icb.target,
			ForWhat:  msg.ForWhat,
			Latency:  icb.endTime.Sub(icb.begTime),
			Peers:    nbs.Nodes,
			Provider: nil,
			Value:    nil,
			Pcs:      nbs.Pcs,
		}

		for idx, peer := range nbs.Nodes {
			if peer.IP.IsUnspecified() == false {
				ind.Peers = append(ind.Peers, peer)
				ind.Pcs = append(ind.Pcs, nbs.Pcs[idx])
			}
		}

		icb.sdl.SchMakeMessage(&msgResult, icb.ptnInst, icb.ptnQryMgr, sch.EvDhtQryInstResultInd, &ind)

	case sch.EvDhtConInstGetValRsp:

		gvr, ok := msg.Msg.(*GetValueRsp)
		if !ok {
			qiLog.Debug("protoMsgInd: mismatched type GetValueRsp, " +
				"sdl: %s, inst: %s",
				icb.sdlName, icb.name)
			return sch.SchEnoMismatched
		}

		if gvr.Value != nil {

			if bytes.Equal(gvr.Value.Key, icb.target[0:]) == false {
				qiLog.Debug("protoMsgInd: key mismatched, " +
					"sdl: %s, inst: %s",
					icb.sdlName, icb.name)
				return sch.SchEnoMismatched
			}

			log.Infof("protoMsgInd: EvDhtConInstGetValRsp, " +
				"sdl: %s, forWhat: %d, inst: %s, key: %x",
				icb.sdlName, msg.ForWhat, icb.name, icb.target)

			ind := sch.MsgDhtQryInstResultInd{
				From:     gvr.From,
				Target:   icb.target,
				ForWhat:  msg.ForWhat,
				Latency:  icb.endTime.Sub(icb.begTime),
				Peers:    nil,
				Provider: nil,
				Value:    gvr.Value.Val,
				Pcs:      gvr.Pcs,
			}

			icb.sdl.SchMakeMessage(&msgResult, icb.ptnInst, icb.ptnQryMgr, sch.EvDhtQryInstResultInd, &ind)

		} else {

			ind := sch.MsgDhtQryInstResultInd{
				From:     gvr.From,
				Target:   icb.target,
				ForWhat:  msg.ForWhat,
				Latency:  icb.endTime.Sub(icb.begTime),
				Peers:    gvr.Nodes,
				Provider: nil,
				Value:    nil,
				Pcs:      gvr.Pcs,
			}

			icb.sdl.SchMakeMessage(&msgResult, icb.ptnInst, icb.ptnQryMgr, sch.EvDhtQryInstResultInd, &ind)
		}

	case sch.EvDhtConInstGetProviderRsp:

		gpr, ok := msg.Msg.(*GetProviderRsp)
		if !ok {
			qiLog.Debug("protoMsgInd: mismatched type GetProviderRsp, " +
				"sdl: %s, inst: %s",
				icb.sdlName, icb.name)
			return sch.SchEnoMismatched
		}

		if gpr.Provider != nil {

			if bytes.Equal(gpr.Key, icb.target[0:]) == false {
				qiLog.Debug("protoMsgInd: key mismatched, " +
					"sdl: %s, inst: %s",
					icb.sdlName, icb.name)
				return sch.SchEnoMismatched
			}

			ind := sch.MsgDhtQryInstResultInd{
				From:     gpr.From,
				Target:   icb.target,
				ForWhat:  msg.ForWhat,
				Latency:  icb.endTime.Sub(icb.begTime),
				Peers:    nil,
				Provider: (*sch.Provider)(gpr.Provider),
				Value:    nil,
				Pcs:      gpr.Pcs,
			}

			icb.sdl.SchMakeMessage(&msgResult, icb.ptnInst, icb.ptnQryMgr, sch.EvDhtQryInstResultInd, &ind)

		} else {

			ind := sch.MsgDhtQryInstResultInd{
				From:     gpr.From,
				Target:   icb.target,
				ForWhat:  msg.ForWhat,
				Latency:  icb.endTime.Sub(icb.begTime),
				Peers:    gpr.Nodes,
				Provider: nil,
				Value:    nil,
				Pcs:      gpr.Pcs,
			}

			icb.sdl.SchMakeMessage(&msgResult, icb.ptnInst, icb.ptnQryMgr, sch.EvDhtQryInstResultInd, &ind)
		}

	default:
		qiLog.Debug("protoMsgInd: mismatched, " +
			"sdl: %s, inst: %d, ForWhat: %d",
			icb.sdlName, icb.name, msg.ForWhat)
		return sch.SchEnoMismatched
	}

	if icb.sdl.SchSendMessage(&msgResult) != sch.SchEnoNone {
		log.Errorf("protoMsgInd: send EvDhtQryInstResultInd failed, " +
			"sdl: %s, inst: %d, ForWhat: %d",
			icb.sdlName, icb.name, msg.ForWhat)
	}

	icb.status = qisDoneOk
	ind := sch.MsgDhtQryInstStatusInd{
		Peer:   icb.to.ID,
		Target: icb.target,
		Status: qisNull,
	}
	msgInd := sch.SchMessage{}
	icb.sdl.SchMakeMessage(&msgInd, icb.ptnInst, icb.ptnQryMgr, sch.EvDhtQryInstStatusInd, &ind)
	if eno := icb.sdl.SchSendMessage(&msgInd); eno != sch.SchEnoNone {
		log.Errorf("protoMsgInd: send EvDhtQryInstStatusInd failed, " +
			"sdl: %s, inst: %d, ForWhat: %d",
			icb.sdlName, icb.name, msg.ForWhat)
		return eno
	}
	return sch.SchEnoNone
}

//
// Tx status indication handler
//
func (qryInst *QryInst) conInstTxInd(msg *sch.MsgDhtConInstTxInd) sch.SchErrno {
	if msg == nil {
		qiLog.Debug("conInstTxInd： invalid parameter, " +
			"sdl: %s, inst: %s",
			qryInst.icb.sdlName, qryInst.icb.name)
		return sch.SchEnoParameter
	}
	qiLog.Debug("conInstTxInd：" +
		"sdl: %s, inst: %s, msg: %+v",
		qryInst.icb.sdlName, qryInst.icb.name, *msg)
	return sch.SchEnoNone
}

//
// Setup the package for query by protobuf schema
//
func (qryInst *QryInst) setupQryPkg() (DhtErrno, *DhtPackage) {

	icb := qryInst.icb
	forWhat := icb.qryReq.ForWhat
	dhtMsg := DhtMessage{Mid: MID_UNKNOWN}
	dhtPkg := DhtPackage{}

	if forWhat == MID_PUTPROVIDER {

		msg := icb.qryReq.Msg.(*sch.MsgDhtPrdMgrAddProviderReq)
		pp := PutProvider{
			From:     *icb.local,
			To:       icb.to,
			Provider: &DhtProvider{Key: msg.Key, Nodes: []*config.Node{&msg.Prd}, Extra: nil},
			Id:       icb.qryReq.Seq,
			Extra:    nil,
		}

		dhtMsg.Mid = MID_PUTPROVIDER
		dhtMsg.PutProvider = &pp

	} else if forWhat == MID_PUTVALUE {

		msg := icb.qryReq.Msg.(*sch.MsgDhtDsMgrAddValReq)
		pv := PutValue{
			From:   *icb.local,
			To:     icb.to,
			Values: []DhtValue{{Key: msg.Key, Val: msg.Val, Extra: nil}},
			Id:     icb.qryReq.Seq,
			Extra:  nil,
		}

		dhtMsg.Mid = MID_PUTVALUE
		dhtMsg.PutValue = &pv

	} else if forWhat == MID_FINDNODE {

		fn := FindNode{
			From:   *icb.local,
			To:     icb.to,
			Target: icb.target,
			Id:     icb.qryReq.Seq,
			Extra:  nil,
		}

		dhtMsg.Mid = MID_FINDNODE
		dhtMsg.FindNode = &fn

	} else if forWhat == MID_GETVALUE_REQ {

		gvr := GetValueReq{
			From:  *qryInst.icb.local,
			To:    qryInst.icb.to,
			Key:   icb.target[0:],
			Id:    icb.qryReq.Seq,
			Extra: nil,
		}

		dhtMsg.Mid = MID_GETVALUE_REQ
		dhtMsg.GetValueReq = &gvr

	} else if forWhat == MID_GETPROVIDER_REQ {

		gpr := GetProviderReq{
			From:  *qryInst.icb.local,
			To:    qryInst.icb.to,
			Key:   icb.target[0:],
			Id:    icb.qryReq.Seq,
			Extra: nil,
		}

		dhtMsg.Mid = MID_GETPROVIDER_REQ
		dhtMsg.GetProviderReq = &gpr

	} else {
		qiLog.Debug("setupQryPkg: unknown what's for, " +
			"sdl: %s, inst: %s, forWhat: %d",
			icb.sdlName, icb.name, forWhat)
		return DhtEnoMismatched, nil
	}

	if eno := dhtMsg.GetPackage(&dhtPkg); eno != DhtEnoNone {
		qiLog.Debug("setupQryPkg: GetPackage failed, " +
			"sdl: %s, inst: %s, eno: %d",
			icb.sdlName, icb.name, eno)
		return eno, nil
	}

	return DhtEnoNone, &dhtPkg
}
