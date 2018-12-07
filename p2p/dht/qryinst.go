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
	"bytes"
	log "github.com/yeeco/gyee/p2p/logger"
	"github.com/yeeco/gyee/p2p/config"
	sch	"github.com/yeeco/gyee/p2p/scheduler"
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
		log.Debug("qryInstProc: " +
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

	case sch.EvDhtQryInstProtoMsgInd:
		eno = qryInst.protoMsgInd(msg.Body.(*sch.MsgDhtQryInstProtoMsgInd))

	case sch.EvDhtConInstTxInd:
		eno = qryInst.conInstTxInd(msg.Body.(*sch.MsgDhtConInstTxInd))

	default:
		log.Debug("qryInstProc: unknown event: %d", msg.Id)
		return sch.SchEnoParameter
	}

	return eno
}

//
// Power on handler
//
func (qryInst *QryInst)powerOn(ptn interface{}) sch.SchErrno {

	var sdl = sch.SchGetScheduler(ptn)
	var ptnQryMgr interface{}
	var ptnConMgr interface{}
	var ptnRutMgr interface{}
	var icb *qryInstCtrlBlock

	if sdl == nil {
		log.Debug("powerOn: SchGetScheduler failed")
		return sch.SchEnoInternal
	}

	if icb = sdl.SchGetUserDataArea(ptn).(*qryInstCtrlBlock); icb == nil {
		log.Debug("powerOn: impossible nil instance control block")
		return sch.SchEnoInternal
	}

	if _, ptnQryMgr = sdl.SchGetUserTaskNode(QryMgrName); ptnQryMgr == nil {
		log.Debug("powerOn: nil query manager")
		return sch.SchEnoInternal
	}

	if _, ptnConMgr = sdl.SchGetUserTaskNode(ConMgrName); ptnConMgr == nil {
		log.Debug("powerOn: nil connection manager")
		return sch.SchEnoInternal
	}

	if _, ptnRutMgr = sdl.SchGetUserTaskNode(RutMgrName); ptnRutMgr == nil {
		log.Debug("powerOn: nil route manager")
		return sch.SchEnoInternal
	}

	icb.status = qisInited
	icb.ptnQryMgr = ptnQryMgr
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
	log.Debug("powerOff: task will be done ...")
	return qryInst.icb.sdl.SchTaskDone(qryInst.icb.ptnInst, sch.SchEnoKilled)
}

//
// Start instance handler
//
func (qryInst *QryInst)startReq() sch.SchErrno {

	icb := qryInst.icb

	if icb.status != qisInited {
		log.Debug("startReq: state mismatched: %d", icb.status)
		return sch.SchEnoUserTask
	}

	msg := sch.SchMessage{}
	req := sch.MsgDhtConMgrConnectReq{
		Task:		icb.ptnInst,
		Peer:		&icb.to,
		IsBlind:	false,
	}

	log.Debug("startReq: ask connection manager for peer, " +
		"inst: %s, req: %+v", qryInst.icb.name, req)

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

		log.Debug("startReq: SchSetTimer failed, eno: %d", eno)

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
		log.Debug("stopReq: mismatched")
		return sch.SchEnoMismatched
	}

	log.Debug("stopReq: stopped for eno: %d, target: %x, peer: %x",
		msg.Eno, msg.Target, msg.Peer)

	if icb.status == qisWaitConnect  || icb.status == qisWaitResponse {

		if icb.qTid != sch.SchInvalidTid {
			sdl.SchKillTimer(icb.ptnInst, icb.qTid)
			icb.qTid = sch.SchInvalidTid
		}

		req := sch.MsgDhtConMgrCloseReq {
			Task:	icb.sdl.SchGetTaskName(icb.ptnInst),
			Peer:	&icb.to,
		}

		sdl.SchMakeMessage(&schMsg, icb.ptnInst, icb.ptnConMgr, sch.EvDhtConMgrCloseReq, &req)
		sdl.SchSendMessage(&schMsg)
	}

	rsp := sch.MsgDhtQryInstStopRsp {
		To:		icb.to,
		Target:	icb.target,
	}

	sdl.SchMakeMessage(&schMsg, icb.ptnInst, icb.ptnQryMgr, sch.EvDhtQryInstStopRsp, &rsp)
	return sdl.SchSendMessage(&schMsg)
}

//
// instance timer handler
//
func (qryInst *QryInst)icbTimerHandler(msg *QryInst) sch.SchErrno {

	if msg == nil {
		log.Debug("icbTimerHandler: invalid parameter")
		return sch.SchEnoParameter
	}

	dht := qryInst.icb.sdl.SchGetP2pCfgName()

	log.Debug("icbTimerHandler: " +
		"query instance timer expired, dht: %s, inst: %s",
		dht, qryInst.icb.name)

	if qryInst != msg {
		log.Debug("icbTimerHandler: dht: %s, instance pointer mismatched", dht)
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
		log.Debug("icbTimerHandler:" +
			"mismatched, dht: %s, status: %d, qTid: %d",
			dht, icb.status, icb.qTid)
		return sch.SchEnoMismatched
	}

	//
	// if we are waiting connection to be established, we request the connection manager
	// to abandon the connect-procedure. when this request received, the connection manager
	// should check if the connection had been established and route talbe updated, if ture,
	// then do not care this request, else it should close the connection and free all
	// resources had been allocated to the connection instance.
	//

	if icb.status == qisWaitConnect || icb.status == qisWaitResponse {
		req := sch.MsgDhtConMgrCloseReq{
			Task:	icb.sdl.SchGetTaskName(icb.ptnInst),
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
		Eno:	DhtEnoTimeout.GetEno(),
		Seens:	[]config.Node{icb.to},
		Duras:	[]time.Duration{-1},
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

	if msg == nil {
		log.Debug("connectRsp: invalid parameter")
		return sch.SchEnoParameter
	}

	log.Debug("connectRsp: msg: %+v", *msg)

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

	if icb.qTid != sch.SchInvalidTid {

		sdl.SchKillTimer(icb.ptnInst, icb.qTid)
		icb.qTid = sch.SchInvalidTid
	}

	if msg.Eno != DhtEnoNone.GetEno() && msg.Eno != DhtEnoDuplicated.GetEno() {

		log.Debug("connectRsp:" +
			"connect failed, eno: %d, peer: %+V",
			msg.Eno, *msg.Peer)

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

	eno, pkg := qryInst.setupQryPkg()

	if eno != DhtEnoNone {

		log.Debug("connectRsp: setupQryPkg failed, eno: %d", eno)

		ind.Status = qisDone
		icb.status = qisDone

		sdl.SchMakeMessage(&schMsg, icb.ptnInst, icb.ptnQryMgr, sch.EvDhtQryInstStatusInd, &ind)
		sdl.SchSendMessage(&schMsg)
		sdl.SchTaskDone(icb.ptnInst, sch.SchEnoKilled)

		return sch.SchEnoUserTask
	}

	log.Debug("connectRsp: setupQryPkg ok, " +
		"inst: %s, icb.qryReq: %+v", icb.name, *icb.qryReq)

	sendReq.Task = icb.ptnInst
	sendReq.Peer = &icb.to
	sendReq.Data = pkg

	var waitMid = map[int]int {
		MID_FINDNODE:			MID_NEIGHBORS,
		MID_GETPROVIDER_REQ:	MID_GETPROVIDER_RSP,
		MID_GETVALUE_REQ:		MID_GETVALUE_RSP,
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

	sdl.SchMakeMessage(&schMsg, icb.ptnInst, icb.ptnConMgr, sch.EvDhtConMgrSendReq, &sendReq)
	sdl.SchSendMessage(&schMsg)

	//
	// for "put-value" or "put-provider", we should send indication to query manager
	// as following, since no responses is expected from peer in this cases.
	// notice: the dht package might still not be sent at this moment, firstly it will
	// be put into pending queue of a connection instance.
	//

	if icb.qryReq.ForWhat == MID_PUTVALUE || icb.qryReq.ForWhat == MID_PUTPROVIDER {

		//
		// tell query manager about result
		//

		fwMap := map[int] int {
			MID_PUTVALUE:		sch.EvDhtMgrPutValueReq,
			MID_PUTPROVIDER:	sch.EvDhtMgrPutProviderReq,
		}
		fw := fwMap[icb.qryReq.ForWhat]

		indResult := sch.MsgDhtQryInstResultInd{
			From:		icb.to,
			Target:		icb.target,
			Latency:	icb.conEndTime.Sub(icb.conBegTime),
			ForWhat:	fw,
			Peers:		[]*config.Node{&icb.to},
			Provider:	nil,
			Value:		nil,
			Pcs:		[]int{pcsConnYes},
		}

		schMsg := sch.SchMessage{}
		sdl.SchMakeMessage(&schMsg, icb.ptnInst, icb.ptnQryMgr, sch.EvDhtQryInstResultInd, &indResult)
		return sdl.SchSendMessage(&schMsg)
	}

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

	schEno, tid := sdl.SchSetTimer(icb.ptnInst, &td)
	if schEno != sch.SchEnoNone || tid == sch.SchInvalidTid {
		log.Debug("connectRsp: SchSetTimer failed, eno: %d, tid: %d", eno, tid)
		return schEno
	}

	icb.qTid = tid

	return sch.SchEnoNone
}

//
// Incoming DHT messages handler
//
func (qryInst *QryInst)protoMsgInd(msg *sch.MsgDhtQryInstProtoMsgInd) sch.SchErrno {

	icb := qryInst.icb
	icb.endTime = time.Now()
	schMsg := sch.SchMessage{}

	switch msg.ForWhat {

	case sch.EvDhtConInstNeighbors:

		nbs, ok := msg.Msg.(*Neighbors)
		if !ok {
			log.Debug("protoMsgInd: mismatched type Neighbors")
			return sch.SchEnoMismatched
		}

		ind := sch.MsgDhtQryInstResultInd {
			From:		nbs.From,
			Target:		icb.target,
			ForWhat:	msg.ForWhat,
			Latency:	icb.endTime.Sub(icb.begTime),
			Peers:		nbs.Nodes,
			Provider:	nil,
			Value:		nil,
			Pcs:		nbs.Pcs,
		}

		icb.sdl.SchMakeMessage(&schMsg, icb.ptnInst, icb.ptnQryMgr, sch.EvDhtQryInstResultInd, &ind)

	case sch.EvDhtConInstGetValRsp:

		gvr, ok := msg.Msg.(*GetValueRsp)
		if !ok {
			log.Debug("protoMsgInd: mismatched type GetValueRsp")
			return sch.SchEnoMismatched
		}

		if gvr.Value != nil {

			if bytes.Equal(gvr.Value.Key, icb.target[0:]) == false {
				log.Debug("protoMsgInd: key mismatched")
				return sch.SchEnoMismatched
			}

			ind := sch.MsgDhtQryInstResultInd{
				From:		gvr.From,
				Target:		icb.target,
				ForWhat:	msg.ForWhat,
				Latency:	icb.endTime.Sub(icb.begTime),
				Peers:		nil,
				Provider:	nil,
				Value:		gvr.Value.Val,
				Pcs:		gvr.Pcs,
			}

			icb.sdl.SchMakeMessage(&schMsg, icb.ptnInst, icb.ptnQryMgr, sch.EvDhtQryInstResultInd, &ind)

		} else {

			ind := sch.MsgDhtQryInstResultInd{
				From:		gvr.From,
				Target:		icb.target,
				ForWhat:	msg.ForWhat,
				Latency:	icb.endTime.Sub(icb.begTime),
				Peers:		gvr.Nodes,
				Provider:	nil,
				Value:		nil,
				Pcs:		gvr.Pcs,
			}

			icb.sdl.SchMakeMessage(&schMsg, icb.ptnInst, icb.ptnQryMgr, sch.EvDhtQryInstResultInd, &ind)
		}


	case sch.EvDhtConInstGetProviderRsp:

		gpr, ok := msg.Msg.(*GetProviderRsp)
		if !ok {
			log.Debug("protoMsgInd: mismatched type GetProviderRsp")
			return sch.SchEnoMismatched
		}

		if gpr.Provider != nil {

			ind := sch.MsgDhtQryInstResultInd{
				From:		gpr.From,
				Target:		icb.target,
				ForWhat:	msg.ForWhat,
				Latency:	icb.endTime.Sub(icb.begTime),
				Peers:		nil,
				Provider:	(*sch.Provider)(gpr.Provider),
				Value:		nil,
				Pcs:		gpr.Pcs,
			}

			icb.sdl.SchMakeMessage(&schMsg, icb.ptnInst, icb.ptnQryMgr, sch.EvDhtQryInstResultInd, &ind)

		} else {

			ind := sch.MsgDhtQryInstResultInd{
				From:		gpr.From,
				Target:		icb.target,
				ForWhat:	msg.ForWhat,
				Latency:	icb.endTime.Sub(icb.begTime),
				Peers:		gpr.Nodes,
				Provider:	nil,
				Value:		nil,
				Pcs:		gpr.Pcs,
			}

			icb.sdl.SchMakeMessage(&schMsg, icb.ptnInst, icb.ptnQryMgr, sch.EvDhtQryInstResultInd, &ind)
		}

	default:
		log.Debug("protoMsgInd: mismatched, ForWhat: %d", msg.ForWhat)
		return sch.SchEnoMismatched
	}

	return icb.sdl.SchSendMessage(&schMsg)
}

//
// Tx status indication handler
//
func (qryInst *QryInst)conInstTxInd(msg *sch.MsgDhtConInstTxInd) sch.SchErrno {
	dht := qryInst.icb.sdl.SchGetP2pCfgName()
	if msg == nil {
		log.Debug("conInstTxInd： invalid parameter, dht: %s, inst: %s", dht, qryInst.icb.name)
		return sch.SchEnoParameter
	}
	log.Debug("conInstTxInd： dht: %s, inst: %s, msg: %+v", dht, qryInst.icb.name, *msg)
	return sch.SchEnoNone
}

//
// Setup the package for query by protobuf schema
//
func (qryInst *QryInst)setupQryPkg() (DhtErrno, *DhtPackage) {

	icb := qryInst.icb
	forWhat := icb.qryReq.ForWhat
	dhtMsg := DhtMessage{Mid:MID_UNKNOWN}
	dhtPkg := DhtPackage{}

	if forWhat == MID_PUTPROVIDER {

		msg := icb.qryReq.Msg.(*sch.MsgDhtPrdMgrAddProviderReq)
		pp := PutProvider {
			From:   *icb.local,
			To:     icb.to,
			Provider: &DhtProvider{Key:msg.Key, Nodes:[]*config.Node{&msg.Prd}, Extra:nil},
			Id:     icb.qryReq.Seq,
			Extra:  nil,
		}

		dhtMsg.Mid = MID_PUTPROVIDER
		dhtMsg.PutProvider = &pp

	} else if forWhat == MID_PUTVALUE {

		msg := icb.qryReq.Msg.(*sch.MsgDhtDsMgrAddValReq)
		pv := PutValue {
			From:   *icb.local,
			To:     icb.to,
			Values:	[]DhtValue{{Key:msg.Key, Val:msg.Val, Extra:nil}},
			Id:     icb.qryReq.Seq,
			Extra:  nil,
		}

		dhtMsg.Mid = MID_PUTVALUE
		dhtMsg.PutValue = &pv

	} else if forWhat == MID_FINDNODE {

		fn := FindNode {
			From:   *icb.local,
			To:     icb.to,
			Target: icb.target,
			Id:     icb.qryReq.Seq,
			Extra:  nil,
		}

		dhtMsg.Mid = MID_FINDNODE
		dhtMsg.FindNode = &fn

	} else if forWhat == MID_GETVALUE_REQ {

		gvr := GetValueReq {
			From:   *qryInst.icb.local,
			To:     qryInst.icb.to,
			Key:	icb.target[0:],
			Id:     icb.qryReq.Seq,
			Extra:  nil,
		}

		dhtMsg.Mid = MID_GETVALUE_REQ
		dhtMsg.GetValueReq = &gvr

	} else if forWhat == MID_GETPROVIDER_REQ {

		gpr := GetProviderReq {
			From:   *qryInst.icb.local,
			To:     qryInst.icb.to,
			Key:	icb.target[0:],
			Id:     icb.qryReq.Seq,
			Extra:  nil,
		}

		dhtMsg.Mid = MID_GETPROVIDER_REQ
		dhtMsg.GetProviderReq = &gpr

	} else {
		log.Debug("setupQryPkg: unknown what's for")
		return DhtEnoMismatched, nil
	}

	if eno := dhtMsg.GetPackage(&dhtPkg); eno != DhtEnoNone {
		log.Debug("setupQryPkg: GetPackage failed, eno: %d", eno)
		return eno, nil
	}

	return DhtEnoNone, &dhtPkg
}
