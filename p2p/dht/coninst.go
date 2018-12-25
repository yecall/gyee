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
	"net"
	"time"
	"sync"
	"io"
	"fmt"
	"container/list"
	ggio	"github.com/gogo/protobuf/io"
	pb		"github.com/yeeco/gyee/p2p/dht/pb"
	config	"github.com/yeeco/gyee/p2p/config"
	sch		"github.com/yeeco/gyee/p2p/scheduler"
	p2plog	"github.com/yeeco/gyee/p2p/logger"
)


//
// debug
//
type coninstLogger struct {
	debug__		bool
}

var ciLog = coninstLogger {
	debug__:	true,
}

func (log coninstLogger)Debug(fmt string, args ... interface{}) {
	if log.debug__ {
		p2plog.Debug(fmt, args ...)
	}
}

//
// Dht connection instance
//
type ConInst struct {
	sdl				*sch.Scheduler			// pointer to scheduler
	name			string					// task name
	tep				sch.SchUserTaskEp		// task entry
	local			*config.Node			// pointer to local node specification
	ptnMe			interface{}				// pointer to myself task node
	ptnDhtMgr		interface{}				// pointer to dht manager task node
	ptnRutMgr		interface{}				// pointer to route manager task node
	ptnDsMgr		interface{}				// pointer to data store manager task node
	ptnPrdMgr		interface{}				// pointer to provider manager task node
	ptnConMgr		interface{}				// pointer to connection manager task node

	//
	// Notice: this is the pointer to the task which asks to establish this connection instance,
	// but this owner task might have been done while the connection instance might be still alived,
	// in current implement, before handshake is completed, this pointer is to the owner task, and
	// after that, this pointer is senseless.
	//

	srcTaskName		string					// name of the owner source task
	ptnSrcTsk		interface{}				// for outbound, the source task requests the connection

	status			conInstStatus			// instance status
	hsTimeout		time.Duration			// handshake timeout value
	cid				conInstIdentity			// connection instance identity
	con				net.Conn				// connection
	iow				ggio.WriteCloser		// IO writer
	ior				ggio.ReadCloser			// IO reader
	dir				ConInstDir				// connection instance directory
	hsInfo			conInstHandshakeInfo	// handshake information
	txPending		*list.List				// pending packages to be sent
	txWaitRsp		*list.List				// packages had been sent but waiting for response from peer
	txLock			sync.Mutex				// tx lock
	txChan			chan interface{}		// tx pendings signal
	txDone			chan int				// tx-task done signal
	rxDone			chan int				// rx-task done signal
	cbRxLock		sync.Mutex				// lock for data plane callback
	cbfRxData		ConInstRxDataCallback	// data plane callback entry
	isBlind			bool					// is blind connection instance
	txPkgCnt		int64					// statistics for number of packages sent
	rxPkgCnt		int64					// statistics for number off package received
}

//
// Call back type for rx data of protocols than PID_DHT
//
type ConInstRxDataCallback func(conInst interface{}, pid uint32, msg interface{})int

//
// Connection instance identity
//
type conInstIdentity struct {
	nid			config.NodeID		// node identity
	dir			ConInstDir			// connection direction
}

//
// Connection instance status
//
const (
	CisNull			= iota			// null, not inited
	CisConnecting					// connecting
	CisConnected					// connected
	CisAccepted						// accepted
	CisInHandshaking				// handshaking
	CisHandshaked					// handshaked
	CisInService					// in service
	CisClosed						// closed
	CisOutOfService					// out of service but is not closed
)

type conInstStatus int

//
// Connection instance direction
//
const (
	ConInstDirInbound	= 0			// out from local
	ConInstDirOutbound	= 1			// in from peer
	ConInstDirAllbound	= 2			// in & out
	ConInstDirUnknown	= -1		// not be initialized
)

type ConInstDir int

//
// Handshake information
//
type conInstHandshakeInfo struct {
	peer		config.Node			// peer node identity
	extra		interface{}			// extra information
}

//
// Outcoming package
//
type conInstTxPkg struct {
	taskName	string				// task name
	task		interface{}			// pointer to owner task node

	responsed	chan bool			// wait response from peer signal. notice: this chan is not applied for
									// syncing as a signal really in current implement, instead, it is used
									// as a flag for response checking, if not nil, a package sent would be
									// push into queue (ConInst.txWaitRsp), and timer start for response.

	waitMid		int					// wait message identity
	waitSeq		int64				// wait message sequence number
	submitTime	time.Time			// time the payload submitted
	payload		interface{}			// payload buffer
	txTid		int					// wait peer response timer
}

//
// Constants related to performance
//
const (
	ciTxPendingQueueSize = 64				// max tx-pending queue size
	ciConn2PeerTimeout = time.Second * 16	// Connect to peer timeout vale
	ciMaxPackageSize = 1024 * 1024			// bytes
	ciTxTimerDuration = time.Second * 8		// tx timer duration
	ciTxMaxWaitResponseSize = 32			// tx max wait peer response queue size
)

//
// Create connection instance
//
func newConInst(postFixed string, isBlind bool) *ConInst {

	conInst := ConInst {
		name:				"conInst" + postFixed,
		status:				CisNull,
		dir:				ConInstDirUnknown,
		txPending:			list.New(),
		txWaitRsp:			list.New(),
		isBlind:			isBlind,
		txPkgCnt:			0,
		rxPkgCnt:			0,
	}

	conInst.tep = conInst.conInstProc

	return &conInst
}

//
// Entry point exported to shceduler
//
func (conInst *ConInst)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return conInst.tep(ptn, msg)
}

//
// Connection instance entry
//
func (conInst *ConInst)conInstProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	if ptn == nil || msg == nil {
		ciLog.Debug("conInstProc: " +
			"invalid parameters, ptn: %p, msg: %p",
			ptn, msg)
		return sch.SchEnoParameter
	}

	var eno = sch.SchEnoUnknown

	switch msg.Id {

	case sch.EvSchPoweron:
		eno = conInst.poweron(ptn)

	case sch.EvSchPoweroff:
		eno = conInst.poweroff(ptn)

	case sch.EvDhtConInstHandshakeReq:
		eno = conInst.handshakeReq(msg.Body.(*sch.MsgDhtConInstHandshakeReq))

	case sch.EvDhtConInstCloseReq:
		eno = conInst.closeReq(msg.Body.(*sch.MsgDhtConInstCloseReq))

	case sch.EvDhtConInstTxDataReq:
		eno = conInst.txDataReq(msg.Body.(*sch.MsgDhtConInstTxDataReq))

	case sch.EvDhtRutMgrNearestRsp:
		eno = conInst.rutMgrNearestRsp(msg.Body.(*sch.MsgDhtRutMgrNearestRsp))

	case sch.EvDhtConInstTxTimer:
		eno = conInst.txTimerHandler(msg.Body.(*list.Element))

	case sch.EvDhtQryInstProtoMsgInd:
		eno = conInst.protoMsgInd(msg.Body.(*sch.MsgDhtQryInstProtoMsgInd))

	default:
		ciLog.Debug("conInstProc: unknown event: %d", msg.Id)
		return sch.SchEnoParameter
	}

	return eno
}

//
// Poweron handler
//
func (conInst *ConInst)poweron(ptn interface{}) sch.SchErrno {

	//
	// initialization for an instance had been done when this task is created,
	// not so much to do, and here for a inbound instance, it still not be mapped
	// into connection manager's instance table, so its' status should not be
	// reported at this moment.
	//

	if conInst.ptnMe != ptn {
		ciLog.Debug("poweron: task mismatched")
		return sch.SchEnoMismatched
	}

	if conInst.dir == ConInstDirInbound {
		if conInst.statusReport() != DhtEnoNone {
			return sch.SchEnoUserTask
		}
		conInst.status = CisConnected
		return sch.SchEnoNone
	}

	if conInst.dir == ConInstDirOutbound {
		if conInst.statusReport() != DhtEnoNone {
			return sch.SchEnoUserTask
		}
		return sch.SchEnoNone
	}

	return sch.SchEnoUserTask
}

//
// Poweroff handler
//
func (conInst *ConInst)poweroff(ptn interface{}) sch.SchErrno {

	if conInst.ptnMe != ptn {
		ciLog.Debug("poweroff: task mismatched")
		return sch.SchEnoMismatched
	}

	ciLog.Debug("poweroff: task will be done ...")

	conInst.cleanUp(DhtEnoScheduler.GetEno())
	conInst.sdl.SchTaskDone(conInst.ptnMe, sch.SchEnoKilled)

	return sch.SchEnoNone
}

//
// Handshake-request handler
//
func (conInst *ConInst)handshakeReq(msg *sch.MsgDhtConInstHandshakeReq) sch.SchErrno {

	//
	// if handshake failed, the instance task should done itself, and send handshake
	// response message to connection manager task.
	//

	dht := conInst.sdl.SchGetP2pCfgName()

	rsp := sch.MsgDhtConInstHandshakeRsp {
		Eno:	DhtEnoUnknown.GetEno(),
		Inst:	conInst,
		Peer:	nil,
		Dir:	int(conInst.dir),
		HsInfo:	nil,
		Dur:	time.Duration(-1),
	}

	rsp2ConMgr := func() sch.SchErrno {
		if conInst.con != nil {
			ciLog.Debug("handshakeReq: rsp2ConMgr, "+
				"dht: %s, inst: %s, dir: %d, localAddr: %s, remoteAddr: %s, rsp: %+v",
				dht, conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String(), rsp)
		} else {
			ciLog.Debug("handshakeReq: rsp2ConMgr, "+
				"dht: %s, inst: %s, dir: %d, localAddr: %s, remoteAddr: %s, rsp: %+v",
				dht, conInst.name, conInst.dir, "none", "none", rsp)
		}
		schMsg := sch.SchMessage{}
		conInst.sdl.SchMakeMessage(&schMsg, conInst.ptnMe, conInst.ptnConMgr, sch.EvDhtConInstHandshakeRsp, &rsp)
		return conInst.sdl.SchSendMessage(&schMsg)
	}

	//
	// connect to peer if it's not
	//

	if conInst.con == nil && conInst.dir == ConInstDirOutbound {

		conInst.status = CisConnecting
		conInst.statusReport()

		if eno := conInst.connect2Peer(); eno != DhtEnoNone {
			peer := conInst.hsInfo.peer
			hsInfo := conInst.hsInfo
			rsp.Eno = int(eno)
			rsp.Peer = &peer
			rsp.Inst = conInst
			rsp.HsInfo = &hsInfo

			return rsp2ConMgr()
		}

		ciLog.Debug("handshakeReq: connect ok, dht: %s, inst: %s, dir: %d, localAddr: %s, remoteAddr: %s",
		dht, conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String())

		conInst.status = CisConnected
		conInst.statusReport()
	}

	//
	// handshake
	//

	conInst.status = CisInHandshaking
	conInst.hsTimeout = msg.DurHs
	conInst.statusReport()

	if conInst.dir == ConInstDirOutbound {

		if eno := conInst.outboundHandshake(); eno != DhtEnoNone {

			peer := conInst.hsInfo.peer
			hsInfo := conInst.hsInfo
			rsp.Eno = int(eno)
			rsp.Peer = &peer
			rsp.Inst = conInst
			rsp.HsInfo = &hsInfo

			return rsp2ConMgr()
		}

	} else {

		ciLog.Debug("handshakeReq: dht: %s, inst: %s, dir: %d, localAddr: %s, remoteAddr: %s",
			dht, conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String())

		if eno := conInst.inboundHandshake(); eno != DhtEnoNone {

			rsp.Eno = int(eno)
			rsp.Peer = nil
			rsp.HsInfo = nil
			rsp.Inst = conInst

			return rsp2ConMgr()
		}
	}

	conInst.status = CisHandshaked
	conInst.statusReport()

	rsp.Eno = DhtEnoNone.GetEno()
	rsp.Peer = &conInst.hsInfo.peer
	rsp.HsInfo = &conInst.hsInfo
	rsp2ConMgr()

	//
	// service startup
	//

	ciLog.Debug("handshakeReq: ok, try to start tx and rx for connection instance, " +
		"dht: %s, inst: %s, dir: %d, localAddr: %s, remoteAddr: %s",
		dht, conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String())

	conInst.txTaskStart()
	conInst.rxTaskStart()

	conInst.status = CisInService
	conInst.statusReport()

	return sch.SchEnoNone
}

//
// Instance-close-request handler
//
func (conInst *ConInst)closeReq(msg *sch.MsgDhtConInstCloseReq) sch.SchErrno {

	if conInst.status != CisHandshaked &&
		conInst.status != CisInService &&
		conInst.dir != ConInstDirOutbound {

		ciLog.Debug("closeReq: " +
			"status mismatched, dir: %d, status: %d",
			conInst.dir, conInst.status)
		return sch.SchEnoMismatched
	}

	if *msg.Peer != conInst.hsInfo.peer.ID {
		ciLog.Debug("closeReq: peer node identity mismatched")
		return sch.SchEnoMismatched
	}

	ciLog.Debug("closeReq: " +
		"connection will be closed, why: %d, peer: %x",
		msg.Why, *msg.Peer)

	conInst.cleanUp(DhtEnoNone.GetEno())
	conInst.status = CisClosed

	schMsg := sch.SchMessage{}
	rsp := sch.MsgDhtConInstCloseRsp{
		Peer:	&conInst.hsInfo.peer.ID,
		Dir:	int(conInst.dir),
	}
	conInst.sdl.SchMakeMessage(&schMsg, conInst.ptnMe, conInst.ptnConMgr, sch.EvDhtConInstCloseRsp, &rsp)
	conInst.sdl.SchSendMessage(&schMsg)

	return conInst.sdl.SchTaskDone(conInst.ptnMe, sch.SchEnoUserTask)
}

//
// Send-data-request handler
//
func (conInst *ConInst)txDataReq(msg *sch.MsgDhtConInstTxDataReq) sch.SchErrno {

	pkg := conInstTxPkg {
		task:		msg.Task,
		responsed:	nil,
		waitMid:	-1,
		waitSeq:	-1,
		submitTime:	time.Now(),
		payload:	msg.Payload,
	}

	if msg.WaitRsp == true {
		pkg.responsed = make(chan bool, 1)
		pkg.waitMid = msg.WaitMid
		pkg.waitSeq = msg.WaitSeq
	}

	if eno := conInst.txPutPending(&pkg); eno != DhtEnoNone {
		ciLog.Debug("txDataReq: txPutPending failed, eno: %d", eno)
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

//
// Nearest response handler
//
func (conInst *ConInst)rutMgrNearestRsp(msg *sch.MsgDhtRutMgrNearestRsp) sch.SchErrno {

	//
	// notice: here the response must for "Find_NODE" from peers, see function
	// dispatch for more please.
	//

	if msg == nil {
		ciLog.Debug("rutMgrNearestRsp: invalid parameters")
		return sch.SchEnoParameter
	}

	ciLog.Debug("rutMgrNearestRsp: msg: %+v", msg)

	var dhtMsg = DhtMessage {
		Mid: MID_UNKNOWN,
	}
	var nodes []*config.Node
	bns, ok := msg.Peers.([]*rutMgrBucketNode)
	if !ok {
		ciLog.Debug("rutMgrNearestRsp: invalid parameters")
		return sch.SchEnoParameter
	}

	for idx := 0; idx < len(bns); idx++ {
		nodes = append(nodes, &bns[idx].node)
	}

	if msg.ForWhat == MID_FINDNODE {

		if msg.Msg == nil {
			ciLog.Debug("rutMgrNearestRsp: original message is nil")
			return sch.SchEnoMismatched
		}

		findNode, ok := msg.Msg.(*FindNode)
		if !ok {
			ciLog.Debug("rutMgrNearestRsp: invalid original message type")
			return sch.SchEnoMismatched
		}

		nbs := Neighbors {
			From:		*conInst.local,
			To:    		conInst.hsInfo.peer,
			Nodes:		nodes,
			Pcs:		msg.Pcs.([]int),
			Id:			findNode.Id,
			Extra:		nil,
		}

		dhtMsg = DhtMessage{
			Mid:       MID_NEIGHBORS,
			Neighbors: &nbs,
		}

	} else if msg.ForWhat == MID_GETPROVIDER_REQ {

		ciLog.Debug("rutMgrNearestRsp: for MID_GETPROVIDER_REQ should not come here")
		return sch.SchEnoMismatched

	} else if msg.ForWhat == MID_GETVALUE_REQ {

		ciLog.Debug("rutMgrNearestRsp: for MID_GETVALUE_REQ should not come here")
		return sch.SchEnoMismatched

	} else {

		ciLog.Debug("rutMgrNearestRsp: unknown what's for")
		return sch.SchEnoMismatched
	}

	dhtPkg := DhtPackage{}
	if eno := dhtMsg.GetPackage(&dhtPkg); eno != DhtEnoNone {
		ciLog.Debug("rutMgrNearestRsp: GetPackage failed, eno: %d", eno)
		return sch.SchEnoUserTask
	}

	txPkg := conInstTxPkg {
		task:		conInst.ptnMe,
		responsed:	nil,
		waitMid:	-1,
		waitSeq:	-1,
		submitTime:	time.Now(),
		payload:	&dhtPkg,
	}

	if eno := conInst.txPutPending(&txPkg); eno != DhtEnoNone {
		ciLog.Debug("rutMgrNearestRsp: txPutPending failed, eno: %d", eno)
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

//
// Map connection instance status to "peer connection status"
//
func conInstStatus2PCS(cis conInstStatus) conMgrPeerConnStat {
	cis2pcs := map[conInstStatus] conMgrPeerConnStat {
		CisNull:			pcsConnNo,
		CisConnected:		pcsConnNo,
		CisInHandshaking:	pcsConnNo,
		CisHandshaked:		pcsConnYes,
		CisInService:		pcsConnYes,
		CisClosed:			pcsConnNo,
	}
	return cis2pcs[cis]
}

//
// Put outbound package into pending queue
//
func (conInst *ConInst)txPutPending(pkg *conInstTxPkg) DhtErrno {

	dht := conInst.sdl.SchGetP2pCfgName()

	if pkg == nil {
		ciLog.Debug("txPutPending: invalid parameter, dht: %s, inst: %s, hsInfo: %+v, local: %+v",
			dht, conInst.name, conInst.hsInfo, *conInst.local)
		return DhtEnoParameter
	}

	conInst.txLock.Lock()
	defer conInst.txLock.Unlock()

	if conInst.txPending.Len() >= ciTxPendingQueueSize {
		ciLog.Debug("txPutPending: pending queue full, dht: %s, inst: %s, hsInfo: %+v, local: %+v",
			dht, conInst.name, conInst.hsInfo, *conInst.local)
		return DhtEnoResource
	}

	if conInst.txWaitRsp.Len() >= ciTxMaxWaitResponseSize {
		ciLog.Debug("txPutPending: waiting response queue full, dht: %s, inst: %s, hsInfo: %+v, local: %+v",
			dht, conInst.name, conInst.hsInfo, *conInst.local)
		return DhtEnoResource
	}

	ciLog.Debug("txPutPending: put, dht: %s, inst: %s, hsInfo: %+v, local: %+v, waitMid: %d, waitSeq: %d",
		dht, conInst.name, conInst.hsInfo, *conInst.local, pkg.waitMid, pkg.waitSeq)

	conInst.txPending.PushBack(pkg)
	conInst.txChan<-pkg

	return DhtEnoNone
}

//
// Set timer for tx-package which would wait response from peer
//
func (conInst *ConInst)txSetTimer(el *list.Element) DhtErrno {

	dht := conInst.sdl.SchGetP2pCfgName()

	if el == nil {
		ciLog.Debug("txSetTimer: invalid parameter, dht: %s, inst: %s, hsInfo: %+v, local: %+v",
			dht, conInst.name, conInst.hsInfo, *conInst.local)
		return DhtEnoParameter
	}

	txPkg, ok := el.Value.(*conInstTxPkg)
	if !ok {
		ciLog.Debug("txSetTimer: invalid parameter, dht: %s, inst: %s, hsInfo: %+v, local: %+v",
			dht, conInst.name, conInst.hsInfo, *conInst.local)
		return DhtEnoMismatched
	}

	var td = sch.TimerDescription {
		Name:	fmt.Sprintf("%s%d", conInst.name, "_txTimer"),
		Utid:	sch.DhtConInstTxTimerId,
		Tmt:	sch.SchTmTypeAbsolute,
		Dur:	ciTxTimerDuration,
		Extra:	el,
	}

	eno, tid := conInst.sdl.SchSetTimer(conInst.ptnMe, &td)
	if eno != sch.SchEnoNone {
		ciLog.Debug("txSetTimer: invalid parameter, dht: %s, inst: %s, eno: %d, hsInfo: %+v, local: %+v",
			dht, conInst.name, eno, conInst.hsInfo, *conInst.local)
		return DhtEnoScheduler
	}

	txPkg.txTid = tid

	return DhtEnoNone
}

//
// Tx timer expired event handler
//
func (conInst *ConInst)txTimerHandler(el *list.Element) sch.SchErrno {

	dht := conInst.sdl.SchGetP2pCfgName()

	if el == nil {
		ciLog.Debug("txTimerHandler: invalid parameter, dht: %s, inst: %s, hsInfo: %+v, local: %+v",
			dht, conInst.name, conInst.hsInfo, *conInst.local)
		return sch.SchEnoParameter
	}

	txPkg, ok := el.Value.(*conInstTxPkg)
	if !ok {
		ciLog.Debug("txTimerHandler: invalid parameter, dht: %s, inst: %s, hsInfo: %+v, local: %+v",
			dht, conInst.name, conInst.hsInfo, *conInst.local)
		return sch.SchEnoMismatched
	}

	ciLog.Debug("txTimerHandler: dht: %s, inst: %s, hsInfo: %+v, local: %+v, txPkg: %+v",
		dht, conInst.name, conInst.hsInfo, *conInst.local, *txPkg)

	conInst.status = CisClosed
	conInst.statusReport()

	conInst.cleanUp(int(DhtEnoTimeout))

	if eno := conInst.sdl.SchTaskDone(conInst.ptnMe, sch.SchEnoUserTask); eno != sch.SchEnoNone {
		ciLog.Debug("txTimerHandler: invalid parameter, dht: %s, inst: %s, eno: %d, hsInfo: %+v, local: %+v",
			dht, conInst.name, eno, conInst.hsInfo, *conInst.local)
		return eno
	}

	return sch.SchEnoNone
}

func (conInst *ConInst)protoMsgInd(msg *sch.MsgDhtQryInstProtoMsgInd) sch.SchErrno {

	var eno DhtErrno
	var txPkg *conInstTxPkg
	var schMsg = sch.SchMessage{}

	switch msg.ForWhat {

	case sch.EvDhtConInstNeighbors:
		nbs, _ := msg.Msg.(*Neighbors)
		eno, txPkg = conInst.checkTxWaitResponse(MID_NEIGHBORS, int64(nbs.Id))

	case sch.EvDhtConInstGetProviderRsp:
		gpr, _ := msg.Msg.(*GetProviderRsp)
		eno, txPkg = conInst.checkTxWaitResponse(MID_GETPROVIDER_RSP, int64(gpr.Id))

	case sch.EvDhtConInstGetValRsp:
		gvr, _ := msg.Msg.(*GetValueRsp)
		eno, txPkg = conInst.checkTxWaitResponse(MID_GETVALUE_RSP, int64(gvr.Id))

	default:
		ciLog.Debug("protoMsgInd: invalid indication, for: %d", msg.ForWhat)
		return sch.SchEnoParameter
	}

	if eno == DhtEnoNone && txPkg != nil {
		_, ptn := conInst.sdl.SchGetUserTaskNode(txPkg.taskName)
		if ptn != nil && ptn == txPkg.task {
			conInst.sdl.SchMakeMessage(&schMsg, conInst.ptnMe, txPkg.task, sch.EvDhtQryInstProtoMsgInd, msg)
			conInst.sdl.SchSendMessage(&schMsg)
		}
	}

	return sch.SchEnoNone
}

//
// Set current Tx pending
//
func (conInst *ConInst)txSetPending(txPkg *conInstTxPkg) (DhtErrno, *list.Element){
	conInst.txLock.Lock()
	defer conInst.txLock.Unlock()

	var el *list.Element = nil

	if txPkg != nil {

		txPkg.taskName = conInst.sdl.SchGetTaskName(txPkg.task)
		if len(txPkg.taskName) == 0 {
			ciLog.Debug("txSetPending: task without name")
			panic("txSetPending: task without name")
		}

		el = conInst.txWaitRsp.PushBack(txPkg)
	}

	return DhtEnoNone, el
}

//
// Start tx-task
//
func (conInst *ConInst)txTaskStart() DhtErrno {

	if conInst.txDone != nil {
		ciLog.Debug("txTaskStart: non-nil chan for done")
		return DhtEnoMismatched
	}
	conInst.txDone = make(chan int, 1)

	if conInst.txChan != nil {
		ciLog.Debug("txTaskStart: non-nil chan for txChan")
		return DhtEnoMismatched
	}
	conInst.txChan = make(chan interface{}, ciTxPendingQueueSize)

	go conInst.txProc()

	return DhtEnoNone
}

//
// Start rx-task
//
func (conInst *ConInst)rxTaskStart() DhtErrno {
	if conInst.rxDone != nil {
		ciLog.Debug("rxTaskStart: non-nil chan for done")
		return DhtEnoMismatched
	}
	conInst.rxDone = make(chan int, 1)
	go conInst.rxProc()
	return DhtEnoNone
}

//
// Stop tx-task
//
func (conInst *ConInst)txTaskStop(why int) DhtErrno {

	if conInst.txDone != nil {

		conInst.txDone<-why
		done := <-conInst.txDone
		close(conInst.txDone)
		conInst.txDone = nil
		close(conInst.txChan)
		conInst.txChan = nil

		return DhtErrno(done)
	}

	return DhtEnoNone
}

//
// Stop rx-task
//
func (conInst *ConInst)rxTaskStop(why int) DhtErrno {

	if conInst.rxDone != nil {

		conInst.rxDone<-why
		done := <-conInst.rxDone
		close(conInst.rxDone)
		conInst.rxDone = nil

		return DhtErrno(done)
	}

	return DhtEnoNone
}

//
// Cleanup the instance
//
func (conInst *ConInst)cleanUp(why int) DhtErrno {

	dht := conInst.sdl.SchGetP2pCfgName()
	ciLog.Debug("cleanUp: dht: %s, inst: %s, local: %+v, hsInfo: %+v, why: %d",
		dht, conInst.name, *conInst.local, conInst.hsInfo, why)

	conInst.txTaskStop(why)
	conInst.rxTaskStop(why)

	que := conInst.txWaitRsp

	for que.Len() != 0 {

		el := que.Front()

		if txPkg, ok := el.Value.(*conInstTxPkg); ok {

			if txPkg.txTid != sch.SchInvalidTid {
				conInst.sdl.SchKillTimer(conInst.ptnMe, txPkg.txTid)
			}

			//
			// check if task still lived to confirm it
			//

			eno, ptn := conInst.sdl.SchGetUserTaskNode(txPkg.taskName)
			if eno == sch.SchEnoNone && ptn != nil && ptn == txPkg.task {

				if txPkg.task != nil {

					schMsg := sch.SchMessage{}
					ind := sch.MsgDhtConInstTxInd {
						Eno:     DhtEnoTimeout.GetEno(),
						WaitMid: txPkg.waitMid,
						WaitSeq: txPkg.waitSeq,
					}

					conInst.sdl.SchMakeMessage(&schMsg, conInst.ptnMe, txPkg.task, sch.EvDhtConInstTxInd, &ind)
					conInst.sdl.SchSendMessage(&schMsg)
				}
			}

			//
			// close responsed channel if needed
			//

			if txPkg.responsed != nil {
				close(txPkg.responsed)
			}
		}

		que.Remove(el)
	}

	if conInst.con != nil {
		conInst.con.Close()
		conInst.con = nil
	}

	return DhtEnoNone
}

//
// Connect to peer
//
func (conInst *ConInst)connect2Peer() DhtErrno {

	dht := conInst.sdl.SchGetP2pCfgName()

	if conInst.dir != ConInstDirOutbound {
		ciLog.Debug("connect2Peer: mismatched direction: dht: %s, inst: %s, dir: %d",
			dht, conInst.name, conInst.dir)
		return DhtEnoInternal
	}

	peer := conInst.hsInfo.peer
	dialer := &net.Dialer{Timeout: ciConn2PeerTimeout}
	addr := &net.TCPAddr{IP: peer.IP, Port: int(peer.TCP)}

	ciLog.Debug("connect2Peer: try to connect, " +
		"dht: %s, inst: %s, dir: %d, local: %s, remote: %s",
		dht, conInst.name, conInst.dir,
		conInst.local.IP.String(),
		addr.String())

	var conn net.Conn
	var err error

	if conn, err = dialer.Dial("tcp", addr.String()); err != nil {
		ciLog.Debug("connect2Peer: " +
			"dial failed, dht: %s, inst: %s, local: %s, to: %s, err: %s",
			dht, conInst.name, conInst.dir, conInst.local.IP.String(),
			addr.String(), err.Error())
		return DhtEnoOs
	}

	conInst.con = conn
	r := conInst.con.(io.Reader)
	conInst.ior = ggio.NewDelimitedReader(r, ciMaxPackageSize)
	w := conInst.con.(io.Writer)
	conInst.iow = ggio.NewDelimitedWriter(w)

	ciLog.Debug("connect2Peer: connect ok, " +
		"dht: %s, inst: %s, dir: %d, local: %s, remote: %s",
		dht, conInst.name, conInst.dir,
		conn.LocalAddr().String(),
		conn.RemoteAddr().String())

	return DhtEnoNone
}

//
// Report instance status to connection manager
//
func (conInst *ConInst)statusReport() DhtErrno {

	//
	// notice: during the lifetime of the connection instance, the "Peer" might be
	// still not known at some time. for example, when just connection be accepted
	// and handshake procedure is not completed, so one must check the direction and
	// status of a connection instance to apply the "peer" information indicated by
	// the following message.
	//

	msg := sch.SchMessage{}
	ind := sch.MsgDhtConInstStatusInd {
		Peer:   &conInst.hsInfo.peer.ID,
		Dir:    int(conInst.dir),
		Status: int(conInst.status),
	}

	conInst.sdl.SchMakeMessage(&msg, conInst.ptnMe, conInst.ptnConMgr, sch.EvDhtConInstStatusInd, &ind)
	if conInst.sdl.SchSendMessage(&msg) != sch.SchEnoNone {
		return DhtEnoScheduler
	}

	return DhtEnoNone
}

//
// Outbound handshake
//
func (conInst *ConInst)outboundHandshake() DhtErrno {

	dht := conInst.sdl.SchGetP2pCfgName()
	ciLog.Debug("outboundHandshake: begin, dht: %s, inst: %s, dir: %d, local: %s, remote: %s",
		dht, conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String())

	dhtMsg := new(DhtMessage)
	dhtMsg.Mid = MID_HANDSHAKE
	dhtMsg.Handshake = &Handshake{
		Dir:		ConInstDirOutbound,
		NodeId:		conInst.local.ID,
		IP:			conInst.local.IP,
		UDP:		uint32(conInst.local.UDP),
		TCP:		uint32(conInst.local.TCP),
		ProtoNum:	1,
		Protocols:	[]DhtProtocol {
			{
				Pid:	uint32(PID_DHT),
				Ver:	DhtVersion,
			},
		},
	}

	pbPkg := dhtMsg.GetPbPackage()
	if pbPkg == nil {
		ciLog.Debug("outboundHandshake: GetPbPackage failed, " +
			"dht: %s, inst: %s, dir: %d, local: %s, remote: %s",
			dht, conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String())
		return DhtEnoSerialization
	}

	conInst.con.SetDeadline(time.Now().Add(conInst.hsTimeout))
	if err := conInst.iow.WriteMsg(pbPkg); err != nil {
		ciLog.Debug("outboundHandshake: WriteMsg failed, " +
			"dht: %s, inst: %s, dir: %d, local: %s, remote: %s, err: %s",
			dht, conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String(),
			"err: %s", err.Error())
		return DhtEnoSerialization
	}

	*pbPkg = pb.DhtPackage{}
	conInst.con.SetDeadline(time.Now().Add(conInst.hsTimeout))
	if err := conInst.ior.ReadMsg(pbPkg); err != nil {
		ciLog.Debug("outboundHandshake: ReadMsg failed, " +
			"dht: %s, inst: %s, dir: %d, local: %s, remote: %s, err: %s",
			dht, conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String(),
			err.Error())
		return DhtEnoSerialization
	}

	if *pbPkg.Pid != PID_DHT {
		ciLog.Debug("outboundHandshake: invalid pid, " +
			"dht: %s, inst: %s, dir: %d, local: %s, remote: %s, pid: %d",
			dht, conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String(),
			pbPkg.Pid)
		return DhtEnoProtocol
	}

	if *pbPkg.PayloadLength <= 0 {
		ciLog.Debug("outboundHandshake: invalid payload length, " +
			"dht: %s, inst: %s, dir: %d, local: %s, remote: %s, length: %d",
			dht, conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String(),
			*pbPkg.PayloadLength)
		return DhtEnoProtocol
	}

	if len(pbPkg.Payload) != int(*pbPkg.PayloadLength) {
		ciLog.Debug("outboundHandshake: payload length mismatched, " +
			"dht: %s, inst: %s, dir: %d, local: %s, remote: %s, PlLen: %d, real: %d",
			dht, conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String(),
			*pbPkg.PayloadLength, len(pbPkg.Payload))
		return DhtEnoProtocol
	}

	dhtPkg := new(DhtPackage)
	dhtPkg.Pid = uint32(*pbPkg.Pid)
	dhtPkg.PayloadLength = *pbPkg.PayloadLength
	dhtPkg.Payload = pbPkg.Payload

	*dhtMsg = DhtMessage{}
	if eno := dhtPkg.GetMessage(dhtMsg); eno != DhtEnoNone {
		ciLog.Debug("outboundHandshake: GetMessage failed, " +
			"dht: %s, inst: %s, dir: %d, local: %s, remote: %s, eno: %d",
			dht, conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String(),
			eno)
		return eno
	}

	if dhtMsg.Mid != MID_HANDSHAKE {
		ciLog.Debug("outboundHandshake: invalid MID, " +
			"dht: %s, inst: %s, dir: %d, local: %s, remote: %s, MID: %d",
			dht, conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String(),
			dhtMsg.Mid)
		return DhtEnoProtocol
	}

	hs := dhtMsg.Handshake
	if hs.Dir != ConInstDirInbound {
		ciLog.Debug("outboundHandshake: mismatched direction, " +
			"dht: %s, inst: %s, dir: %d, local: %s, remote: %s, hsdir: %d",
			dht, conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String(),
			hs.Dir)
		return DhtEnoProtocol
	}

	conInst.hsInfo.peer = config.Node{
		IP:		hs.IP,
		TCP:	uint16(hs.TCP & 0xffff),
		UDP:	uint16(hs.UDP & 0xffff),
		ID:		hs.NodeId,
	}

	ciLog.Debug("outboundHandshake: end ok, dht: %s, inst: %s, dir: %d, local: %s, remote: %s",
		dht, conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String())

	return DhtEnoNone
}

//
// Inbound handshake
//
func (conInst *ConInst)inboundHandshake() DhtErrno {

	dht := conInst.sdl.SchGetP2pCfgName()
	ciLog.Debug("inboundHandshake: begin, dht: %s, inst: %s, dir: %d, local: %s, remote: %s",
		dht, conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String())

	pkg := new(pb.DhtPackage)
	conInst.con.SetDeadline(time.Now().Add(conInst.hsTimeout))
	if err := conInst.ior.ReadMsg(pkg); err != nil {
		ciLog.Debug("inboundHandshake: ReadMsg failed, dht: %s, inst: %s, err: %s",
			dht, conInst.name, err.Error())
		return DhtEnoSerialization
	}

	if *pkg.Pid != PID_DHT {
		ciLog.Debug("inboundHandshake: invalid pid, dht: %s, inst: %s, pid: %d",
			dht, conInst.name, pkg.Pid)
		return DhtEnoProtocol
	}

	if *pkg.PayloadLength <= 0 {
		ciLog.Debug("inboundHandshake: invalid payload length: %d, dht: %s, inst: %s",
			*pkg.PayloadLength, dht, conInst.name)
		return DhtEnoProtocol
	}

	if len(pkg.Payload) != int(*pkg.PayloadLength) {
		ciLog.Debug("inboundHandshake: " +
			"payload length mismatched, PlLen: %d, real: %d, dht: %s, inst: %s",
			*pkg.PayloadLength, len(pkg.Payload), dht, conInst.name)
		return DhtEnoProtocol
	}

	dhtPkg := new(DhtPackage)
	dhtPkg.Pid = uint32(*pkg.Pid)
	dhtPkg.PayloadLength = *pkg.PayloadLength
	dhtPkg.Payload = pkg.Payload

	dhtMsg := new(DhtMessage)
	if eno := dhtPkg.GetMessage(dhtMsg); eno != DhtEnoNone {
		ciLog.Debug("inboundHandshake: GetMessage failed, eno: %d, dht: %s, inst: %s",
			eno, dht, conInst.name)
		return eno
	}

	if dhtMsg.Mid != MID_HANDSHAKE {
		ciLog.Debug("inboundHandshake: invalid MID: %d, dht: %s, inst: %s",
			dhtMsg.Mid, dht, conInst.name)
		return DhtEnoProtocol
	}

	hs := dhtMsg.Handshake
	if hs.Dir != ConInstDirOutbound {
		ciLog.Debug("inboundHandshake: mismatched direction: %d, dht: %s, inst: %s",
			hs.Dir, dht, conInst.name)
		return DhtEnoProtocol
	}

	conInst.hsInfo.peer = config.Node{
		IP:		hs.IP,
		TCP:	uint16(hs.TCP & 0xffff),
		UDP:	uint16(hs.UDP & 0xffff),
		ID:		hs.NodeId,
	}
	conInst.cid.nid = conInst.hsInfo.peer.ID

	*dhtMsg = DhtMessage{}
	dhtMsg.Mid = MID_HANDSHAKE
	dhtMsg.Handshake = &Handshake{
		Dir:		ConInstDirInbound,
		NodeId:		conInst.local.ID,
		IP:			conInst.local.IP,
		UDP:		uint32(conInst.local.UDP),
		TCP:		uint32(conInst.local.TCP),
		ProtoNum:	1,
		Protocols:	[]DhtProtocol {
			{
				Pid:	uint32(PID_DHT),
				Ver:	DhtVersion,
			},
		},
	}

	pbPkg := dhtMsg.GetPbPackage()
	if pbPkg == nil {
		ciLog.Debug("inboundHandshake: GetPbPackage failed, dht: %s, inst: %s",
			dht, conInst.name)
		return DhtEnoSerialization
	}

	conInst.con.SetDeadline(time.Now().Add(conInst.hsTimeout))
	if err := conInst.iow.WriteMsg(pbPkg); err != nil {
		ciLog.Debug("inboundHandshake: WriteMsg failed, err: %s, dht: %s, inst: %s",
			err.Error(), dht, conInst.name)
		return DhtEnoSerialization
	}

	ciLog.Debug("inboundHandshake: end ok, dht: %s, inst: %s, dir: %d, local: %s, remote: %s",
		dht, conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String())

	return DhtEnoNone
}

//
// Tx routine entry
//
func (conInst *ConInst)txProc() {

	//
	// longlong loop in a blocked mode
	//

	dht := conInst.sdl.SchGetP2pCfgName()
	conInst.con.SetDeadline(time.Time{})
	errUnderlying := false
	isDone := false

_txLoop:

	for {

		var txPkg *conInstTxPkg = nil
		var dhtPkg *DhtPackage = nil
		var pbPkg *pb.DhtPackage = nil
		var ok bool
		var el *list.Element

		//
		// fetch pending signal
		//

		_, ok = <-conInst.txChan
		if !ok {
			goto _checkDone
		}

		//
		// get pending and send it
		//

		conInst.txLock.Lock()
		el = conInst.txPending.Front()
		if el != nil {
			conInst.txPending.Remove(el)
		}
		conInst.txLock.Unlock()

		if el == nil {
			time.Sleep(time.Microsecond * 10)
			goto _checkDone
		}

		if txPkg, ok = el.Value.(*conInstTxPkg); !ok {
			ciLog.Debug("txProc: mismatched type, dht: %s, inst: %s", dht, conInst.name)
			goto _checkDone
		}

		if dhtPkg, ok = txPkg.payload.(*DhtPackage); !ok {
			ciLog.Debug("txProc: mismatched type, dht: %s, inst: %s", dht, conInst.name)
			goto _checkDone
		}

		pbPkg = new(pb.DhtPackage)
		dhtPkg.ToPbPackage(pbPkg)

		//
		// check if peer response needed, since we will block here until response from peer
		// is received if it's the case, we had to start a timer for the connection instance
		// task before we are blocked here.
		//

		if txPkg.responsed != nil {
			if eno, el := conInst.txSetPending(txPkg); eno == DhtEnoNone && el != nil {
				conInst.txSetTimer(el)
			}
		}

		if err := conInst.iow.WriteMsg(pbPkg); err != nil {
			ciLog.Debug("txProc: WriteMsg failed, dht: %s, inst: %s, err: %s",
				dht, conInst.name, err.Error())
			errUnderlying = true
			break _txLoop
		}

		if conInst.txPkgCnt++; conInst.txPkgCnt % 16 == 0 {
			ciLog.Debug("txProc: dht: %s, inst: %s, txPkgCnt: %d", dht, conInst.name, conInst.txPkgCnt)
		}

	_checkDone:

		select {
		case done := <-conInst.txDone:
			ciLog.Debug("txProc: dht: %s, inst: %s, done by: %d", dht, conInst.name, done)
			isDone = true
			break _txLoop
		default:
		}
	}

	//
	// here we get out, it might be:
	// 1) errors fired by underlying network;
	// 2) task done for some reasons;
	//

	if errUnderlying == true {

		//
		// the 1) case: report the status and then wait and hen singal done
		//

		conInst.status = CisOutOfService
		conInst.statusReport()

		<-conInst.txDone
		conInst.txDone<-DhtEnoNone.GetEno()

		return
	}

	if isDone == true {

		//
		// the 2) case: signal the done
		//

		conInst.txDone<-DhtEnoNone.GetEno()

		return
	}

	ciLog.Debug("txProc: wOw! impossible errors, dht: %s, inst: %s", dht, conInst.name)
}

//
// Rx routine entry
//
func (conInst *ConInst)rxProc() {

	//
	// longlong loop in a blocked mode
	//

	dht := conInst.sdl.SchGetP2pCfgName()
	conInst.con.SetDeadline(time.Time{})
	errUnderlying := false
	isDone := false

_rxLoop:

	for {

		var msg *DhtMessage = nil

		pbPkg := new(pb.DhtPackage)
		if err := conInst.ior.ReadMsg(pbPkg); err != nil {
			ciLog.Debug("rxProc: ReadMsg failed, dht: %s, inst: %s, err: %s, hsInfo: %+v, local: %+v",
				dht, conInst.name, err.Error(), conInst.hsInfo, *conInst.local)
			errUnderlying = true
			break _rxLoop
		}

		if conInst.rxPkgCnt++; conInst.rxPkgCnt % 16 == 0 {
			ciLog.Debug("rxProc: dht: %s, inst: %s, rxPkgCnt: %d",
				dht, conInst.name, conInst.rxPkgCnt)
		}

		pkg := new(DhtPackage)
		pkg.FromPbPackage(pbPkg)

		if pb.ProtocolId(pkg.Pid) == PID_EXT {

			conInst.cbRxLock.Lock()

			if conInst.cbfRxData != nil {
				conInst.cbfRxData(conInst, pkg.Pid, pkg.Payload)
			}

			conInst.cbRxLock.Unlock()

			goto _checkDone
		}

		msg = new(DhtMessage)
		if eno := pkg.GetMessage(msg); eno != DhtEnoNone {
			ciLog.Debug("rxProc:GetMessage failed, dht: %s, inst: %s, eno: %d",
				dht, conInst.name, eno)
			goto _checkDone
		}

		if eno := conInst.dispatch(msg); eno != DhtEnoNone {
			ciLog.Debug("rxProc: dispatch failed, dht: %s, inst: %s, eno: %d",
				dht, conInst.name, eno)
		}

_checkDone:

		select {
		case done := <-conInst.rxDone:
			isDone = true
			ciLog.Debug("rxProc: dht: %s, inst: %s, done by: %d", dht, conInst.name, done)
			break _rxLoop
		default:
		}
	}

	//
	// here we get out, it might be:
	// 1) errors fired by underlying network;
	// 2) task done for some reasons;
	//

	if errUnderlying == true {

		//
		// the 1) case: report the status and then wait and then signal done
		//

		conInst.status = CisOutOfService
		conInst.statusReport()

		<-conInst.rxDone
		conInst.rxDone <- DhtEnoNone.GetEno()

		return
	}

	if isDone == true {

		//
		// the 2) case: signal the done
		//

		conInst.rxDone <- DhtEnoNone.GetEno()

		return
	}

	ciLog.Debug("rxProc: wOw! impossible errors, dht: %s, inst: %s", dht, conInst.name)
}

//
// messages dispatching
//
func (conInst *ConInst)dispatch(msg *DhtMessage) DhtErrno {

	dht := conInst.sdl.SchGetP2pCfgName()

	if msg == nil {
		ciLog.Debug("dispatch: invalid parameter, " +
			"dht: %s, inst: %s, local: %+v", dht, conInst.name, *conInst.local)
		return DhtEnoParameter
	}

	ciLog.Debug("dispatch: try to dispatch message from peer, " +
		"dht: %s, inst: %s, local: %+v, msg: %+v", dht, conInst.name, *conInst.local, *msg)

	var eno = DhtEnoUnknown

	switch msg.Mid {

	case MID_HANDSHAKE:

		ciLog.Debug("dispatch: re-handshake is not supported now, " +
			"dht: %s, inst: %s, local: %+v", dht, conInst.name, *conInst.local)

		eno = DhtEnoProtocol

	case MID_FINDNODE:

		ciLog.Debug("dispatch: dht: %s, inst: %s, local: %+v, MID_FINDNODE from peer: %+v",
			dht, conInst.name, *conInst.local, *msg.FindNode)

		eno = conInst.findNode(msg.FindNode)

	case MID_NEIGHBORS:

		ciLog.Debug("dispatch: dht: %s, inst: %s, local: %+v, MID_NEIGHBORS from peer: %+v",
			dht, conInst.name, *conInst.local, *msg.Neighbors)

		eno = conInst.neighbors(msg.Neighbors)

	case MID_PUTVALUE:

		ciLog.Debug("dispatch: dht: %s, inst: %s, local: %+v, MID_PUTVALUE from peer: %+v",
			dht, conInst.name, *conInst.local, *msg.PutValue)

		eno = conInst.putValue(msg.PutValue)

	case MID_GETVALUE_REQ:

		ciLog.Debug("dispatch: dht: %s, inst: %s, local: %+v, MID_GETVALUE_REQ from peer: %+v",
			dht, conInst.name, *conInst.local, *msg.GetValueReq)

		eno = conInst.getValueReq(msg.GetValueReq)

	case MID_GETVALUE_RSP:

		ciLog.Debug("dispatch:  dht: %s, inst: local: %+v, %s, MID_GETVALUE_REQ from peer: %+v",
			dht, conInst.name, *conInst.local, *msg.GetValueRsp)

		eno = conInst.getValueRsp(msg.GetValueRsp)

	case MID_PUTPROVIDER:

		ciLog.Debug("dispatch: dht: %s, inst: %s, local: %+v, MID_PUTPROVIDER from peer: %+v",
			dht, conInst.name, *conInst.local, *msg.PutProvider)

		eno = conInst.putProvider(msg.PutProvider)

	case MID_GETPROVIDER_REQ:

		ciLog.Debug("dispatch: dht: %s, inst: %s, local: %+v, MID_GETPROVIDER_REQ from peer: %+v",
			dht, conInst.name, *conInst.local, *msg.GetProviderReq)

		eno = conInst.getProviderReq(msg.GetProviderReq)

	case MID_GETPROVIDER_RSP:

		ciLog.Debug("dispatch: dht: %s, inst: %s, local: %+v, MID_GETPROVIDER_RSP from peer: %+v",
			dht, conInst.name, *conInst.local, *msg.GetProviderRsp)

		eno = conInst.getProviderRsp(msg.GetProviderRsp)

	case MID_PING:

		ciLog.Debug("dispatch: dht: %s, inst: %s, local: %+v, MID_PING from peer: %+v",
			dht, conInst.name, *conInst.local, *msg.Ping)

		eno = conInst.getPing(msg.Ping)

	case MID_PONG:

		ciLog.Debug("dispatch: dht: %s, inst: %s, local: %+v, MID_PONG from peer: %+v",
			dht, conInst.name, *conInst.local, *msg.Pong)

		eno = conInst.getPong(msg.Pong)

	default:

		ciLog.Debug("dispatch: dht: %s, inst: %s, local: %+v, invalid message identity: %d",
			dht, conInst.name, *conInst.local, msg.Mid)

		eno = DhtEnoProtocol
	}

	return eno
}

//
// Handler for "MID_FINDNODE" from peer
//
func (conInst *ConInst)findNode(fn *FindNode) DhtErrno {
	msg := sch.SchMessage{}
	req := sch.MsgDhtRutMgrNearestReq {
		Target:		fn.Target,
		Max:		rutMgrMaxNearest,
		NtfReq:		false,
		Task:		conInst.ptnMe,
		ForWhat:	MID_FINDNODE,
		Msg:		fn,
	}
	conInst.sdl.SchMakeMessage(&msg, conInst.ptnMe, conInst.ptnRutMgr, sch.EvDhtRutMgrNearestReq, &req)
	conInst.sdl.SchSendMessage(&msg)
	return DhtEnoNone
}

//
// Handler for "MID_NEIGHBORS" from peer
//
func (conInst *ConInst)neighbors(nbs *Neighbors) DhtErrno {
	msg := sch.SchMessage{}
	ind := sch.MsgDhtQryInstProtoMsgInd{
		From:    &nbs.From,
		Msg:     nbs,
		ForWhat: sch.EvDhtConInstNeighbors,
	}
	conInst.sdl.SchMakeMessage(&msg, conInst.ptnMe, conInst.ptnMe, sch.EvDhtQryInstProtoMsgInd, &ind)
	conInst.sdl.SchSendMessage(&msg)
	return DhtEnoNone
}

//
// Handler for "MID_PUTVALUE" from peer
//
func (conInst *ConInst)putValue(pv *PutValue) DhtErrno {
	req := sch.MsgDhtDsMgrPutValReq {
		ConInst:	conInst,
		Msg:		pv,
	}
	msg := sch.SchMessage{}
	conInst.sdl.SchMakeMessage(&msg, conInst.ptnMe, conInst.ptnDsMgr, sch.EvDhtDsMgrPutValReq, &req)
	conInst.sdl.SchSendMessage(&msg)
	return DhtEnoNone
}

//
// Handler for "MID_GETVALUE_REQ" from peer
//
func (conInst *ConInst)getValueReq(gvr *GetValueReq) DhtErrno {
	req := sch.MsgDhtDsMgrGetValReq {
		ConInst:	conInst,
		Msg:		gvr,
	}
	msg := sch.SchMessage{}
	conInst.sdl.SchMakeMessage(&msg, conInst.ptnMe, conInst.ptnDsMgr, sch.EvDhtDsMgrGetValReq, &req)
	conInst.sdl.SchSendMessage(&msg)
	return DhtEnoNone
}

//
// Handler for "MID_GETVALUE_RSP" from peer
//
func (conInst *ConInst)getValueRsp(gvr *GetValueRsp) DhtErrno {
	msg := sch.SchMessage{}
	ind := sch.MsgDhtQryInstProtoMsgInd {
		From:		&gvr.From,
		Msg:		gvr,
		ForWhat:	sch.EvDhtConInstGetValRsp,
	}
	conInst.sdl.SchMakeMessage(&msg, conInst.ptnMe, conInst.ptnMe, sch.EvDhtQryInstProtoMsgInd, &ind)
	conInst.sdl.SchSendMessage(&msg)
	return DhtEnoNone
}

//
// Handler for "MID_PUTPROVIDER" from peer
//
func (conInst *ConInst)putProvider(pp *PutProvider) DhtErrno {
	req := sch.MsgDhtPrdMgrPutProviderReq {
		ConInst:	conInst,
		Msg:		pp,
	}
	msg := sch.SchMessage{}
	conInst.sdl.SchMakeMessage(&msg, conInst.ptnMe, conInst.ptnPrdMgr, sch.EvDhtPrdMgrPutProviderReq, &req)
	conInst.sdl.SchSendMessage(&msg)
	return DhtEnoNone
}

//
// Handler for "MID_GETPROVIDER_REQ" from peer
//
func (conInst *ConInst)getProviderReq(gpr *GetProviderReq) DhtErrno {
	req := sch.MsgDhtPrdMgrGetProviderReq {
		ConInst:	conInst,
		Msg:		gpr,
	}
	msg := sch.SchMessage{}
	conInst.sdl.SchMakeMessage(&msg, conInst.ptnMe, conInst.ptnPrdMgr, sch.EvDhtPrdMgrGetProviderReq, &req)
	conInst.sdl.SchSendMessage(&msg)
	return DhtEnoNone
}

//
// Handler for "MID_GETPROVIDER_RSP" from peer
//
func (conInst *ConInst)getProviderRsp(gpr *GetProviderRsp) DhtErrno {
	msg := sch.SchMessage{}
	ind := sch.MsgDhtQryInstProtoMsgInd {
		From:		&gpr.From,
		Msg:		gpr,
		ForWhat:	sch.EvDhtConInstGetProviderRsp,
	}
	conInst.sdl.SchMakeMessage(&msg, conInst.ptnMe, conInst.ptnMe, sch.EvDhtQryInstProtoMsgInd, &ind)
	conInst.sdl.SchSendMessage(&msg)
	return DhtEnoNone
}

//
// Handler for "MID_PING" from peer
//
func (conInst *ConInst)getPing(ping *Ping) DhtErrno {
	pingInd := sch.MsgDhtRutPingInd {
		ConInst:	conInst,
		Msg:		ping,
	}
	msg := sch.SchMessage{}
	conInst.sdl.SchMakeMessage(&msg, conInst.ptnMe, conInst.ptnRutMgr, sch.EvDhtRutPingInd, &pingInd)
	conInst.sdl.SchSendMessage(&msg)
	return DhtEnoNone
}

//
// Handler for "MID_PONG" from peer
//
func (conInst *ConInst)getPong(pong *Pong) DhtErrno {
	pongInd := sch.MsgDhtRutPingInd {
		ConInst:	conInst,
		Msg:		pong,
	}
	msg := sch.SchMessage{}
	conInst.sdl.SchMakeMessage(&msg, conInst.ptnMe, conInst.ptnRutMgr, sch.EvDhtRutPongInd, &pongInd)
	conInst.sdl.SchSendMessage(&msg)
	return DhtEnoNone
}

//
// Check if pending packages sent is responsed by peeer
//
func (conInst *ConInst)checkTxWaitResponse(mid int, seq int64) (DhtErrno, *conInstTxPkg) {

	conInst.txLock.Lock()
	defer conInst.txLock.Unlock()

	que := conInst.txWaitRsp

	for el := que.Front(); el != nil; el = el.Next() {

		if txPkg, ok := el.Value.(*conInstTxPkg); ok {

			if txPkg.waitMid == mid && txPkg.waitSeq == seq {

				ciLog.Debug("checkTxWaitResponse: it's found, mid: %d, seq: %d", mid, seq)

				if txPkg.responsed != nil {

					//
					// see comments about field "responsed" please
					//

					txPkg.responsed<-true
					close(txPkg.responsed)
				}

				if txPkg.txTid != sch.SchInvalidTid {
					conInst.sdl.SchKillTimer(conInst.ptnMe, txPkg.txTid)
					txPkg.txTid = sch.SchInvalidTid
				}

				que.Remove(el)

				return DhtEnoNone, txPkg
			}
		}
	}

	ciLog.Debug("checkTxWaitResponse: not found, mid: %d, seq: %d", mid, seq)

	return DhtEnoNotFound, nil
}

//
// Install callback for rx data with protocol identity PID_EXT
//
func (conInst *ConInst)InstallRxDataCallback(cbf ConInstRxDataCallback) DhtErrno {

	conInst.cbRxLock.Lock()
	defer conInst.cbRxLock.Unlock()

	if conInst.cbfRxData != nil {
		ciLog.Debug("InstallRxDataCallback: old callback will be overlapped")
	}

	if cbf == nil {
		ciLog.Debug("InstallRxDataCallback: nil callback will be set")
	}

	conInst.cbfRxData = cbf

	return DhtEnoNone
}

//
// Get scheduler
//
func (conInst *ConInst)GetScheduler() *sch.Scheduler {
	return conInst.sdl
}