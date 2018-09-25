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
	ggio "github.com/gogo/protobuf/io"
	log "github.com/yeeco/gyee/p2p/logger"
	sch "github.com/yeeco/gyee/p2p/scheduler"
	config "github.com/yeeco/gyee/p2p/config"
	pb "github.com/yeeco/gyee/p2p/dht/pb"
)

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
	ptnSrcTsk		interface{}				// for outbound, the source task requests the connection
	ptnSrcTskBackup	interface{}				// backup for ptnSrcTsk
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
	txPendSig		chan interface{}		// tx pendings signal
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
	ciTxMaxWaitResponseSize = 8				// tx max wait peer response queue size
)

//
// Create connection instance
//
func newConInst(postFixed string, isBlind bool) *ConInst {

	conInst := ConInst {
		name:				"conInst" + postFixed,
		tep:				nil,
		ptnMe:				nil,
		ptnConMgr:			nil,
		ptnSrcTsk:			nil,
		ptnSrcTskBackup:	nil,
		con:				nil,
		ior:				nil,
		iow:				nil,
		status:				CisNull,
		dir:				ConInstDirUnknown,
		txPending:			list.New(),
		txWaitRsp:			list.New(),
		txDone:				nil,
		rxDone:				nil,
		cbfRxData:			nil,
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
		log.LogCallerFileLine("conInstProc: " +
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

	default:
		log.LogCallerFileLine("conInstProc: unknown event: %d", msg.Id)
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
		log.LogCallerFileLine("poweron: task mismatched")
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
		log.LogCallerFileLine("poweroff: task mismatched")
		return sch.SchEnoMismatched
	}

	log.LogCallerFileLine("poweroff: task will be done ...")

	conInst.cleanUp(DhtEnoScheduler)
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

	rsp := sch.MsgDhtConInstHandshakeRsp {
		Eno:	DhtEnoUnknown,
		Inst:	conInst,
		Peer:	nil,
		Dir:	int(conInst.dir),
		HsInfo:	nil,
		Dur:	time.Duration(-1),
	}

	rsp2ConMgr := func() sch.SchErrno {
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
			rsp.Inst = nil
			rsp.HsInfo = &hsInfo
			rsp2ConMgr()

			conInst.cleanUp(int(eno))
			return conInst.sdl.SchTaskDone(conInst.ptnMe, sch.SchEnoUserTask)
		}

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
			rsp.Inst = nil
			rsp.HsInfo = &hsInfo

			return rsp2ConMgr()

			/*conInst.cleanUp(int(eno))
			return conInst.sdl.SchTaskDone(conInst.ptnMe, sch.SchEnoUserTask)*/
		}

	} else {

		if eno := conInst.inboundHandshake(); eno != DhtEnoNone {

			rsp.Eno = int(eno)
			rsp.Peer = nil
			rsp.HsInfo = nil
			rsp.Inst = nil

			return rsp2ConMgr()

			/*conInst.cleanUp(int(eno))
			return conInst.sdl.SchTaskDone(conInst.ptnMe, sch.SchEnoUserTask)*/
		}
	}

	conInst.status = CisHandshaked
	conInst.statusReport()

	rsp.Eno = DhtEnoNone
	rsp.Peer = &conInst.hsInfo.peer
	rsp.HsInfo = &conInst.hsInfo
	rsp2ConMgr()


	//
	// service startup
	//

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

		log.LogCallerFileLine("closeReq: " +
			"status mismatched, dir: %d, status: %d",
			conInst.dir, conInst.status)
		return sch.SchEnoMismatched
	}

	if *msg.Peer != conInst.hsInfo.peer.ID {
		log.LogCallerFileLine("closeReq: peer node identity mismatched")
		return sch.SchEnoMismatched
	}

	log.LogCallerFileLine("closeReq: " +
		"connection will be closed, why: %d, peer: %x",
		msg.Why, *msg.Peer)

	conInst.cleanUp(DhtEnoNone)
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
		log.LogCallerFileLine("txDataReq: txPutPending failed, eno: %d", eno)
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
		log.LogCallerFileLine("rutMgrNearestRsp: invalid parameters")
		return sch.SchEnoParameter
	}

	log.LogCallerFileLine("rutMgrNearestRsp: msg: %+v", msg)

	var dhtMsg = DhtMessage {
		Mid: MID_UNKNOWN,
	}

	var nodes []*config.Node
	bns := msg.Peers.([]*rutMgrBucketNode)
	for idx := 0; idx < len(bns); idx++ {
		nodes = append(nodes, &bns[idx].node)
	}

	if msg.ForWhat == MID_FINDNODE {

		if msg.Msg == nil {
			log.LogCallerFileLine("rutMgrNearestRsp: original message is nil")
			return sch.SchEnoMismatched
		}

		findNode, ok := msg.Msg.(*FindNode)
		if !ok {
			log.LogCallerFileLine("rutMgrNearestRsp: invalid original message type")
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

		log.LogCallerFileLine("rutMgrNearestRsp: for MID_GETPROVIDER_REQ should not come here")
		return sch.SchEnoMismatched

	} else if msg.ForWhat == MID_GETVALUE_REQ {

		log.LogCallerFileLine("rutMgrNearestRsp: for MID_GETVALUE_REQ should not come here")
		return sch.SchEnoMismatched

	} else {

		log.LogCallerFileLine("rutMgrNearestRsp: unknown what's for")
		return sch.SchEnoMismatched
	}

	dhtPkg := DhtPackage{}
	if eno := dhtMsg.GetPackage(&dhtPkg); eno != DhtEnoNone {
		log.LogCallerFileLine("rutMgrNearestRsp: GetPackage failed, eno: %d", eno)
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
		log.LogCallerFileLine("rutMgrNearestRsp: txPutPending failed, eno: %d", eno)
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

	if pkg == nil {
		log.LogCallerFileLine("txPutPending: invalid parameter")
		return DhtEnoParameter
	}

	conInst.txLock.Lock()
	defer conInst.txLock.Unlock()

	if conInst.txPending.Len() >= ciTxPendingQueueSize {
		log.LogCallerFileLine("txPutPending: pending queue full")
		return DhtEnoResource
	}

	if conInst.txWaitRsp.Len() >= ciTxMaxWaitResponseSize {
		log.LogCallerFileLine("txPutPending: waiting response queue full")
		return DhtEnoResource
	}

	log.LogCallerFileLine("txPutPending: put, waitMid: %d, waitSeq: %d", pkg.waitMid, pkg.waitSeq)

	conInst.txPending.PushBack(pkg)
	conInst.txPendSig<-pkg

	return DhtEnoNone
}

//
// Set timer for tx-package which would wait response from peer
//
func (conInst *ConInst)txSetTimer(el *list.Element) DhtErrno {

	if el == nil {
		log.LogCallerFileLine("txSetTimer: invalid parameter")
		return DhtEnoParameter
	}

	txPkg, ok := el.Value.(*conInstTxPkg)
	if !ok {
		log.LogCallerFileLine("txSetTimer: type mismatched")
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
		log.LogCallerFileLine("txSetTimer: SchSetTimer failed, eno: %d", eno)
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
		log.LogCallerFileLine("txTimerHandler: invalid parameter, dht: %s, inst: %s", dht, conInst.name)
		return sch.SchEnoParameter
	}

	txPkg, ok := el.Value.(*conInstTxPkg)
	if !ok {
		log.LogCallerFileLine("txTimerHandler: type mismatched, dht: %s, inst: %s", dht, conInst.name)
		return sch.SchEnoMismatched
	}

	log.LogCallerFileLine("txTimerHandler: expired, " +
		"dht: %s, inst: %s, el: %+v, txPkg: %+v", dht, conInst.name, *el, *txPkg)

	conInst.status = CisClosed
	conInst.statusReport()

	conInst.cleanUp(int(DhtEnoTimeout))
	if eno := conInst.sdl.SchTaskDone(conInst.ptnMe, sch.SchEnoUserTask); eno != sch.SchEnoNone {
		log.LogCallerFileLine("txTimerHandler: SchTaskDone failed, dht: %s, inst: %s, eno: %d", dht, conInst.name, eno)
		return eno
	}

	return sch.SchEnoNone
}

//
// Set current Tx pending
//
func (conInst *ConInst)txSetPending(txPkg *conInstTxPkg) (DhtErrno, *list.Element){

	conInst.txLock.Lock()
	defer conInst.txLock.Unlock()

	//
	// notice: the conInst.ptnSrcTsk will be backuped and then modified to
	// the pending package's owner task node pointer.
	//

	var el *list.Element = nil

	if txPkg != nil {

		conInst.ptnSrcTskBackup = conInst.ptnSrcTsk
		conInst.ptnSrcTsk = txPkg.task
		el = conInst.txWaitRsp.PushBack(txPkg)

	} else {

		//
		// seems no switching is needed: a connection instance might live for a long time,
		// while the original source task (the original owner of this connection) might havd
		// been done for some reasons. so this field "conInst.ptnSrcTsk" is used as the current
		// owner task of the tx-package wait need response from peer.
		//
		// when "txPkg" passed in is nil, means the current tx-packet does not want to wait
		// response from peeer, and the tx routine would continue, we just set the owner nil.
		//

		//conInst.ptnSrcTsk = conInst.ptnSrcTskBackup
		conInst.ptnSrcTsk = nil
	}

	return DhtEnoNone, el
}

//
// Start tx-task
//
func (conInst *ConInst)txTaskStart() DhtErrno {

	if conInst.txDone != nil {
		log.LogCallerFileLine("txTaskStart: non-nil chan for done")
		return DhtEnoMismatched
	}
	conInst.txDone = make(chan int, 1)

	if conInst.txPendSig != nil {
		log.LogCallerFileLine("txTaskStart: non-nil chan for txPendSig")
		return DhtEnoMismatched
	}
	conInst.txPendSig = make(chan interface{}, ciTxPendingQueueSize)

	go conInst.txProc()

	return DhtEnoNone
}

//
// Start rx-task
//
func (conInst *ConInst)rxTaskStart() DhtErrno {
	if conInst.rxDone != nil {
		log.LogCallerFileLine("rxTaskStart: non-nil chan for done")
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
		close(conInst.txPendSig)
		conInst.txPendSig = nil

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

	conInst.txTaskStop(why)
	conInst.rxTaskStop(why)

	que := conInst.txWaitRsp

	for que.Len() != 0 {

		el := que.Front()

		if txPkg, ok := el.Value.(*conInstTxPkg); ok {

			if txPkg.txTid != sch.SchInvalidTid {
				conInst.sdl.SchKillTimer(conInst.ptnMe, txPkg.txTid)
			}

			if txPkg.task != nil {

				//
				// tell the package owner task about timeout
				//

				schMsg := sch.SchMessage{}
				ind := sch.MsgDhtConInstTxInd {
					Eno:		DhtEnoTimeout,
					WaitMid:	txPkg.waitMid,
					WaitSeq:	txPkg.waitSeq,
				}

				conInst.sdl.SchMakeMessage(&schMsg, conInst.ptnMe, txPkg.task, sch.EvDhtConInstTxInd, &ind)
				conInst.sdl.SchSendMessage(&schMsg)
			}

			if txPkg.responsed != nil {

				//
				// see comments about field "responsed" pls
				//

				close(txPkg.responsed)
				txPkg.responsed = nil
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

	if conInst.dir != ConInstDirOutbound {
		log.LogCallerFileLine("connect2Peer: mismatched direction: %d", conInst.dir)
		return DhtEnoInternal
	}

	peer := conInst.hsInfo.peer
	dialer := &net.Dialer{Timeout: ciConn2PeerTimeout}
	addr := &net.TCPAddr{IP: peer.IP, Port: int(peer.TCP)}

	log.LogCallerFileLine("connect2Peer: try to connect, " +
		"local: %s, remote: %s",
		conInst.local.IP.String(),
		addr.String())

	var conn net.Conn
	var err error

	if conn, err = dialer.Dial("tcp", addr.String()); err != nil {
		log.LogCallerFileLine("connect2Peer: " +
			"dial failed, to: %s, err: %s",
			addr.String(), err.Error())
		return DhtEnoOs
	}

	conInst.con = conn
	r := conInst.con.(io.Reader)
	conInst.ior = ggio.NewDelimitedReader(r, ciMaxPackageSize)
	w := conInst.con.(io.Writer)
	conInst.iow = ggio.NewDelimitedWriter(w)

	log.LogCallerFileLine("connect2Peer: connect ok, " +
		"local: %s, remote: %s",
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
		log.LogCallerFileLine("outboundHandshake: GetPbPackage failed")
		return DhtEnoSerialization
	}

	conInst.con.SetDeadline(time.Now().Add(conInst.hsTimeout))
	if err := conInst.iow.WriteMsg(pbPkg); err != nil {
		log.LogCallerFileLine("outboundHandshake: WriteMsg failed, err: %s", err.Error())
		return DhtEnoSerialization
	}

	*pbPkg = pb.DhtPackage{}
	conInst.con.SetDeadline(time.Now().Add(conInst.hsTimeout))
	if err := conInst.ior.ReadMsg(pbPkg); err != nil {
		log.LogCallerFileLine("outboundHandshake: ReadMsg failed, err: %s", err.Error())
		return DhtEnoSerialization
	}

	if *pbPkg.Pid != PID_DHT {
		log.LogCallerFileLine("outboundHandshake: invalid pid: %d", pbPkg.Pid)
		return DhtEnoProtocol
	}

	if *pbPkg.PayloadLength <= 0 {
		log.LogCallerFileLine("outboundHandshake: " +
			"invalid payload length: %d",
			*pbPkg.PayloadLength)
		return DhtEnoProtocol
	}

	if len(pbPkg.Payload) != int(*pbPkg.PayloadLength) {
		log.LogCallerFileLine("outboundHandshake: " +
			"payload length mismatched, PlLen: %d, real: %d",
			*pbPkg.PayloadLength, len(pbPkg.Payload))
		return DhtEnoProtocol
	}

	dhtPkg := new(DhtPackage)
	dhtPkg.Pid = uint32(*pbPkg.Pid)
	dhtPkg.PayloadLength = *pbPkg.PayloadLength
	dhtPkg.Payload = pbPkg.Payload

	*dhtMsg = DhtMessage{}
	if eno := dhtPkg.GetMessage(dhtMsg); eno != DhtEnoNone {
		log.LogCallerFileLine("outboundHandshake: GetMessage failed, eno: %d", eno)
		return eno
	}

	if dhtMsg.Mid != MID_HANDSHAKE {
		log.LogCallerFileLine("outboundHandshake: " +
			"invalid MID: %d", dhtMsg.Mid)
		return DhtEnoProtocol
	}

	hs := dhtMsg.Handshake
	if hs.Dir != ConInstDirInbound {
		log.LogCallerFileLine("outboundHandshake: " +
			"mismatched direction: %d", hs.Dir)
		return DhtEnoProtocol
	}

	conInst.hsInfo.peer = config.Node{
		IP:		hs.IP,
		TCP:	uint16(hs.TCP & 0xffff),
		UDP:	uint16(hs.UDP & 0xffff),
		ID:		hs.NodeId,
	}

	return DhtEnoNone
}

//
// Inbound handshake
//
func (conInst *ConInst)inboundHandshake() DhtErrno {

	pkg := new(pb.DhtPackage)
	conInst.con.SetDeadline(time.Now().Add(conInst.hsTimeout))
	if err := conInst.ior.ReadMsg(pkg); err != nil {
		log.LogCallerFileLine("inboundHandshake: ReadMsg failed, err: %s", err.Error())
		return DhtEnoSerialization
	}

	if *pkg.Pid != PID_DHT {
		log.LogCallerFileLine("inboundHandshake: invalid pid: %d", pkg.Pid)
		return DhtEnoProtocol
	}

	if *pkg.PayloadLength <= 0 {
		log.LogCallerFileLine("inboundHandshake: " +
			"invalid payload length: %d",
			*pkg.PayloadLength)
		return DhtEnoProtocol
	}

	if len(pkg.Payload) != int(*pkg.PayloadLength) {
		log.LogCallerFileLine("inboundHandshake: " +
			"payload length mismatched, PlLen: %d, real: %d",
			*pkg.PayloadLength, len(pkg.Payload))
		return DhtEnoProtocol
	}

	dhtPkg := new(DhtPackage)
	dhtPkg.Pid = uint32(*pkg.Pid)
	dhtPkg.PayloadLength = *pkg.PayloadLength
	dhtPkg.Payload = pkg.Payload

	dhtMsg := new(DhtMessage)
	if eno := dhtPkg.GetMessage(dhtMsg); eno != DhtEnoNone {
		log.LogCallerFileLine("inboundHandshake: GetMessage failed, eno: %d", eno)
		return eno
	}

	if dhtMsg.Mid != MID_HANDSHAKE {
		log.LogCallerFileLine("inboundHandshake: " +
			"invalid MID: %d", dhtMsg.Mid)
		return DhtEnoProtocol
	}

	hs := dhtMsg.Handshake
	if hs.Dir != ConInstDirOutbound {
		log.LogCallerFileLine("inboundHandshake: " +
			"mismatched direction: %d", hs.Dir)
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
		log.LogCallerFileLine("inboundHandshake: GetPbPackage failed")
		return DhtEnoSerialization
	}

	conInst.con.SetDeadline(time.Now().Add(conInst.hsTimeout))
	if err := conInst.iow.WriteMsg(pbPkg); err != nil {
		log.LogCallerFileLine("inboundHandshake: WriteMsg failed, err: %s", err.Error())
		return DhtEnoSerialization
	}

	return DhtEnoNone
}

//
// Tx routine entry
//
func (conInst *ConInst)txProc() {

	//
	// longlong loop in a blocked mode
	//

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

		_, ok = <-conInst.txPendSig
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
			log.LogCallerFileLine("txProc: mismatched type, inst: %s", conInst.name)
			goto _checkDone
		}

		if dhtPkg, ok = txPkg.payload.(*DhtPackage); !ok {
			log.LogCallerFileLine("txProc: mismatched type, inst: %s", conInst.name)
			goto _checkDone
		}

		pbPkg = new(pb.DhtPackage)
		dhtPkg.ToPbPackage(pbPkg)

		if err := conInst.iow.WriteMsg(pbPkg); err != nil {
			log.LogCallerFileLine("txProc: WriteMsg failed, inst: %s, err: %s", conInst.name, err.Error())
			errUnderlying = true
			break _txLoop
		}

		if conInst.txPkgCnt++; conInst.txPkgCnt % 16 == 0 {
			log.LogCallerFileLine("txProc: inst: %s, txPkgCnt: %d", conInst.name, conInst.txPkgCnt)
		}

		//
		// check if peer response needed, since we will block here until response from peer
		// is received if it's the case, we had to start a timer for the connection instance
		// task before we are blocked here.
		//

		if txPkg.responsed != nil {

			if eno, el := conInst.txSetPending(txPkg); eno == DhtEnoNone && el != nil {
				conInst.txSetTimer(el)
			}

		} else {

			conInst.txSetPending(nil)
		}

_checkDone:

		select {
		case done := <-conInst.txDone:
			log.LogCallerFileLine("txProc: inst: %s, done by: %d", conInst.name, done)
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
		// the 1) case
		//

		conInst.status = CisClosed
		conInst.statusReport()
		conInst.cleanUp(DhtEnoOs)
		return
	}

	if isDone == true {

		//
		// the 2) case
		//

		conInst.txDone <- DhtEnoNone
		return
	}

	log.LogCallerFileLine("txProc: wOw! impossible errors, inst: %s", conInst.name)
}

//
// Rx routine entry
//
func (conInst *ConInst)rxProc() {

	//
	// longlong loop in a blocked mode
	//

	conInst.con.SetDeadline(time.Time{})
	errUnderlying := false
	isDone := false

_rxLoop:

	for {

		var msg *DhtMessage = nil

		pbPkg := new(pb.DhtPackage)
		if err := conInst.ior.ReadMsg(pbPkg); err != nil {
			log.LogCallerFileLine("rxProc: ReadMsg failed, inst: %s, err: %s", conInst.name, err.Error())
			errUnderlying = true
			break _rxLoop
		}

		if conInst.rxPkgCnt++; conInst.rxPkgCnt % 16 == 0 {
			log.LogCallerFileLine("rxProc: inst: %s, rxPkgCnt: %d", conInst.name, conInst.rxPkgCnt)
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
			log.LogCallerFileLine("rxProc:GetMessage failed, inst: %s, eno: %d", conInst.name, eno)
			goto _checkDone
		}

		if eno := conInst.dispatch(msg); eno != DhtEnoNone {
			log.LogCallerFileLine("rxProc: dispatch failed, inst: %s, eno: %d", conInst.name, eno)
		}

_checkDone:

		select {
		case done := <-conInst.rxDone:
			isDone = true
			log.LogCallerFileLine("rxProc: inst: %s, done by: %d", conInst.name, done)
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
		// the 1) case
		//

		conInst.status = CisClosed
		conInst.statusReport()
		conInst.cleanUp(DhtEnoOs)
		return
	}

	if isDone == true {

		//
		// the 2) case
		//

		conInst.txDone <- DhtEnoNone
		return
	}

	log.LogCallerFileLine("rxProc: wOw! impossible errors, inst: %s", conInst.name)
}

//
// messages dispatching
//
func (conInst *ConInst)dispatch(msg *DhtMessage) DhtErrno {

	if msg == nil {
		log.LogCallerFileLine("dispatch: invalid parameter")
		return DhtEnoParameter
	}

	log.LogCallerFileLine("dispatch: try to dispatch message from peer, " +
		"inst: %s, msg: %+v", conInst.name, *msg)

	var eno DhtErrno = DhtEnoUnknown

	switch msg.Mid {

	case MID_HANDSHAKE:
		log.LogCallerFileLine("dispatch: re-handshake is not supported now")
		eno = DhtEnoProtocol

	case MID_FINDNODE:
		log.LogCallerFileLine("dispatch: MID_FINDNODE from peer: %+v", *msg.FindNode)
		eno = conInst.findNode(msg.FindNode)

	case MID_NEIGHBORS:
		log.LogCallerFileLine("dispatch: MID_NEIGHBORS from peer: %+v", *msg.Neighbors)
		eno = conInst.neighbors(msg.Neighbors)

	case MID_PUTVALUE:
		log.LogCallerFileLine("dispatch: MID_PUTVALUE from peer: %+v", *msg.PutValue)
		eno = conInst.putValue(msg.PutValue)

	case MID_GETVALUE_REQ:
		log.LogCallerFileLine("dispatch: MID_GETVALUE_REQ from peer: %+v", *msg.GetValueReq)
		eno = conInst.getValueReq(msg.GetValueReq)

	case MID_GETVALUE_RSP:
		log.LogCallerFileLine("dispatch: MID_GETVALUE_REQ from peer: %+v", *msg.GetValueRsp)
		eno = conInst.getValueRsp(msg.GetValueRsp)

	case MID_PUTPROVIDER:
		log.LogCallerFileLine("dispatch: MID_PUTPROVIDER from peer: %+v", *msg.PutProvider)
		eno = conInst.putProvider(msg.PutProvider)

	case MID_GETPROVIDER_REQ:
		log.LogCallerFileLine("dispatch: MID_GETPROVIDER_REQ from peer: %+v", *msg.GetProviderReq)
		eno = conInst.getProviderReq(msg.GetProviderReq)

	case MID_GETPROVIDER_RSP:
		log.LogCallerFileLine("dispatch: MID_GETPROVIDER_RSP from peer: %+v", *msg.GetProviderRsp)
		eno = conInst.getProviderRsp(msg.GetProviderRsp)

	case MID_PING:
		log.LogCallerFileLine("dispatch: MID_PING from peer: %+v", *msg.Ping)
		eno = conInst.getPing(msg.Ping)

	case MID_PONG:
		log.LogCallerFileLine("dispatch: MID_PONG from peer: %+v", *msg.Pong)
		eno = conInst.getPong(msg.Pong)

	default:
		log.LogCallerFileLine("dispatch: invalid message identity: %d", msg.Mid)
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

	eno, txPkg := conInst.checkTxCurPending(MID_NEIGHBORS, int64(nbs.Id))

	if eno != DhtEnoNone || txPkg == nil {
		log.LogCallerFileLine("neighbors: checkTxCurPending failed, eno: %d, txPkg: %p", eno, txPkg)
		return eno
	}

	msg := sch.SchMessage{}
	ind := sch.MsgDhtQryInstProtoMsgInd {
		From:		&nbs.From,
		Msg:		nbs,
		ForWhat:	sch.EvDhtConInstNeighbors,
	}

	conInst.sdl.SchMakeMessage(&msg, conInst.ptnMe, txPkg.task, sch.EvDhtQryInstProtoMsgInd, &ind)
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

	eno, txPkg := conInst.checkTxCurPending(MID_GETVALUE_RSP, int64(gvr.Id))

	if eno != DhtEnoNone || txPkg == nil {
		log.LogCallerFileLine("getValueRsp: checkTxCurPending failed, eno: %d, txPkg: %p", eno, txPkg)
		return eno
	}

	msg := sch.SchMessage{}
	ind := sch.MsgDhtQryInstProtoMsgInd {
		From:		&gvr.From,
		Msg:		gvr,
		ForWhat:	sch.EvDhtConInstGetValRsp,
	}

	conInst.sdl.SchMakeMessage(&msg, conInst.ptnMe, txPkg.task, sch.EvDhtQryInstProtoMsgInd, &ind)
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

	eno, txPkg := conInst.checkTxCurPending(MID_GETPROVIDER_RSP, int64(gpr.Id))

	if eno != DhtEnoNone || txPkg == nil {
		log.LogCallerFileLine("getProviderRsp: checkTxCurPending failed, eno: %d", eno)
		return eno
	}

	msg := sch.SchMessage{}
	ind := sch.MsgDhtQryInstProtoMsgInd {
		From:		&gpr.From,
		Msg:		gpr,
		ForWhat:	sch.EvDhtConInstGetProviderRsp,
	}

	conInst.sdl.SchMakeMessage(&msg, conInst.ptnMe, txPkg.task, sch.EvDhtQryInstProtoMsgInd, &ind)
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
// Check if current Tx pending package is responsed by peeer
//
func (conInst *ConInst)checkTxCurPending(mid int, seq int64) (DhtErrno, *conInstTxPkg) {

	que := conInst.txWaitRsp

	for el := que.Front(); el != nil; el = el.Next() {

		if txPkg, ok := el.Value.(*conInstTxPkg); ok {

			if txPkg.waitMid == mid && txPkg.waitSeq == seq {

				log.LogCallerFileLine("checkTxCurPending: it's found, mid: %d, seq: %d", mid, seq)

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

	log.LogCallerFileLine("checkTxCurPending: not found, mid: %d, seq: %d", mid, seq)

	return DhtEnoNotFound, nil
}

//
// Install callback for rx data with protocol identity PID_EXT
//
func (conInst *ConInst)InstallRxDataCallback(cbf ConInstRxDataCallback) DhtErrno {

	conInst.cbRxLock.Lock()
	defer conInst.cbRxLock.Unlock()

	if conInst.cbfRxData != nil {
		log.LogCallerFileLine("InstallRxDataCallback: old callback will be overlapped")
	}

	if cbf == nil {
		log.LogCallerFileLine("InstallRxDataCallback: nil callback will be set")
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