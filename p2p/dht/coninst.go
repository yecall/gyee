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
	"container/list"
	log "github.com/yeeco/gyee/p2p/logger"
	sch "github.com/yeeco/gyee/p2p/scheduler"
	config "github.com/yeeco/gyee/p2p/config"
)

//
// Dht connection instance
//
type ConInst struct {
	sdl			*sch.Scheduler			// pointer to scheduler
	name		string					// task name
	tep			sch.SchUserTaskEp		// task entry
	ptnMe		interface{}				// pointer to myself task node
	ptnConMgr	interface{}				// pointer to connection manager task node
	ptnSrcTsk	interface{}				// for outbound, the source task requests the connection
	status		conInstStatus			// instance status
	cid			conInstIdentity			// connection instance identity
	con			net.Conn				// connection
	dir			conInstDir				// connection instance directory
	hsInfo		conInstHandshakeInfo	// handshake information
	txPending	*list.List				// pending package to be sent
	txLock		sync.Mutex				// tx lock
	txDone		chan int				// tx-task done signal
	rxDone		chan int				// rx-task done signal
}

//
// Connection instance identity
//
type conInstIdentity struct {
	nid			config.NodeID		// node identity
	dir			conInstDir			// connection direction
}

//
// Connection instance status
//
const (
	cisNull			= iota			// null, not inited
	cisConnecting					// connecting
	cisConnected					// connected
	cisInHandshaking				// handshaking
	cisHandshaked					// handshaked
	cisInService					// in service
	cisClosed						// closed
)

type conInstStatus int

//
// Connection instance direction
//
const (
	conInstDirInbound	= 0			// out from local
	conInstDirOutbound	= 1			// in from peer
	conInstDirAllbound	= 2			// in & out
	conInstDirUnknown	= -1		// not be initialized
)

type conInstDir int

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
	submitTime	time.Time			// time the payload submitted
	payload		[]byte				// payload buffer
}

//
// Max tx-pending queue size
//
const txPendingQueueSize = 64

//
// Connect to peer timeout vale
//
const ciConn2PeerTimeout = time.Second * 16


//
// Create connection instance
//
func newConInst(postFixed string) *ConInst {

	conInst := ConInst {
		name:		"conInst" + postFixed,
		tep:		nil,
		ptnMe:		nil,
		ptnConMgr:	nil,
		ptnSrcTsk:	nil,
		status:		cisNull,
		dir:		conInstDirUnknown,
		txPending:	list.New(),
		txDone:		nil,
		rxDone:		nil,
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

	if conInst.dir == conInstDirInbound {
		conInst.status = cisConnected
		return sch.SchEnoNone
	}

	if conInst.dir == conInstDirOutbound {
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

	if conInst.con == nil && conInst.dir == conInstDirOutbound {

		conInst.status = cisConnecting
		conInst.statusReport()

		if eno := conInst.connect2Peer(); eno != DhtEnoNone {

			rsp.Eno = int(eno)
			rsp.Peer = &conInst.hsInfo.peer
			rsp2ConMgr()

			conInst.cleanUp(int(eno))
			return conInst.sdl.SchTaskDone(conInst.ptnMe, sch.SchEnoUserTask)
		}

		conInst.status = cisConnected
		conInst.statusReport()
	}

	//
	// handshake
	//

	conInst.status = cisInHandshaking
	conInst.statusReport()

	if conInst.dir == conInstDirOutbound {

		if eno := conInst.outboundHandshake(); eno != DhtEnoNone {

			rsp.Eno = int(eno)
			rsp.Peer = &conInst.hsInfo.peer
			rsp.HsInfo = &conInst.hsInfo
			rsp2ConMgr()

			conInst.cleanUp(int(eno))
			return conInst.sdl.SchTaskDone(conInst.ptnMe, sch.SchEnoUserTask)
		}

	} else {

		if eno := conInst.inboundHandshake(); eno != DhtEnoNone {

			rsp.Eno = int(eno)
			rsp.Peer = &conInst.hsInfo.peer
			rsp.HsInfo = &conInst.hsInfo
			rsp2ConMgr()

			conInst.cleanUp(int(eno))
			return conInst.sdl.SchTaskDone(conInst.ptnMe, sch.SchEnoUserTask)
		}
	}

	//
	// response to conntion manager
	//

	conInst.status = cisHandshaked
	conInst.statusReport()

	rsp.Eno = DhtEnoNone
	rsp.Peer = &conInst.hsInfo.peer
	rsp.HsInfo = &conInst.hsInfo
	rsp2ConMgr()

	conInst.status = cisInService
	conInst.statusReport()

	return sch.SchEnoNone
}

//
// Instance-close-request handler
//
func (conInst *ConInst)closeReq(msg *sch.MsgDhtConInstCloseReq) sch.SchErrno {

	if conInst.status != cisHandshaked &&
		conInst.status != cisInService &&
		conInst.dir != conInstDirOutbound {

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
	conInst.status = cisClosed

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
		submitTime:	time.Now(),
		payload:	msg.Data,
	}

	if eno := conInst.txPutPending(&pkg); eno != DhtEnoNone {
		log.LogCallerFileLine("txDataReq: txPutPending failed, eno: %d", eno)
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

//
// Map connection instance status to "peer connection status"
//
func conInstStatus2PCS(cis conInstStatus) conMgrPeerConnStat {
	cis2pcs := map[conInstStatus] conMgrPeerConnStat {
		cisNull:			pcsConnNo,
		cisConnected:		pcsConnNo,
		cisInHandshaking:	pcsConnNo,
		cisHandshaked:		pcsConnNo,
		cisInService:		pcsConnYes,
		cisClosed:			pcsConnNo,
	}
	return cis2pcs[cis]
}

//
// Put outbound package into pending queue
//
func (conInst *ConInst)txPutPending(pkg *conInstTxPkg) DhtErrno {

	conInst.txLock.Lock()
	defer conInst.txLock.Unlock()

	if conInst.txPending.Len() >= txPendingQueueSize {
		log.LogCallerFileLine("txPutPending: queue full")
		return DhtEnoResource
	}

	conInst.txPending.PushBack(pkg)

	return DhtEnoNone
}

//
// Start tx-task
//
func (conInst *ConInst)txTaskStart() DhtErrno {
	go conInst.txProc()
	return DhtEnoNone
}

//
// Start rx-task
//
func (conInst *ConInst)rxTaskStart() DhtErrno {
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

	if conInst.dir != conInstDirOutbound {
		log.LogCallerFileLine("connect2Peer: mismatched direction: %d", conInst.dir)
		return DhtEnoInternal
	}

	peer := conInst.hsInfo.peer
	dialer := &net.Dialer{Timeout: ciConn2PeerTimeout}
	addr := &net.TCPAddr{IP: peer.IP, Port: int(peer.TCP)}

	var conn net.Conn
	var err error

	if conn, err = dialer.Dial("tcp", addr.String()); err != nil {
		log.LogCallerFileLine("connect2Peer: " +
			"dial failed, to: %s, err: %s",
			addr.String(), err.Error())
		return DhtEnoOs
	}

	conInst.con = conn

	return DhtEnoNone
}

//
// Report instance status to connection manager
//
func (conInst *ConInst)statusReport() DhtErrno {

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
	return DhtEnoNone
}

//
// Inbound handshake
//
func (conInst *ConInst)inboundHandshake() DhtErrno {
	return DhtEnoNone
}

//
// Tx routine entry
//
func (conInst *ConInst)txProc() {
}

//
// Rx routine entry
//
func (conInst *ConInst)rxProc() {
}
