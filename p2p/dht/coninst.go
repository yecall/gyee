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
	sdl			*sch.Scheduler			// pointer to scheduler
	name		string					// task name
	tep			sch.SchUserTaskEp		// task entry
	local		*config.Node			// pointer to local node specification
	ptnMe		interface{}				// pointer to myself task node
	ptnConMgr	interface{}				// pointer to connection manager task node
	ptnSrcTsk	interface{}				// for outbound, the source task requests the connection
	status		conInstStatus			// instance status
	hsTimeout	time.Duration			// handshake timeout value
	cid			conInstIdentity			// connection instance identity
	con			net.Conn				// connection
	iow			ggio.WriteCloser		// IO writer
	ior			ggio.ReadCloser			// IO reader
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
	payload		interface{}			// payload buffer
}

//
// Constants related to performance
//
const (
	ciTxPendingQueueSize = 64				// Max tx-pending queue size
	ciConn2PeerTimeout = time.Second * 16	// Connect to peer timeout vale
	ciMaxPackageSize = 1024 * 1024			// bytes
)

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
		con:		nil,
		ior:		nil,
		iow:		nil,
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
	conInst.hsTimeout = msg.DurHs
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

	conInst.status = cisHandshaked
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

	if conInst.txPending.Len() >= ciTxPendingQueueSize {
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
	if conInst.txDone != nil {
		log.LogCallerFileLine("txTaskStart: non-nil chan for done")
		return DhtEnoMismatched
	}
	conInst.txDone = make(chan int, 1)
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
	conInst.ior = ggio.NewDelimitedReader(conn, ciMaxPackageSize)
	conInst.iow = ggio.NewDelimitedWriter(conn)

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

	dhtMsg := new(DhtMessage)
	dhtMsg.Mid = MID_HANDSHAKE
	dhtMsg.Handshake = &Handshake{
		Dir:		conInstDirOutbound,
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
	if hs.Dir != conInstDirOutbound {
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
	if hs.Dir != conInstDirOutbound {
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

	*dhtMsg = DhtMessage{}
	dhtMsg.Mid = MID_HANDSHAKE
	dhtMsg.Handshake = &Handshake{
		Dir:		conInstDirInbound,
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

		conInst.txLock.Lock()
		el := conInst.txPending.Front()
		conInst.txPending.Remove(el)
		conInst.txLock.Unlock()

		if el == nil {
			time.Sleep(time.Microsecond * 10)
			goto _checkDone
		}

		if txPkg, ok = el.Value.(*conInstTxPkg); !ok {
			log.LogCallerFileLine("txProc: mismatched type")
			goto _checkDone
		}

		if dhtPkg, ok = txPkg.payload.(*DhtPackage); !ok {
			log.LogCallerFileLine("txProc: mismatched type")
			goto _checkDone
		}

		pbPkg = new(pb.DhtPackage)
		dhtPkg.ToPbPackage(pbPkg)

		if err := conInst.iow.WriteMsg(pbPkg); err != nil {
			log.LogCallerFileLine("txProc: WriteMsg failed, err: %s", err.Error())
			errUnderlying = true
			break _txLoop
		}

_checkDone:

		select {
		case done := <-conInst.rxDone:
			log.LogCallerFileLine("txProc: done by: %d", done)
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

		conInst.status = cisClosed
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

	log.LogCallerFileLine("txProc: wOw! impossible errors")
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
			log.LogCallerFileLine("rxProc: ReadMsg failed, err: %s", err.Error())
			errUnderlying = true
			break _rxLoop
		}

		pkg := new(DhtPackage)
		pkg.FromPbPackage(pbPkg)
		if pb.ProtocolId(pkg.Pid) == PID_EXT {
			log.LogCallerFileLine("rxProc: PID_EXT is not supported now")
			goto _checkDone
		}

		msg = new(DhtMessage)
		if eno := pkg.GetMessage(msg); eno != DhtEnoNone {
			log.LogCallerFileLine("rxProc: GetMessage failed, eno: %d", eno)
			goto _checkDone
		}

		if eno := conInst.dispatch(msg); eno != DhtEnoNone {
			log.LogCallerFileLine("rxProc: dispatch failed, eno: %d", eno)
		}

_checkDone:

		select {
		case done := <-conInst.txDone:
			isDone = true
			log.LogCallerFileLine("rxProc: done by: %d", done)
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

		conInst.status = cisClosed
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

	log.LogCallerFileLine("rxProc: wOw! impossible errors")
}

//
// messages dispatching
//
func (conInst *ConInst)dispatch(msg *DhtMessage) DhtErrno {

	var eno DhtErrno = DhtEnoUnknown

	switch msg.Mid {

	case MID_HANDSHAKE:
		log.LogCallerFileLine("dispatch: re-handshake is not supported now")
		eno = DhtEnoProtocol

	case MID_FINDNODE:
		eno = conInst.findNode(msg.FindNode)

	case MID_NEIGHBORS:
		eno = conInst.neighbors(msg.Neighbors)

	case MID_PUTVALUE:
		eno = conInst.putValue(msg.PutValue)

	case MID_GETVALUE_REQ:
		eno = conInst.getValueReq(msg.GetValueReq)

	case MID_GETVALUE_RSP:
		eno = conInst.getValueRsp(msg.GetValueRsp)

	case MID_PUTPROVIDER:
		eno = conInst.putProvider(msg.PutProvider)

	case MID_GETPROVIDER_REQ:
		eno = conInst.getProviderReq(msg.GetProviderReq)

	case MID_GETPROVIDER_RSP:
		eno = conInst.getProviderRsp(msg.GetProviderRsp)

	case MID_PING:
		log.LogCallerFileLine("dispatch: MID_PING is not supported now")
		eno = DhtEnoNotSup

	case MID_PONG:
		log.LogCallerFileLine("dispatch: MID_PONG is not supported now")
		eno = DhtEnoNotSup

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
	return DhtEnoNone
}

//
// Handler for "MID_NEIGHBORS" from peer
//
func (conInst *ConInst)neighbors(nb *Neighbors) DhtErrno {
	return DhtEnoNone
}

//
// Handler for "MID_PUTVALUE" from peer
//
func (conInst *ConInst)putValue(pv *PutValue) DhtErrno {
	return DhtEnoNone
}

//
// Handler for "MID_GETVALUE_REQ" from peer
//
func (conInst *ConInst)getValueReq(gvr *GetValueReq) DhtErrno {
	return DhtEnoNone
}

//
// Handler for "MID_GETVALUE_RSP" from peer
//
func (conInst *ConInst)getValueRsp(gvr *GetValueRsp) DhtErrno {
	return DhtEnoNone
}

//
// Handler for "MID_PUTPROVIDER" from peer
//
func (conInst *ConInst)putProvider(pp *PutProvider) DhtErrno {
	return DhtEnoNone
}

//
// Handler for "MID_GETPROVIDER_REQ" from peer
//
func (conInst *ConInst)getProviderReq(gpr *GetProviderReq) DhtErrno {
	return DhtEnoNone
}

//
// Handler for "MID_GETPROVIDER_RSP" from peer
//
func (conInst *ConInst)getProviderRsp(gpr *GetProviderRsp) DhtErrno {
	return DhtEnoNone
}
