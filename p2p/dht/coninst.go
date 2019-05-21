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
	"container/list"
	"io"
	"net"
	"sync"
	"time"

	"github.com/pkg/errors"
	ggio "github.com/gogo/protobuf/io"
	"github.com/yeeco/gyee/log"
	config "github.com/yeeco/gyee/p2p/config"
	pb "github.com/yeeco/gyee/p2p/dht/pb"
	sch "github.com/yeeco/gyee/p2p/scheduler"
)


//
// package identity
//
type txPkgId struct {
	txSeq int64 // moment the package submitted to connection instance
}

//
// Dht connection instance
//
type ConInst struct {
	sdl           *sch.Scheduler    // pointer to scheduler
	sdlName       string            // scheduler name
	name          string            // task name
	chainId		  uint32			// chain identity
	bootstrapNode bool              // bootstrap node flag
	tep           sch.SchUserTaskEp // task entry
	local         *config.Node      // pointer to local node specification
	ptnMe         interface{}       // pointer to myself task node
	ptnDhtMgr     interface{}       // pointer to dht manager task node
	ptnRutMgr     interface{}       // pointer to route manager task node
	ptnDsMgr      interface{}       // pointer to data store manager task node
	ptnPrdMgr     interface{}       // pointer to provider manager task node
	ptnConMgr     interface{}       // pointer to connection manager task node

	//
	// Notice: this is the pointer to the task which asks to establish this connection instance,
	// but this owner task might have been done while the connection instance might be still alived,
	// in current implement, before handshake is completed, this pointer is to the owner task, and
	// after that, this pointer is senseless.
	//

	srcTaskName string      // name of the owner source task
	ptnSrcTsk   interface{} // for outbound, the source task requests the connection

	lock          sync.Mutex                // lock for status updating
	status        conInstStatus             // instance status
	hsTimeout     time.Duration             // handshake timeout value
	cid           conInstIdentity           // connection instance identity
	con           net.Conn                  // connection
	iow           ggio.WriteCloser          // IO writer
	ior           ggio.ReadCloser           // IO reader
	dir           ConInstDir                // connection instance directory
	hsInfo        conInstHandshakeInfo      // handshake information
	txWaitRsp     map[txPkgId]*conInstTxPkg // packages had been sent but waiting for response from peer
	txChan        chan interface{}          // tx pendings signal
	txDone        chan int                  // tx-task done signal
	rxDone        chan int                  // rx-task done signal
	cbRxLock      sync.Mutex                // lock for data plane callback
	cbfRxData     ConInstRxDataCallback     // data plane callback entry
	isBlind       bool                      // is blind connection instance
	txPkgCnt      int64                     // statistics for number of packages sent
	rxPkgCnt      int64                     // statistics for number off package received
	txqDiscardCnt int64                     // number of tx packages discarded for tx queue full
	wrqDiscardCnt int64                     // number of tx packages discarded for wait-response queue full
	trySendingCnt int64                     // number of trying to send data
	dtmDone       chan bool                 // signal DTM to done
	txDtm         *DiffTimerManager         // difference timer manager for response waiting
	txTmCycle     int                       // wait peer response timer cycle in ticks
	bakReq2Conn   map[string]interface{}    // connection request backup map, k: task name, v: message

	// for debug only
	doneCnt			int						// counted for requesting to be done
	doneWhy			[]int					// buffer for why done
}

//
// Call back type for rx data of protocols than PID_DHT
//
type ConInstRxDataCallback func(conInst interface{}, pid uint32, msg interface{}) int

//
// Connection instance identity
//
type conInstIdentity struct {
	nid config.NodeID // node identity
	dir ConInstDir    // connection direction
}

//
// Connection instance status
//
const (
	CisNull          = iota // null, not inited
	CisConnecting           // connecting
	CisConnected            // connected
	CisAccepted             // accepted
	CisInHandshaking        // handshaking
	CisHandshook            // handshook
	CisInService            // in service
	CisOutOfService         // out of service but is not closed
	CisClosed               // closed
)

type conInstStatus int

//
// Connection instance direction
//
const (
	ConInstDirInbound  = 0  // out from local
	ConInstDirOutbound = 1  // in from peer
	ConInstDirAllbound = 2  // in & out
	ConInstDirUnknown  = -1 // not be initialized
)

type ConInstDir = int

//
// Handshake information
//
type conInstHandshakeInfo struct {
	peer  config.Node // peer node identity
	extra interface{} // extra information
}

//
// Outcoming package
//
type conInstTxPkg struct {
	taskName string      // task name
	task     interface{} // pointer to owner task node

	responsed chan bool // wait response from peer signal. notice: this chan is not applied for
	// syncing as a signal really in current implement, instead, it is used
	// as a flag for response checking, if not nil, a package sent would be
	// push into queue (ConInst.txWaitRsp), and timer start for response,
	// no other module access this filed now.

	waitMid    int         // wait message identity
	waitSeq    int64       // wait message sequence number
	submitTime time.Time   // time the payload submitted
	payload    interface{} // payload buffer
	txTid      interface{} // wait peer response timer
}

//
// Constants related to performance
//
const (
	ciTxPendingQueueSize    = 4096            // max tx-pending queue size
	ciConn2PeerTimeout      = time.Second * 16 // Connect to peer timeout vale
	ciMaxPackageSize        = 1024 * 1024     // bytes
	ciTxTimerDuration       = time.Second * 8 // tx timer duration
	ciTxDtmTick             = time.Second * 1 // tx difference timer mananger tick
	ciTxMaxWaitResponseSize = 4096            // tx max wait peer response queue size
	ciHandshakeTimeout	= time.Second * 8	// handshake timeout
)

//
// Create connection instance
//
func newConInst(postFixed string, isBlind bool) *ConInst {
	conInst := ConInst{
		name:        "conInst" + postFixed,
		status:      CisNull,
		dir:         ConInstDirUnknown,
		txWaitRsp:   make(map[txPkgId]*conInstTxPkg, 0),
		isBlind:     isBlind,
		txPkgCnt:    0,
		rxPkgCnt:    0,
		txDone:      nil,
		rxDone:      nil,
		con:         nil,
		dtmDone:     make(chan bool, 1),
		txDtm:       NewDiffTimerManager(ciTxDtmTick, nil),
		bakReq2Conn: make(map[string]interface{}, 0),
	}
	conInst.tep = conInst.conInstProc
	return &conInst
}

//
// Entry point exported to shceduler
//
func (conInst *ConInst) TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return conInst.tep(ptn, msg)
}

//
// Connection instance entry
//
func (conInst *ConInst) conInstProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	if msg.Id != sch.EvDhtConInstTxDataReq && msg.Id != sch.EvDhtQryInstProtoMsgInd {
		log.Tracef("conInstProc: sdl: %s, inst: %s, msg.Id: %d", conInst.sdlName, conInst.name, msg.Id)
	}

	var eno = sch.SchEnoUnknown

	switch msg.Id {

	case sch.EvSchPoweron:
		eno = conInst.poweron(ptn)

	case sch.EvSchPoweroff:
		eno = conInst.poweroff(ptn)

	case sch.EvDhtConInstHandshakeReq:
		eno = conInst.handshakeReq(msg.Body.(*sch.MsgDhtConInstHandshakeReq))

	case sch.EvDhtConInstStartupReq:
		eno = conInst.startUpReq(msg.Body.(*sch.MsgDhtConInstStartupReq))

	case sch.EvDhtConInstCloseReq:
		eno = conInst.closeReq(msg.Body.(*sch.MsgDhtConInstCloseReq))

	case sch.EvDhtConInstTxDataReq:
		eno = conInst.txDataReq(msg.Body.(*sch.MsgDhtConInstTxDataReq))

	case sch.EvDhtRutMgrNearestRsp:
		eno = conInst.rutMgrNearestRsp(msg.Body.(*sch.MsgDhtRutMgrNearestRsp))

	case sch.EvDhtQryInstProtoMsgInd:
		eno = conInst.protoMsgInd(msg.Body.(*sch.MsgDhtQryInstProtoMsgInd))

	default:
		log.Debugf("conInstProc: unknown event: %d", msg.Id)
		eno = sch.SchEnoParameter
	}

	if msg.Id != sch.EvDhtConInstTxDataReq && msg.Id != sch.EvDhtQryInstProtoMsgInd {
		log.Tracef("conInstProc: get out, sdl: %s, inst: %s, msg.Id: %d", conInst.sdlName, conInst.name, msg.Id)
	}

	return eno
}

//
// Poweron handler
//
func (conInst *ConInst) poweron(ptn interface{}) sch.SchErrno {
	if conInst.ptnMe != ptn {
		log.Debugf("poweron: sdl: %s, inst: %s, dir: %d, task mismatched",
			conInst.sdlName, conInst.name, conInst.dir)
		return sch.SchEnoMismatched
	}

	log.Debugf("poweron: sdl: %s, inst: %s, dir: %d",
		conInst.sdlName, conInst.name, conInst.dir)

	conInst.txDtm.setCallback(conInst.txTimerHandler)
	conInst.txTmCycle, _ = conInst.txDtm.dur2Ticks(ciTxTimerDuration)

	if conInst.dir == ConInstDirInbound {

		if conInst.statusReport() != DhtEnoNone {
			return sch.SchEnoUserTask
		}
		conInst.updateStatus(CisConnected)
		return sch.SchEnoNone

	} else if conInst.dir == ConInstDirOutbound {

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
func (conInst *ConInst) poweroff(ptn interface{}) sch.SchErrno {
	log.Debugf("poweroff: sdl: %s, inst: %s, dir: %d, task will be done ...",
		conInst.sdlName, conInst.name, conInst.dir)
	conInst.cleanUp(DhtEnoScheduler.GetEno())
	return conInst.sdl.SchTaskDone(conInst.ptnMe, conInst.name, sch.SchEnoKilled)
}

//
// Handshake-request handler
//
func (conInst *ConInst) handshakeReq(msg *sch.MsgDhtConInstHandshakeReq) sch.SchErrno {

	//
	// if handshake failed, the instance task should done itself, and send handshake
	// response message to connection manager task.
	//

	log.Debugf("handshakeReq: sdl: %s, inst: %s, dir: %d",
		conInst.sdlName, conInst.name, conInst.dir)

	rsp := sch.MsgDhtConInstHandshakeRsp{
		Eno:    DhtEnoUnknown.GetEno(),
		Inst:   conInst,
		Peer:   nil,
		Dir:    int(conInst.dir),
		HsInfo: nil,
		Dur:    time.Duration(-1),
	}

	rsp2ConMgr := func() sch.SchErrno {
		if conInst.con != nil {
			log.Debugf("handshakeReq: rsp2ConMgr, " +
				"sdl: %s, inst: %s, dir: %d, localAddr: %s, remoteAddr: %s, eno: %d",
				conInst.sdlName, conInst.name, conInst.dir, conInst.con.LocalAddr().String(),
				conInst.con.RemoteAddr().String(), rsp.Eno)
		} else {
			log.Debugf("handshakeReq: rsp2ConMgr, " +
				"sdl: %s, inst: %s, dir: %d, localAddr: %s, remoteAddr: %s, eno: %d",
				conInst.sdlName, conInst.name, conInst.dir, "none", "none", rsp.Eno)
		}
		schMsg := sch.SchMessage{}
		conInst.sdl.SchMakeMessage(&schMsg, conInst.ptnMe, conInst.ptnConMgr, sch.EvDhtConInstHandshakeRsp, &rsp)
		return conInst.sdl.SchSendMessage(&schMsg)
	}

	//
	// connect to peer if it's not
	//

	if conInst.dir == ConInstDirOutbound {
		if conInst.con != nil {
			log.Errorf("handshakeReq: dirty connection, sdl: %s", conInst.sdlName)
		}
		conInst.updateStatus(CisConnecting)
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

		log.Debugf("handshakeReq: connect ok, sdl: %s, inst: %s, dir: %d, localAddr: %s, remoteAddr: %s",
			conInst.sdlName, conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String())

		conInst.updateStatus(CisConnected)
		conInst.statusReport()
	}

	//
	// handshake: in practice, we find that it might be blocked for ever in this procedure when reading
	// from or writing data to the connection, even dead line is set. following statements invoke the
	// "Close" interface of ggio.WriteCloser and ggio.ReadCloser to force the procedure to get out when
	// timer expired.
	//

	conInst.updateStatus(CisInHandshaking)
	conInst.hsTimeout = msg.DurHs
	conInst.statusReport()

	log.Debugf("handshakeReq: sdl: %s, inst: %s, dir: %d, localAddr: %s, remoteAddr: %s",
		conInst.sdlName, conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String())

	hsTm := time.NewTimer(ciHandshakeTimeout)
	defer hsTm.Stop()
	hsCh := make(chan DhtErrno, 1)

	cleanUp := func() {
		conInst.iow.Close()
		conInst.ior.Close()
		conInst.con.Close()
	}

	go func() {
		if conInst.dir == ConInstDirOutbound {
			hsCh<-conInst.outboundHandshake()
		} else if conInst.dir == ConInstDirInbound {
			hsCh<-conInst.inboundHandshake()
		} else {
			log.Debugf("handshakeReq: sdl: %s, inst: %s, dir: %d, localAddr: %s, remoteAddr: %s",
				conInst.sdlName, conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String())
			hsCh<-DhtEnoInternal
		}
	}()

	select {
	case <-hsTm.C:
		cleanUp()
		rsp.Eno = DhtEnoTimeout.GetEno()
		if conInst.dir == ConInstDirOutbound {
			peer := conInst.hsInfo.peer
			hsInfo := conInst.hsInfo
			rsp.Peer = &peer
			rsp.Inst = conInst
			rsp.HsInfo = &hsInfo
		}
		return rsp2ConMgr()
	case hsEno, ok := <-hsCh:
		if !ok {
			log.Errorf("handshakeReq: impossible result, sdl: %s", conInst.sdlName)
			hsEno = DhtEnoInternal
		}
		if hsEno != DhtEnoNone {
			cleanUp()
			if conInst.dir == ConInstDirOutbound {
				peer := conInst.hsInfo.peer
				hsInfo := conInst.hsInfo
				rsp.Eno = int(hsEno)
				rsp.Peer = &peer
				rsp.Inst = conInst
				rsp.HsInfo = &hsInfo
				return rsp2ConMgr()
			} else if conInst.dir == ConInstDirInbound {
				rsp.Eno = int(hsEno)
				rsp.Peer = nil
				rsp.HsInfo = nil
				rsp.Inst = conInst
				return rsp2ConMgr()
			}
		}
	}

	conInst.updateStatus(CisHandshook)
	conInst.statusReport()

	rsp.Eno = DhtEnoNone.GetEno()
	rsp.Peer = &conInst.hsInfo.peer
	rsp.HsInfo = &conInst.hsInfo
	return rsp2ConMgr()
}

//
// service startup
//
func (conInst *ConInst) startUpReq(msg *sch.MsgDhtConInstStartupReq) sch.SchErrno {
	log.Debugf("startUpReq: enter, start rx/tx and confirm peMgr, " +
		"sdl: %s, inst: %s, dir: %d, localAddr: %s, remoteAddr: %s",
		conInst.sdlName, conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String())

	conInst.updateStatus(CisInService)
	conInst.con.SetDeadline(time.Time{})
	conInst.statusReport()
	conInst.txTaskStart()
	conInst.rxTaskStart()
	msg.EnoCh <- DhtEnoNone.GetEno()

	log.Debugf("startUpReq: ok, exit, " +
		"sdl: %s, inst: %s, dir: %d, localAddr: %s, remoteAddr: %s",
		conInst.sdlName, conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String())

	return sch.SchEnoNone
}

//
// Instance-close-request handler
//
func (conInst *ConInst) closeReq(msg *sch.MsgDhtConInstCloseReq) sch.SchErrno {
	if status := conInst.getStatus(); status >= CisClosed {
		log.Debugf("closeReq: sdl: %s, inst: %s, dir: %d, why: %d, status mismatched: %d",
			conInst.sdlName, conInst.name, conInst.dir, msg.Why, status)
		return sch.SchEnoMismatched
	}

	log.Debugf("closeReq: sdl: %s, inst: %s, dir: %d, why: %d, status: %d",
		conInst.sdlName, conInst.name, conInst.dir, msg.Why, conInst.getStatus())

	if *msg.Peer != conInst.hsInfo.peer.ID {
		log.Debugf("closeReq: sdl: %s, inst: %s, peer node identity mismatched",
			conInst.sdlName, conInst.name)
		return sch.SchEnoMismatched
	}

	conInst.cleanUp(msg.Why)
	conInst.updateStatus(CisClosed)
	conInst.statusReport()

	log.Debugf("closeReq: send EvDhtConInstCloseRsp, sdl: %s, inst: %s, dir: %d, why: %d, status: %d, peer: %x",
		conInst.sdlName, conInst.name, conInst.dir, msg.Why, conInst.getStatus(), conInst.hsInfo.peer.ID)

	rsp := sch.MsgDhtConInstCloseRsp{
		Peer: &conInst.hsInfo.peer.ID,
		Dir:  int(conInst.dir),
	}
	schMsg := sch.SchMessage{}
	conInst.sdl.SchMakeMessage(&schMsg, conInst.ptnMe, conInst.ptnConMgr, sch.EvDhtConInstCloseRsp, &rsp)
	conInst.sdl.SchSendMessage(&schMsg)
	return conInst.sdl.SchTaskDone(conInst.ptnMe, conInst.name, sch.SchEnoKilled)
}

//
// Send-data-request handler
//
func (conInst *ConInst) txDataReq(msg *sch.MsgDhtConInstTxDataReq) sch.SchErrno {

	//
	// notice: the EvDhtConInstTxDataReq events are always sent from connection
	// instance itself than anyone else. see event EvDhtConMgrSendReq handler in
	// connection manager for more please.
	//

	if s := conInst.getStatus(); s != CisInService {
		log.Debugf("txDataReq: not it CisInService, status: %d", s)
		return sch.SchEnoUserTask
	}

	pkg := conInstTxPkg{
		task:       msg.Task,
		responsed:  nil,
		waitMid:    -1,
		waitSeq:    -1,
		submitTime: time.Now(),
		payload:    msg.Payload,
	}

	if msg.WaitRsp == true {
		pkg.responsed = make(chan bool, 1)
		pkg.waitMid = msg.WaitMid
		pkg.waitSeq = msg.WaitSeq
	}

	if eno := conInst.txPutPending(&pkg); eno != DhtEnoNone {
		log.Debugf("txDataReq: txPutPending failed, eno: %d", eno)
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

//
// Nearest response handler
//
func (conInst *ConInst) rutMgrNearestRsp(msg *sch.MsgDhtRutMgrNearestRsp) sch.SchErrno {

	//
	// notice: here the response must for "Find_NODE" from peers, see function
	// dispatch for more please.
	//

	curStat := conInst.getStatus()
	if curStat != CisInService {
		log.Debugf("rutMgrNearestRsp: not in CisInService, curStat: %d", curStat)
		return sch.SchEnoMismatched
	}

	if msg == nil {
		log.Errorf("rutMgrNearestRsp: invalid parameters")
		return sch.SchEnoParameter
	}
	log.Debugf("rutMgrNearestRsp: msg: %+v", msg)

	var dhtMsg = DhtMessage{
		Mid: MID_UNKNOWN,
	}
	var nodes []*config.Node
	bns, ok := msg.Peers.([]*rutMgrBucketNode)
	if !ok {
		log.Errorf("rutMgrNearestRsp: invalid parameters")
		return sch.SchEnoParameter
	}

	for idx := 0; idx < len(bns); idx++ {
		nodes = append(nodes, &bns[idx].node)
	}

	if msg.ForWhat == MID_FINDNODE {

		if msg.Msg == nil {
			log.Debugf("rutMgrNearestRsp: original message is nil")
			return sch.SchEnoMismatched
		}

		findNode, ok := msg.Msg.(*FindNode)
		if !ok {
			log.Errorf("rutMgrNearestRsp: invalid original message type")
			return sch.SchEnoMismatched
		}

		nbs := Neighbors{
			From:  *conInst.local,
			To:    conInst.hsInfo.peer,
			Nodes: nodes,
			Pcs:   msg.Pcs.([]int),
			Id:    findNode.Id,
			Extra: nil,
		}

		dhtMsg = DhtMessage{
			Mid:       MID_NEIGHBORS,
			Neighbors: &nbs,
		}

	} else if msg.ForWhat == MID_GETPROVIDER_REQ {

		log.Debugf("rutMgrNearestRsp: for MID_GETPROVIDER_REQ should not come here")
		return sch.SchEnoMismatched

	} else if msg.ForWhat == MID_GETVALUE_REQ {

		log.Debugf("rutMgrNearestRsp: for MID_GETVALUE_REQ should not come here")
		return sch.SchEnoMismatched

	} else {

		log.Debugf("rutMgrNearestRsp: unknown what's for")
		return sch.SchEnoMismatched
	}

	dhtPkg := DhtPackage{}
	if eno := dhtMsg.GetPackage(&dhtPkg); eno != DhtEnoNone {
		log.Debugf("rutMgrNearestRsp: GetPackage failed, eno: %d", eno)
		return sch.SchEnoUserTask
	}

	txPkg := conInstTxPkg{
		task:       conInst.ptnMe,
		responsed:  nil,
		waitMid:    -1,
		waitSeq:    -1,
		submitTime: time.Now(),
		payload:    &dhtPkg,
	}

	if eno := conInst.txPutPending(&txPkg); eno != DhtEnoNone {
		log.Debugf("rutMgrNearestRsp: txPutPending failed, eno: %d", eno)
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

//
// Map connection instance status to "peer connection status"
//
func conInstStatus2PCS(cis conInstStatus) conMgrPeerConnStat {
	cis2pcs := map[conInstStatus]conMgrPeerConnStat{
		CisNull:          pcsConnNo,
		CisConnected:     pcsConnNo,
		CisInHandshaking: pcsConnNo,
		CisHandshook:     pcsConnYes,
		CisInService:     pcsConnYes,
		CisClosed:        pcsConnNo,
	}
	return cis2pcs[cis]
}

//
// Put outbound package into pending queue
//
func (conInst *ConInst) txPutPending(pkg *conInstTxPkg) DhtErrno {

	if pkg == nil {
		log.Warnf("txPutPending: invalid parameter, inst: %s, hsInfo: %+v, local: %+v",
			conInst.name, conInst.hsInfo, *conInst.local)
		return DhtEnoParameter
	}

	if status := conInst.getStatus(); status != CisInService {
		log.Warnf("txPutPending: mismatched status: %d", status)
		return DhtEnoMismatched
	}

	if conInst.trySendingCnt += 1; conInst.trySendingCnt & 0xff == 0 {
		log.Debugf("txPutPending: trySendingCnt: %d, peer: %s:%d",
			conInst.trySendingCnt, conInst.hsInfo.peer.IP.String(), conInst.hsInfo.peer.TCP)
	}

	if len(conInst.txChan) >= cap(conInst.txChan) {
		log.Warnf("txPutPending: pending queue full, inst: %s, hsInfo: %s:%d",
			conInst.name, conInst.hsInfo.peer.IP.String(), conInst.hsInfo.peer.TCP)
		if conInst.txqDiscardCnt += 1; conInst.txqDiscardCnt & 0x1f == 0 {
			log.Debugf("txPutPending: txqDiscardCnt: %d, peer: %s:%d",
				conInst.txqDiscardCnt, conInst.hsInfo.peer.IP.String(), conInst.hsInfo.peer.TCP)
		}
		return DhtEnoResource
	}

	if len(conInst.txWaitRsp) >= ciTxMaxWaitResponseSize {
		log.Warnf("txPutPending: waiting response queue full, inst: %s, hsInfo: %+v, local: %+v",
			conInst.name, conInst.hsInfo, *conInst.local)
		if conInst.wrqDiscardCnt += 1; conInst.wrqDiscardCnt & 0x1f == 0 {
			log.Debugf("txPutPending: wrqDiscardCnt: %d, peer: %s:%d",
				conInst.wrqDiscardCnt, conInst.hsInfo.peer.IP.String(), conInst.hsInfo.peer.TCP)
		}
		return DhtEnoResource
	}

	conInst.txChan <- pkg

	log.Debugf("txPutPending: put, inst: %s, hsInfo: %+v, local: %+v, waitMid: %d, waitSeq: %d",
		conInst.name, conInst.hsInfo, *conInst.local, pkg.waitMid, pkg.waitSeq)

	return DhtEnoNone
}

//
// Set timer for tx-package which would wait response from peer
//
func (conInst *ConInst) txSetPkgTimer(userData interface{}) DhtErrno {
	conInst.txDtm.lock.Lock()
	defer conInst.txDtm.lock.Unlock()
	if txPkg, ok := userData.(*conInstTxPkg); ok {
		tid, err := conInst.txDtm.setTimer(userData, conInst.txTmCycle)
		if tid == nil || err != nil {
			log.Errorf("setTimer: failed")
			return DhtEnoTimer
		}
		txPkg.txTid = tid
		return DhtEnoNone
	}
	return DhtEnoParameter
}

//
// Tx timer expired event handler
//
func (conInst *ConInst) txTimerHandler(userData interface{}) error {
	if userData == nil {
		log.Errorf("txTimerHandler: invalid parameter, inst: %s", conInst.name)
		return sch.SchEnoParameter
	}
	txPkg, ok := userData.(*conInstTxPkg)
	if !ok || txPkg == nil {
		log.Errorf("txTimerHandler: invalid parameter, inst: %s", conInst.name)
		return sch.SchEnoMismatched
	}

	pkgId := txPkgId{txSeq: txPkg.waitSeq}
	_, ok = conInst.txWaitRsp[pkgId]
	if !ok {
		log.Debugf("txTimerHandler: not found, seq: %d", txPkg.waitSeq)
		return sch.SchEnoMismatched
	}

	eno, ptn := conInst.sdl.SchGetUserTaskNode(txPkg.taskName)
	if eno == sch.SchEnoNone && ptn != nil && ptn == txPkg.task {
		if txPkg.task != nil {
			ind := sch.MsgDhtConInstTxInd{
				Eno:     DhtEnoTimeout.GetEno(),
				WaitMid: txPkg.waitMid,
				WaitSeq: txPkg.waitSeq,
			}
			schMsg := sch.SchMessage{}
			conInst.sdl.SchMakeMessage(&schMsg, conInst.ptnMe, txPkg.task, sch.EvDhtConInstTxInd, &ind)
			conInst.sdl.SchSendMessage(&schMsg)
		}
	}

	if txPkg.responsed != nil {
		close(txPkg.responsed)
	}
	delete(conInst.txWaitRsp, pkgId)

	return sch.SchEnoNone
}

func (conInst *ConInst) protoMsgInd(msg *sch.MsgDhtQryInstProtoMsgInd) sch.SchErrno {

	var eno DhtErrno
	var txPkg *conInstTxPkg

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
		log.Debugf("protoMsgInd: invalid indication, for: %d", msg.ForWhat)
		return sch.SchEnoParameter
	}

	if eno == DhtEnoNone && txPkg != nil {
		_, ptn := conInst.sdl.SchGetUserTaskNode(txPkg.taskName)
		if ptn != nil && ptn == txPkg.task {
			schMsg := new(sch.SchMessage)
			conInst.sdl.SchMakeMessage(schMsg, conInst.ptnMe, txPkg.task, sch.EvDhtQryInstProtoMsgInd, msg)
			conInst.sdl.SchSendMessage(schMsg)
		}
	}

	return sch.SchEnoNone
}

//
// Set current Tx pending
//
func (conInst *ConInst) txSetPending(txPkg *conInstTxPkg) DhtErrno {
	conInst.txDtm.lock.Lock()
	defer conInst.txDtm.lock.Unlock()
	if txPkg != nil {
		txPkg.taskName = conInst.sdl.SchGetTaskName(txPkg.task)
		if len(txPkg.taskName) == 0 {
			log.Debugf("txSetPending: task without name")
			return DhtEnoScheduler
		}
		pkgId := txPkgId{txSeq: txPkg.waitSeq}
		conInst.txWaitRsp[pkgId] = txPkg
	}
	return DhtEnoNone
}

//
// Start tx-task
//
func (conInst *ConInst) txTaskStart() DhtErrno {
	conInst.txDone = make(chan int)
	conInst.txChan = make(chan interface{}, ciTxPendingQueueSize)
	go conInst.txProc()
	return DhtEnoNone
}

//
// Start rx-task
//
func (conInst *ConInst) rxTaskStart() DhtErrno {
	conInst.rxDone = make(chan int)
	go conInst.rxProc()
	return DhtEnoNone
}

//
// Stop tx-task
//
func (conInst *ConInst) txTaskStop(why int) DhtErrno {

	// notic: conInst.iow.Close() should be called or the rx task might be blocked
	// in writing so deadlock produced.

	if conInst.txDone != nil {

		close(conInst.txChan)
		log.Debugf("txTaskStop: txChan closed, sdl: %s, inst: %s, ", conInst.sdlName, conInst.name)

		conInst.lock.Lock()
		if conInst.con != nil {
			conInst.iow.Close()
			conInst.con.Close()
			conInst.con = nil
			log.Debugf("txTaskStop: iow&con closed, sdl: %s, inst: %s", conInst.sdlName, conInst.name)
		}
		conInst.lock.Unlock()

		conInst.txDone <- why
		log.Debugf("txTaskStop: txDone signaled, sdl: %s, inst: %s", conInst.sdlName, conInst.name)

		done := <-conInst.txDone
		log.Debugf("txTaskStop: txDone feedback, sdl: %s, inst: %s", conInst.sdlName, conInst.name)

		close(conInst.txDone)
		conInst.txDone = nil
		log.Debugf("txTaskStop: txDone closed: sdl: %s, inst: %s", conInst.sdlName, conInst.name)

		return DhtErrno(done)

	} else if conInst.con != nil {

		conInst.iow.Close()
		conInst.con.Close()
		conInst.con = nil
		log.Debugf("txTaskStop: iow&con closed, sdl: %s, inst: %s", conInst.sdlName, conInst.name)
	}

	return DhtEnoNone
}

//
// Stop rx-task
//
func (conInst *ConInst) rxTaskStop(why int) DhtErrno {

	// notic: conInst.ior.Close() should be called or the rx task might be blocked
	// in reading so deadlock produced.

	if conInst.rxDone != nil {

		conInst.lock.Lock()
		if conInst.con != nil {
			conInst.ior.Close()
			conInst.con.Close()
			conInst.con = nil
			log.Debugf("rxTaskStop: ior&con closed, sdl: %s, inst: %s", conInst.sdlName, conInst.name)
		}
		conInst.lock.Unlock()

		conInst.rxDone <- why
		log.Debugf("rxTaskStop: rxDone signaled, sdl: %s, inst: %s", conInst.sdlName, conInst.name)

		done := <-conInst.rxDone
		log.Debugf("rxTaskStop: rxDone feedback, sdl: %s, inst: %s", conInst.sdlName, conInst.name)

		close(conInst.rxDone)
		conInst.rxDone = nil
		log.Debugf("rxTaskStop: rxDone closed: sdl: %s, inst: %s", conInst.sdlName, conInst.name)

		return DhtErrno(done)

	} else if conInst.con != nil {

		conInst.ior.Close()
		conInst.con.Close()
		conInst.con = nil
		log.Debugf("rxTaskStop: ior&con closed, sdl: %s, inst: %s", conInst.sdlName, conInst.name)
	}

	return DhtEnoNone
}

//
// Cleanup the instance
//
func (conInst *ConInst) cleanUp(why int) DhtErrno {

	log.Debugf("cleanUp: sdl: %s, inst: %s, why: %d",	conInst.sdlName, conInst.name, why)

	conInst.txTaskStop(why)
	log.Debugf("cleanUp: tx done, sdl: %s, inst: %s", conInst.sdlName, conInst.name)

	conInst.rxTaskStop(why)
	log.Debugf("cleanUp: rx done, sdl: %s, inst: %s", conInst.sdlName, conInst.name)

	close(conInst.dtmDone)
	log.Debugf("cleanUp: dtmDone signaled, sdl: %s, inst: %s", conInst.sdlName, conInst.name)

	return DhtEnoNone
}

//
// Connect to peer
//
func (conInst *ConInst) connect2Peer() DhtErrno {

	if conInst.dir != ConInstDirOutbound {
		log.Debugf("connect2Peer: mismatched direction: inst: %s, dir: %d",
			conInst.name, conInst.dir)
		return DhtEnoInternal
	}

	peer := conInst.hsInfo.peer
	dialer := &net.Dialer{Timeout: ciConn2PeerTimeout}
	addr := &net.TCPAddr{IP: peer.IP, Port: int(peer.TCP)}

	log.Debugf("connect2Peer: try to connect, " +
		"inst: %s, dir: %d, local: %s, remote: %s",
		conInst.name, conInst.dir,
		conInst.local.IP.String(),
		addr.String())

	var conn net.Conn
	var err error

	if conn, err = dialer.Dial("tcp", addr.String()); err != nil {
		log.Debugf("connect2Peer: " +
			"dial failed, inst: %s, dir: %d, local: %s, to: %s, err: %s",
			conInst.name, conInst.dir, conInst.local.IP.String(),
			addr.String(), err.Error())
		return DhtEnoOs
	}

	conInst.con = conn
	r := conInst.con.(io.Reader)
	conInst.ior = ggio.NewDelimitedReader(r, ciMaxPackageSize)
	w := conInst.con.(io.Writer)
	conInst.iow = ggio.NewDelimitedWriter(w)

	log.Debugf("connect2Peer: connect ok, " +
		"inst: %s, dir: %d, local: %s, remote: %s",
		conInst.name, conInst.dir,
		conn.LocalAddr().String(),
		conn.RemoteAddr().String())

	return DhtEnoNone
}

//
// Report instance status to connection manager
//
func (conInst *ConInst) statusReport() DhtErrno {

	//
	// notice: during the lifetime of the connection instance, the "Peer" might be
	// still not known at some time. for example, when just connection be accepted
	// and handshake procedure is not completed, so one must check the direction and
	// status of a connection instance to apply the "peer" information indicated by
	// the following message.
	//
	ind := sch.MsgDhtConInstStatusInd{
		Peer:   &conInst.hsInfo.peer.ID,
		Dir:    int(conInst.dir),
		Status: int(conInst.getStatus()),
	}
	msg := sch.SchMessage{}
	conInst.sdl.SchMakeMessage(&msg, conInst.ptnMe, conInst.ptnConMgr, sch.EvDhtConInstStatusInd, &ind)
	if conInst.sdl.SchSendMessage(&msg) != sch.SchEnoNone {
		return DhtEnoScheduler
	}
	return DhtEnoNone
}

//
// Outbound handshake
//
func (conInst *ConInst) outboundHandshake() DhtErrno {

	log.Debugf("outboundHandshake: begin, inst: %s, dir: %d, local: %s, remote: %s",
		conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String())

	dhtMsg := new(DhtMessage)
	dhtMsg.Mid = MID_HANDSHAKE
	dhtMsg.Handshake = &Handshake{
		ChainId: conInst.chainId,
		Dir:      ConInstDirOutbound,
		NodeId:   conInst.local.ID,
		IP:       conInst.local.IP,
		UDP:      uint32(conInst.local.UDP),
		TCP:      uint32(conInst.local.TCP),
		ProtoNum: 1,
		Protocols: []DhtProtocol{
			{
				Pid: uint32(PID_DHT),
				Ver: DhtVersion,
			},
		},
	}

	pbPkg := dhtMsg.GetPbPackage()
	if pbPkg == nil {
		log.Debugf("outboundHandshake: GetPbPackage failed, " +
			"inst: %s, dir: %d, local: %s, remote: %s",
			conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String())
		return DhtEnoSerialization
	}

	conInst.con.SetDeadline(time.Now().Add(conInst.hsTimeout))
	if err := conInst.iow.WriteMsg(pbPkg); err != nil {
		log.Debugf("outboundHandshake: WriteMsg failed, " +
			"inst: %s, dir: %d, local: %s, remote: %s, error: %s",
			conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String(),
			err.Error())
		return DhtEnoSerialization
	}

	*pbPkg = pb.DhtPackage{}
	conInst.con.SetDeadline(time.Now().Add(conInst.hsTimeout))
	if err := conInst.ior.ReadMsg(pbPkg); err != nil {
		log.Debugf("outboundHandshake: ReadMsg failed, " +
			"inst: %s, dir: %d, local: %s, remote: %s, error: %s",
			conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String(),
			err.Error())
		return DhtEnoSerialization
	}

	if *pbPkg.Pid != PID_DHT {
		log.Debugf("outboundHandshake: invalid pid, " +
			"inst: %s, dir: %d, local: %s, remote: %s, pid: %d",
			conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String(),
			pbPkg.Pid)
		return DhtEnoProtocol
	}

	if *pbPkg.PayloadLength <= 0 {
		log.Debugf("outboundHandshake: invalid payload length, " +
			"inst: %s, dir: %d, local: %s, remote: %s, length: %d",
			conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String(),
			*pbPkg.PayloadLength)
		return DhtEnoProtocol
	}

	if len(pbPkg.Payload) != int(*pbPkg.PayloadLength) {
		log.Debugf("outboundHandshake: payload length mismatched, " +
			"inst: %s, dir: %d, local: %s, remote: %s, PlLen: %d, real: %d",
			conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String(),
			*pbPkg.PayloadLength, len(pbPkg.Payload))
		return DhtEnoProtocol
	}

	dhtPkg := new(DhtPackage)
	dhtPkg.Pid = uint32(*pbPkg.Pid)
	dhtPkg.PayloadLength = *pbPkg.PayloadLength
	dhtPkg.Payload = pbPkg.Payload

	*dhtMsg = DhtMessage{}
	if eno := dhtPkg.GetMessage(dhtMsg); eno != DhtEnoNone {
		log.Debugf("outboundHandshake: GetMessage failed, " +
			"inst: %s, dir: %d, local: %s, remote: %s, eno: %d",
			conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String(),
			eno)
		return eno
	}

	if dhtMsg.Mid != MID_HANDSHAKE {
		log.Debugf("outboundHandshake: invalid MID, " +
			"inst: %s, dir: %d, local: %s, remote: %s, MID: %d",
			conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String(),
			dhtMsg.Mid)
		return DhtEnoProtocol
	}

	hs := dhtMsg.Handshake
	if hs.ChainId != conInst.chainId {
		log.Warnf("outboundHandshake: mismatched chain identity, " +
			"inst: %s, local: %s, remote: %s, id:[%d,%d]",
			conInst.name, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String(),
			conInst.chainId, hs.ChainId)
		return DhtEnoMismatched
	}
	if hs.Dir != ConInstDirInbound {
		log.Debugf("outboundHandshake: mismatched direction, " +
			"inst: %s, dir: %d, local: %s, remote: %s, hsdir: %d",
			conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String(),
			hs.Dir)
		return DhtEnoMismatched
	}

	//
	// notice: when try outbound, the peer(conInst.hsInfo.peer) is alway known
	// before the handshaking, but here after the handshaking ok, we update the
	// peer according what we obtained in the procedure as following, and this
	// can be different from that we had believed it would be. the connection
	// manager must handle this case(or we can check against this here, and if
	// it's the case, we return failed, so the connection manager would drop
	// this connection later). see function handshakeRsp in connection.go for
	// detail please.
	//

	conInst.hsInfo.peer = config.Node{
		IP:  hs.IP,
		TCP: uint16(hs.TCP & 0xffff),
		UDP: uint16(hs.UDP & 0xffff),
		ID:  hs.NodeId,
	}

	log.Debugf("outboundHandshake: end ok, inst: %s, dir: %d, local: %s, remote: %s",
		conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String())

	return DhtEnoNone
}

//
// Inbound handshake
//
func (conInst *ConInst) inboundHandshake() DhtErrno {

	log.Debugf("inboundHandshake: begin, inst: %s, dir: %d, local: %s, remote: %s",
		conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String())

	pkg := new(pb.DhtPackage)
	conInst.con.SetDeadline(time.Now().Add(conInst.hsTimeout))
	if err := conInst.ior.ReadMsg(pkg); err != nil {
		log.Debugf("inboundHandshake: ReadMsg failed, " +
			"inst: %s, local: %s, remote: %s, error: %s",
			conInst.name, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String(),
			err.Error())
		return DhtEnoSerialization
	}

	if *pkg.Pid != PID_DHT {
		log.Debugf("inboundHandshake: invalid pid, " +
			"inst: %s, local: %s, remote: %s, pid: %d",
			conInst.name, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String(),
			pkg.Pid)
		return DhtEnoProtocol
	}

	if *pkg.PayloadLength <= 0 {
		log.Debugf("inboundHandshake: invalid payload length, " +
			"inst: %s, local: %s, remote: %s, PayloadLength: %d",
			conInst.name, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String(),
			*pkg.PayloadLength)
		return DhtEnoProtocol
	}

	if len(pkg.Payload) != int(*pkg.PayloadLength) {
		log.Debugf("inboundHandshake: payload length mismatched, " +
			"inst: %s, local: %s, remote: %s, PayloadLength: %d, real: %d",
			conInst.name, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String(),
			*pkg.PayloadLength, len(pkg.Payload))
		return DhtEnoProtocol
	}

	dhtPkg := new(DhtPackage)
	dhtPkg.Pid = uint32(*pkg.Pid)
	dhtPkg.PayloadLength = *pkg.PayloadLength
	dhtPkg.Payload = pkg.Payload

	dhtMsg := new(DhtMessage)
	if eno := dhtPkg.GetMessage(dhtMsg); eno != DhtEnoNone {
		log.Debugf("inboundHandshake: GetMessage failed, " +
			"inst: %s, local: %s, remote: %s, eno: %d",
			conInst.name, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String(),
			eno)
		return eno
	}

	if dhtMsg.Mid != MID_HANDSHAKE {
		log.Debugf("inboundHandshake: invalid MID, " +
			"inst: %s, local: %s, remote: %s, mid: %d",
			conInst.name, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String(),
			dhtMsg.Mid)
		return DhtEnoProtocol
	}

	hs := dhtMsg.Handshake
	if hs.Dir != ConInstDirOutbound {
		log.Debugf("inboundHandshake: mismatched direction, " +
			"inst: %s, local: %s, remote: %s, dir: %d",
			conInst.name, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String(),
			hs.Dir)
		return DhtEnoProtocol
	}

	if hs.ChainId != conInst.chainId {
		log.Warnf("inboundHandshake: mismatched chain identity, " +
			"inst: %s, local: %s, remote: %s, id:[%d,%d]",
			conInst.name, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String(),
			conInst.chainId, hs.ChainId)
		return DhtEnoMismatched
	}

	conInst.hsInfo.peer = config.Node{
		IP:  hs.IP,
		TCP: uint16(hs.TCP & 0xffff),
		UDP: uint16(hs.UDP & 0xffff),
		ID:  hs.NodeId,
	}
	conInst.cid.nid = conInst.hsInfo.peer.ID

	*dhtMsg = DhtMessage{}
	dhtMsg.Mid = MID_HANDSHAKE
	dhtMsg.Handshake = &Handshake{
		ChainId:  conInst.chainId,
		Dir:      ConInstDirInbound,
		NodeId:   conInst.local.ID,
		IP:       conInst.local.IP,
		UDP:      uint32(conInst.local.UDP),
		TCP:      uint32(conInst.local.TCP),
		ProtoNum: 1,
		Protocols: []DhtProtocol{
			{
				Pid: uint32(PID_DHT),
				Ver: DhtVersion,
			},
		},
	}

	pbPkg := dhtMsg.GetPbPackage()
	if pbPkg == nil {
		log.Debugf("inboundHandshake: GetPbPackage failed, " +
			"inst: %s, local: %s, remote: %s",
			conInst.name, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String())
		return DhtEnoSerialization
	}

	conInst.con.SetDeadline(time.Now().Add(conInst.hsTimeout))
	if err := conInst.iow.WriteMsg(pbPkg); err != nil {
		log.Debugf("inboundHandshake: WriteMsg failed, " +
			"inst: %s, local: %s, remote: %s, error: %s",
			conInst.name, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String(),
			err.Error())
		return DhtEnoSerialization
	}

	log.Debugf("inboundHandshake: end ok, inst: %s, dir: %d, local: %s, remote: %s",
		conInst.name, conInst.dir, conInst.con.LocalAddr().String(), conInst.con.RemoteAddr().String())

	return DhtEnoNone
}

//
// Tx routine entry
//
func (conInst *ConInst) txProc() {

	// exception handler for debug
	defer func() {
		if err := recover(); err != nil {
			log.Debugf("txProc: inst: %s, dir: %d, exception raised, wait done...",
				conInst.name, conInst.dir)
		}
	}()

	errUnderlying := false
	isDone := false

	//
	// dtm scanner routine
	//
	ticker := time.NewTicker(ciTxDtmTick)
	go func() {
	_dtmScanLoop:
		for {
			select {
			case <-conInst.dtmDone:
				break _dtmScanLoop
			case <-ticker.C:
				conInst.txDtm.lock.Lock()
				conInst.txDtm.scan()
				conInst.txDtm.lock.Unlock()
			}
		}

		ticker.Stop()

		conInst.txDtm.lock.Lock()
		conInst.txDtm.reset()
		conInst.txWaitRsp = make(map[txPkgId]*conInstTxPkg, 0)
		conInst.txDtm.lock.Unlock()

		log.Debugf("txProc:dtm exit, sdl: %s, inst: %s, dir: %d",
			conInst.sdlName, conInst.name, conInst.dir)
	}()

	//
	// tx loop
	//
_txLoop:
	for {
		var (
			txPkg  *conInstTxPkg  = nil
			dhtPkg *DhtPackage    = nil
			pbPkg  *pb.DhtPackage = nil
		)

		inf, ok := <-conInst.txChan
		if !ok {
			goto _checkDone
		}

		txPkg = inf.(*conInstTxPkg)
		if dhtPkg, ok = txPkg.payload.(*DhtPackage); !ok {
			log.Errorf("txProc: mismatched type, sdl: %s, inst: %s, dir: %d",
				conInst.sdlName, conInst.name, conInst.dir)
			goto _checkDone
		}

		pbPkg = new(pb.DhtPackage)
		dhtPkg.ToPbPackage(pbPkg)
		txPkg.txTid = nil
		if txPkg.responsed != nil {
			if eno := conInst.txSetPending(txPkg); eno != DhtEnoNone {
				log.Debugf("txProc: txSetPending failed, sdl: %s, inst: %s, dir: %d, eno: %d",
					conInst.sdlName, conInst.name, conInst.dir, eno)
				goto _checkDone
			}
			if eno := conInst.txSetPkgTimer(txPkg); eno != DhtEnoNone {
				log.Debugf("txProc: txSetPending failed, sdl: %s, inst: %s, dir: %d, eno: %d",
					conInst.sdlName, conInst.name, conInst.dir, eno)
				goto _checkDone
			}
		}

		if err := conInst.iow.WriteMsg(pbPkg); err != nil {
			log.Debugf("txProc: WriteMsg failed, sdl: %s, inst: %s, dir: %d, err: %s",
				conInst.sdlName, conInst.name, conInst.dir, err.Error())
			errUnderlying = true
			break _txLoop
		}

		if conInst.txPkgCnt++; conInst.txPkgCnt & 0xff == 0 {
			log.Debugf("txProc: sdl: %s, inst: %s, dir: %d, txPkgCnt: %d",
				conInst.sdlName, conInst.name, conInst.dir, conInst.txPkgCnt)
		}

	_checkDone:
		select {
		case done := <-conInst.txDone:
			log.Debugf("txProc: sdl: %s, inst: %s, dir: %d, done by: %d",
				conInst.sdlName, conInst.name, conInst.dir, done)
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
		// the 1) case: report the status and then wait singal done
		//
		log.Debugf("txProc: underlying error, sdl: %s, inst: %s, dir: %d",
			conInst.sdlName, conInst.name, conInst.dir)

		if status := conInst.getStatus(); status < CisOutOfService {
			log.Debugf("txProc: CisOutOfService, sdl: %s, inst: %s, dir: %d, status: %d",
				conInst.sdlName, conInst.name, conInst.dir, status)
			conInst.updateStatus(CisOutOfService)
			if eno := conInst.statusReport(); eno != DhtEnoNone {
				log.Debugf("txProc: statusReport failed, sdl: %s, inst: %s, dir: %d, eno: %d",
					conInst.sdlName, conInst.name, conInst.dir, eno)
			}
		}

		log.Debugf("txProc: sdl: %s, inst: %s, dir: %d, try to get signal from txDone",
			conInst.sdlName, conInst.name, conInst.dir)
		<-conInst.txDone

		log.Debugf("txProc: sdl: %s, inst: %s, dir: %d, try to feedback signal to txDone",
			conInst.sdlName, conInst.name, conInst.dir)
		conInst.txDone <- DhtEnoNone.GetEno()

		log.Debugf("txProc: sdl: %s, inst: %s, dir: %d, done",
			conInst.sdlName, conInst.name, conInst.dir)

		return
	}

	if isDone == true {

		//
		// the 2) case: signal the done
		//
		log.Debugf("txProc: sdl: %s, inst: %s, dir: %d, done, feedback signal to txDone",
			conInst.sdlName, conInst.name, conInst.dir)

		conInst.txDone <- DhtEnoNone.GetEno()

		log.Debugf("txProc: sdl: %s, inst: %s, dir: %d, done",
			conInst.sdlName, conInst.name, conInst.dir)

		return
	}

	log.Debugf("txProc: wow! impossible errors, sdl: %s, inst: %s, dir: %d",
		conInst.sdlName, conInst.name, conInst.dir)
}

//
// Rx routine entry
//
func (conInst *ConInst) rxProc() {

	// exception handler for debug
	defer func() {
		if err := recover(); err != nil {
			log.Debugf("rxProc: sdl: %s, inst: %s, dir: %d, exception raised, wait done...",
				conInst.sdlName, conInst.name, conInst.dir)
		}
	}()

	//
	// longlong loop in a blocked mode
	//

	errUnderlying := false
	isDone := false

_rxLoop:

	for {

		var msg *DhtMessage = nil
		pbPkg := new(pb.DhtPackage)
		pkg := new(DhtPackage)

		if err := conInst.ior.ReadMsg(pbPkg); err != nil {
			log.Debugf("rxProc: ReadMsg failed, sdl: %s, inst: %s, dir: %d, err: %s",
				conInst.sdlName, conInst.name, conInst.dir, err.Error())
			errUnderlying = true
			break _rxLoop
		}

		if conInst.rxPkgCnt++; conInst.rxPkgCnt & 0xff == 0 {
			log.Debugf("rxProc: sdl: %s, inst: %s, dir: %d, rxPkgCnt: %d",
				conInst.sdlName, conInst.name, conInst.dir, conInst.rxPkgCnt)
		}

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
			log.Debugf("rxProc: sdl: %s, inst: %s, dir: %d, eno: %d, GetMessage failed",
				conInst.sdlName, conInst.name, conInst.dir, eno)
			goto _checkDone
		}

		if eno := conInst.dispatch(msg); eno != DhtEnoNone {
			log.Debugf("rxProc: sdl: %s, inst: %s, dir: %d, eno: %d, dispatch failed",
				conInst.sdlName, conInst.name, conInst.dir, eno)
		}

	_checkDone:

		select {
		case done := <-conInst.rxDone:
			isDone = true
			log.Debugf("rxProc: sdl: %s, inst: %s, dir: %d, done by: %d",
				conInst.sdlName, conInst.name, conInst.dir, done)
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
		log.Debugf("rxProc: sdl: %s, inst: %s, dir: %d, underlying error",
			conInst.sdlName, conInst.name, conInst.dir)

		if status := conInst.getStatus(); status < CisOutOfService {
			log.Debugf("rxProc: CisOutOfService, sdl: %s, inst: %s, dir: %d, status: %d",
				conInst.sdlName, conInst.name, conInst.dir, status)
			conInst.updateStatus(CisOutOfService)
			if eno := conInst.statusReport(); eno != DhtEnoNone {
				log.Debugf("rxProc: statusReport failed, sdl: %s, inst: %s, dir: %d, eno: %d",
					conInst.sdlName, conInst.name, conInst.dir, eno)
			}
		}

		log.Debugf("rxProc: get signal from rxDone, sdl: %s, inst: %s, dir: %d",
			conInst.sdlName, conInst.name, conInst.dir)
		<-conInst.rxDone

		log.Debugf("rxProc: feedback signal to rxDone, sdl: %s, inst: %s, dir: %d",
			conInst.sdlName, conInst.name, conInst.dir)
		conInst.rxDone <- DhtEnoNone.GetEno()

		log.Debugf("rxProc: sdl: %s, inst: %s, dir: %d, done",
			conInst.sdlName, conInst.name, conInst.dir)

		return
	}

	if isDone == true {

		//
		// the 2) case: signal the done
		//
		log.Debugf("rxProc: feedback signal to rxDone, inst: %s, dir: %d",
			conInst.name, conInst.dir)

		conInst.rxDone <- DhtEnoNone.GetEno()

		log.Debugf("rxProc: done, inst: %s, dir: %d", conInst.name, conInst.dir)

		return
	}

	log.Debugf("rxProc: wow! impossible errors, sdl: %s, inst: %s, dir: %d",
		conInst.sdlName, conInst.name, conInst.dir)
}

//
// messages dispatching
//
func (conInst *ConInst) dispatch(msg *DhtMessage) DhtErrno {

	log.Tracef("dispatch: try to dispatch message from peer, " +
		"inst: %s, local: %+v, msg: %+v", conInst.name, *conInst.local, *msg)

	var eno = DhtEnoUnknown

	switch msg.Mid {

	case MID_HANDSHAKE:

		log.Debugf("dispatch: MID_HANDSHAKE is not supported now")
		eno = DhtEnoProtocol

	case MID_FINDNODE:

		log.Debugf("dispatch: MID_FINDNODE, inst: %s", conInst.name)
		eno = conInst.findNode(msg.FindNode)

	case MID_NEIGHBORS:

		log.Debugf("dispatch: MID_NEIGHBORS, inst: %s", conInst.name)
		eno = conInst.neighbors(msg.Neighbors)

	case MID_PUTVALUE:

		if conInst.bootstrapNode {
			log.Debugf("dispatch: MID_PUTVALUE discarded, inst: %s", conInst.name)
			eno = DhtEnoBootstrapNode
			break
		}

		log.Debugf("dispatch: MID_PUTVALUE, inst: %s", conInst.name)
		eno = conInst.putValue(msg.PutValue)

	case MID_GETVALUE_REQ:

		if conInst.bootstrapNode {
			log.Debugf("dispatch: MID_GETVALUE_REQ discarded, inst: %s", conInst.name)
			eno = DhtEnoBootstrapNode
			break
		}

		log.Debugf("dispatch: MID_GETVALUE_REQ, inst: %s", conInst.name)
		eno = conInst.getValueReq(msg.GetValueReq)

	case MID_GETVALUE_RSP:

		if conInst.bootstrapNode {
			log.Debugf("dispatch: MID_GETVALUE_RSP discarded, inst: %s", conInst.name)
			eno = DhtEnoBootstrapNode
			break
		}

		log.Debugf("dispatch: MID_GETVALUE_RSP, inst: %s", conInst.name)
		eno = conInst.getValueRsp(msg.GetValueRsp)

	case MID_PUTPROVIDER:

		if conInst.bootstrapNode {
			log.Debugf("dispatch: MID_PUTPROVIDER discarded, inst: %s", conInst.name)
			eno = DhtEnoBootstrapNode
			break
		}

		log.Debugf("dispatch: MID_PUTPROVIDER, inst: %s", conInst.name)
		eno = conInst.putProvider(msg.PutProvider)

	case MID_GETPROVIDER_REQ:

		if conInst.bootstrapNode {
			log.Debugf("dispatch: MID_GETPROVIDER_REQ discarded, inst: %s", conInst.name)
			eno = DhtEnoBootstrapNode
			break
		}

		log.Debugf("dispatch: MID_GETPROVIDER_REQ, inst: %s", conInst.name)
		eno = conInst.getProviderReq(msg.GetProviderReq)

	case MID_GETPROVIDER_RSP:

		if conInst.bootstrapNode {
			log.Debugf("dispatch: MID_GETPROVIDER_RSP discarded, inst: %s", conInst.name)
			eno = DhtEnoBootstrapNode
			break
		}

		log.Debugf("dispatch: MID_GETPROVIDER_RSP, inst: %s", conInst.name)
		eno = conInst.getProviderRsp(msg.GetProviderRsp)

	case MID_PING:

		log.Debugf("dispatch: MID_PING, inst: %s", conInst.name)
		eno = conInst.getPing(msg.Ping)

	case MID_PONG:

		log.Debugf("dispatch: MID_PONG, inst: %s", conInst.name)
		eno = conInst.getPong(msg.Pong)

	default:

		log.Debugf("dispatch: unknown: %d, inst: %s", msg.Mid, conInst.name)
		eno = DhtEnoProtocol
	}

	return eno
}

//
// Handler for "MID_FINDNODE" from peer
//
func (conInst *ConInst) findNode(fn *FindNode) DhtErrno {
	req := sch.MsgDhtRutMgrNearestReq{
		Target:  fn.Target,
		Max:     rutMgrMaxNearest,
		NtfReq:  false,
		Task:    conInst.ptnMe,
		ForWhat: MID_FINDNODE,
		Msg:     fn,
	}
	msg := sch.SchMessage{}
	conInst.sdl.SchMakeMessage(&msg, conInst.ptnMe, conInst.ptnRutMgr, sch.EvDhtRutMgrNearestReq, &req)
	conInst.sdl.SchSendMessage(&msg)
	return DhtEnoNone
}

//
// Handler for "MID_NEIGHBORS" from peer
//
func (conInst *ConInst) neighbors(nbs *Neighbors) DhtErrno {
	ind := sch.MsgDhtQryInstProtoMsgInd{
		From:    &nbs.From,
		Msg:     nbs,
		ForWhat: sch.EvDhtConInstNeighbors,
	}
	msg := sch.SchMessage{}
	conInst.sdl.SchMakeMessage(&msg, conInst.ptnMe, conInst.ptnMe, sch.EvDhtQryInstProtoMsgInd, &ind)
	conInst.sdl.SchSendMessage(&msg)
	return DhtEnoNone
}

//
// Handler for "MID_PUTVALUE" from peer
//
func (conInst *ConInst) putValue(pv *PutValue) DhtErrno {
	req := sch.MsgDhtDsMgrPutValReq{
		ConInst: conInst,
		Msg:     pv,
	}
	msg := sch.SchMessage{}
	conInst.sdl.SchMakeMessage(&msg, conInst.ptnMe, conInst.ptnDsMgr, sch.EvDhtDsMgrPutValReq, &req)
	conInst.sdl.SchSendMessage(&msg)
	return DhtEnoNone
}

//
// Handler for "MID_GETVALUE_REQ" from peer
//
func (conInst *ConInst) getValueReq(gvr *GetValueReq) DhtErrno {
	req := sch.MsgDhtDsMgrGetValReq{
		ConInst: conInst,
		Msg:     gvr,
	}
	msg := sch.SchMessage{}
	conInst.sdl.SchMakeMessage(&msg, conInst.ptnMe, conInst.ptnDsMgr, sch.EvDhtDsMgrGetValReq, &req)
	conInst.sdl.SchSendMessage(&msg)
	return DhtEnoNone
}

//
// Handler for "MID_GETVALUE_RSP" from peer
//
func (conInst *ConInst) getValueRsp(gvr *GetValueRsp) DhtErrno {
	ind := sch.MsgDhtQryInstProtoMsgInd{
		From:    &gvr.From,
		Msg:     gvr,
		ForWhat: sch.EvDhtConInstGetValRsp,
	}
	msg := sch.SchMessage{}
	conInst.sdl.SchMakeMessage(&msg, conInst.ptnMe, conInst.ptnMe, sch.EvDhtQryInstProtoMsgInd, &ind)
	conInst.sdl.SchSendMessage(&msg)
	return DhtEnoNone
}

//
// Handler for "MID_PUTPROVIDER" from peer
//
func (conInst *ConInst) putProvider(pp *PutProvider) DhtErrno {
	req := sch.MsgDhtPrdMgrPutProviderReq{
		ConInst: conInst,
		Msg:     pp,
	}
	msg := sch.SchMessage{}
	conInst.sdl.SchMakeMessage(&msg, conInst.ptnMe, conInst.ptnPrdMgr, sch.EvDhtPrdMgrPutProviderReq, &req)
	conInst.sdl.SchSendMessage(&msg)
	return DhtEnoNone
}

//
// Handler for "MID_GETPROVIDER_REQ" from peer
//
func (conInst *ConInst) getProviderReq(gpr *GetProviderReq) DhtErrno {
	req := sch.MsgDhtPrdMgrGetProviderReq{
		ConInst: conInst,
		Msg:     gpr,
	}
	msg := sch.SchMessage{}
	conInst.sdl.SchMakeMessage(&msg, conInst.ptnMe, conInst.ptnPrdMgr, sch.EvDhtPrdMgrGetProviderReq, &req)
	conInst.sdl.SchSendMessage(&msg)
	return DhtEnoNone
}

//
// Handler for "MID_GETPROVIDER_RSP" from peer
//
func (conInst *ConInst) getProviderRsp(gpr *GetProviderRsp) DhtErrno {
	ind := sch.MsgDhtQryInstProtoMsgInd{
		From:    &gpr.From,
		Msg:     gpr,
		ForWhat: sch.EvDhtConInstGetProviderRsp,
	}
	msg := sch.SchMessage{}
	conInst.sdl.SchMakeMessage(&msg, conInst.ptnMe, conInst.ptnMe, sch.EvDhtQryInstProtoMsgInd, &ind)
	conInst.sdl.SchSendMessage(&msg)
	return DhtEnoNone
}

//
// Handler for "MID_PING" from peer
//
func (conInst *ConInst) getPing(ping *Ping) DhtErrno {
	pingInd := sch.MsgDhtRutPingInd{
		ConInst: conInst,
		Msg:     ping,
	}
	msg := sch.SchMessage{}
	conInst.sdl.SchMakeMessage(&msg, conInst.ptnMe, conInst.ptnRutMgr, sch.EvDhtRutPingInd, &pingInd)
	conInst.sdl.SchSendMessage(&msg)
	return DhtEnoNone
}

//
// Handler for "MID_PONG" from peer
//
func (conInst *ConInst) getPong(pong *Pong) DhtErrno {
	pongInd := sch.MsgDhtRutPingInd{
		ConInst: conInst,
		Msg:     pong,
	}
	msg := sch.SchMessage{}
	conInst.sdl.SchMakeMessage(&msg, conInst.ptnMe, conInst.ptnRutMgr, sch.EvDhtRutPongInd, &pongInd)
	conInst.sdl.SchSendMessage(&msg)
	return DhtEnoNone
}

//
// Check if pending packages sent is responsed by peeer
//
func (conInst *ConInst) checkTxWaitResponse(mid int, seq int64) (DhtErrno, *conInstTxPkg) {
	conInst.txDtm.lock.Lock()
	defer conInst.txDtm.lock.Unlock()

	pkgId := txPkgId{txSeq: seq}
	txPkg, ok := conInst.txWaitRsp[pkgId]
	if !ok {
		log.Debugf("checkTxWaitResponse: not found, mid: %d, seq: %d", mid, seq)
		return DhtEnoNotFound, nil
	}
	if txPkg.responsed != nil {
		txPkg.responsed <- true
		close(txPkg.responsed)
	}

	log.Debugf("checkTxWaitResponse: it's found, mid: %d, seq: %d", mid, seq)
	if txPkg.responsed != nil && txPkg.txTid != nil {
		conInst.txDtm.delTimer(txPkg.txTid)
		txPkg.txTid = nil
	}

	eno, ptn := conInst.sdl.SchGetUserTaskNode(txPkg.taskName)
	if eno == sch.SchEnoNone && ptn != nil && ptn == txPkg.task {
		if txPkg.task != nil {
			ind := sch.MsgDhtConInstTxInd{
				Eno:     DhtEnoTimeout.GetEno(),
				WaitMid: txPkg.waitMid,
				WaitSeq: txPkg.waitSeq,
			}
			schMsg := sch.SchMessage{}
			conInst.sdl.SchMakeMessage(&schMsg, conInst.ptnMe, txPkg.task, sch.EvDhtConInstTxInd, &ind)
			conInst.sdl.SchSendMessage(&schMsg)
		}
	}

	delete(conInst.txWaitRsp, pkgId)
	return DhtEnoNone, txPkg
}

//
// Install callback for rx data with protocol identity PID_EXT
//
func (conInst *ConInst) InstallRxDataCallback(cbf ConInstRxDataCallback) DhtErrno {
	conInst.cbRxLock.Lock()
	defer conInst.cbRxLock.Unlock()
	if conInst.cbfRxData != nil {
		log.Debugf("InstallRxDataCallback: old callback will be overlapped")
	}
	if cbf == nil {
		log.Debugf("InstallRxDataCallback: nil callback will be set")
	}
	conInst.cbfRxData = cbf
	return DhtEnoNone
}

//
// Get scheduler
//
func (conInst *ConInst) GetScheduler() *sch.Scheduler {
	return conInst.sdl
}

//
// Update instance status
//
func (conInst *ConInst) updateStatus(status conInstStatus) {
	conInst.lock.Lock()
	defer conInst.lock.Unlock()
	conInst.status = status
}

//
// Get instance status
//
func (conInst *ConInst) getStatus() conInstStatus {
	conInst.lock.Lock()
	defer conInst.lock.Unlock()
	return conInst.status
}

//
// DTM(Difference Timer Manager)
//
type DiffTimerCallback func(interface{}) error

type DiffTimerManager struct {
	lock sync.Mutex
	tick time.Duration
	tmq  *list.List
	sum  int
	cbf  DiffTimerCallback
}

type DiffTimer struct {
	ud interface{}
	tv int
}

func NewDiffTimerManager(tick time.Duration, cbf DiffTimerCallback) *DiffTimerManager {
	return &DiffTimerManager{
		tick: tick,
		tmq:  list.New(),
		sum:  0,
		cbf:  cbf,
	}
}

func (dtm *DiffTimerManager) setCallback(cbf DiffTimerCallback) {
	dtm.cbf = cbf
}

func (dtm *DiffTimerManager) dur2Ticks(d time.Duration) (tv int, err error) {
	if tv = int(d/dtm.tick); tv == 0 {
		return 0, errors.New("dur2Ticks: too small tv")
	}
	return tv, nil
}

func (dtm *DiffTimerManager) setTimer(ud interface{}, tv int) (tid interface{}, err error) {
	tid = nil
	err = nil
	tm := DiffTimer{ud: ud, tv: 0}
	if dtm.tmq == nil {
		err = errors.New("setTimer: tmq not init")
	} else if tv >= dtm.sum {
		tm.tv = tv - dtm.sum
		dtm.sum = tv
		tid = dtm.tmq.PushBack(&tm)
	} else {
		sum := 0
		el := dtm.tmq.Front()
		for {
			old := el.Value.(*DiffTimer)
			if sum+old.tv >= tv {
				tm.tv = tv - sum
				old.tv = sum + old.tv - tv
				tid = dtm.tmq.InsertBefore(&tm, el)
				break
			}
			el = el.Next()
		}
		if tid == nil {
			log.Debugf("setTimer: impossible nil timer")
			err = errors.New("setTimer: impossible nil timer")
		}
	}
	return tid, err
}

func (dtm *DiffTimerManager) delTimer(tid interface{}) error {
	dtm.tmq.Remove(tid.(*list.Element))
	return nil
}

func (dtm *DiffTimerManager) scan() error {
	if dtm.tmq == nil || dtm.tmq.Len() <= 0 {
		return nil
	}
	for dtm.tmq.Len() > 0 {
		dtm.sum--
		dtm.tmq.Front().Value.(*DiffTimer).tv--
		if dtm.tmq.Front().Value.(*DiffTimer).tv > 0 {
			break
		} else {
			tm := dtm.tmq.Remove(dtm.tmq.Front()).(*DiffTimer)
			if dtm.cbf != nil {
				dtm.cbf(tm.ud)
			}
		}
	}
	return nil
}

func (dtm *DiffTimerManager) size() int {
	return dtm.tmq.Len()
}

func (dtm *DiffTimerManager) reset() {
	dtm.tmq = nil
	dtm.sum = 0
	dtm.cbf = nil
}
