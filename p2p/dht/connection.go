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
	"container/list"
	log "github.com/yeeco/gyee/p2p/logger"
	sch	"github.com/yeeco/gyee/p2p/scheduler"
	config "github.com/yeeco/gyee/p2p/config"
)

//
// Connection manager name registered in scheduler
//
const ConMgrName = sch.DhtConMgrName

//
// Peer connection status
//
type conMgrPeerConnStat int

const (
	pcsConnNo	= iota			// not connected
	pcsConnYes					// connected in service
)

//
// Connection manager configuration
//
type conMgrCfg struct {
	maxCon		int								// max number of connection
	hsTimeout	time.Duration					// handshake timeout duration
}

//
// Connection manager
//
type ConMgr struct {
	sdl			*sch.Scheduler					// pointer to scheduler
	name		string							// my name
	cfg			conMgrCfg						// configuration
	tep			sch.SchUserTaskEp				// task entry
	ptnMe		interface{}						// pointer to task node of myself
	ptnRutMgr	interface{}						// pointer to route manager task node
	ptnQryMgr	interface{}						// pointer to query manager task node
	ptnLsnMgr	interface{}						// pointer to the listner manager task node
	ciTab		map[conInstIdentity]*ConInst	// connection instance table
	ciSeq		int64							// connection instance sequence number
	txQueTab	map[conInstIdentity]*list.List	// outbound data pending queue
}

//
// Create route manager
//
func NewConMgr() *ConMgr {

	conMgr := ConMgr{
		sdl:		nil,
		name:		ConMgrName,
		tep:		nil,
		ptnMe:		nil,
		ptnRutMgr:	nil,
		ptnQryMgr:	nil,
		ciTab:		make(map[conInstIdentity]*ConInst, 0),
		ciSeq:		0,
		txQueTab:	make(map[conInstIdentity]*list.List, 0),
	}

	conMgr.tep = conMgr.conMgrProc

	return &conMgr
}

//
// Entry point exported to shceduler
//
func (conMgr *ConMgr)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return conMgr.tep(ptn, msg)
}

//
// Connection manager entry
//
func (conMgr *ConMgr)conMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	if ptn == nil || msg == nil {
		log.LogCallerFileLine("conMgrProc: " +
			"invalid parameters, ptn: %p, msg: %p",
			ptn, msg)
		return sch.SchEnoParameter
	}

	eno := sch.SchEnoUnknown

	switch msg.Id {

	case sch.EvSchPoweron:
		eno = conMgr.poweron(ptn)

	case sch.EvSchPoweroff:
		eno = conMgr.poweroff(ptn)

	case sch.EvDhtConMgrConnectReq:
		eno = conMgr.connctReq(msg.Body.(*sch.MsgDhtConMgrConnectReq))

	case sch.EvDhtConMgrCloseReq:
		eno = conMgr.closeReq(msg.Body.(*sch.MsgDhtConMgrCloseReq))

	case sch.EvDhtLsnMgrAcceptInd:
		eno = conMgr.acceptInd(msg.Body.(*sch.MsgDhtLsnMgrAcceptInd))

	case sch.EvDhtConInstHandshakeRsp:
		eno = conMgr.handshakeRsp(msg.Body.(*sch.MsgDhtConInstHandshakeRsp))

	case sch.EvDhtLsnMgrStatusInd:
		eno = conMgr.lsnMgrStatusInd(msg.Body.(*sch.MsgDhtLsnMgrStatusInd))

	case sch.EvDhtConMgrSendReq:
		eno = conMgr.sendReq(msg.Body.(*sch.MsgDhtConMgrSendReq))

	case sch.EvDhtConInstStatusInd:
		eno = conMgr.instStatusInd(msg.Body.(*sch.MsgDhtConInstStatusInd))

	case sch.EvDhtConInstCloseRsp:
		eno = conMgr.instCloseRsp(msg.Body.(*sch.MsgDhtConInstCloseRsp))

	case sch.EvDhtRutPeerRemovedInd:
		eno = conMgr.rutPeerRemoveInd(msg.Body.(*sch.MsgDhtRutPeerRemovedInd))

	default:
		log.LogCallerFileLine("conMgrProc: unknown event: %d", msg.Id)
		eno = sch.SchEnoParameter
	}

	return eno
}

//
// Poweron handler
//
func (conMgr *ConMgr)poweron(ptn interface{}) sch.SchErrno {

	sdl := sch.SchGetScheduler(ptn)

	conMgr.ptnMe = ptn
	_, conMgr.ptnRutMgr = sdl.SchGetTaskNodeByName(RutMgrName)
	_, conMgr.ptnQryMgr = sdl.SchGetTaskNodeByName(QryMgrName)
	_, conMgr.ptnLsnMgr = sdl.SchGetTaskNodeByName(LsnMgrName)

	if conMgr.ptnRutMgr == nil || conMgr.ptnQryMgr == nil || conMgr.ptnLsnMgr == nil {
		log.LogCallerFileLine("poweron: internal errors")
		return sch.SchEnoInternal
	}

	if dhtEno := conMgr.getConfig(); dhtEno != DhtEnoNone {
		log.LogCallerFileLine("poweron: getConfig failed, dhtEno: %d", dhtEno)
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

//
// Poweroff handler
//
func (conMgr *ConMgr)poweroff(ptn interface{}) sch.SchErrno {

	log.LogCallerFileLine("poweroff: task will be done ...")

	po := sch.SchMessage{}
	for _, ci := range conMgr.ciTab {
		conMgr.sdl.SchMakeMessage(&po, conMgr.ptnMe, ci.ptnMe, sch.EvSchPoweroff, nil)
		conMgr.sdl.SchSendMessage(&po)
	}

	conMgr.sdl.SchTaskDone(conMgr.ptnMe, sch.SchEnoKilled)

	return sch.SchEnoNone
}

//
// Inbound connection accepted indication handler
//
func (conMgr *ConMgr)acceptInd(msg *sch.MsgDhtLsnMgrAcceptInd) sch.SchErrno {

	//
	// at this moment, we do not know any peer information, since it just only the
	// inbound connection accepted. we can check the peer only after the handshake
	// procedure is completed.
	//

	sdl := conMgr.sdl
	ci := newConInst(fmt.Sprintf("%d", conMgr.ciSeq))
	conMgr.setupOutboundInst(ci, conMgr.ptnLsnMgr, nil)
	conMgr.ciSeq++

	td := sch.SchTaskDescription{
		Name:		ci.name,
		MbSize:		-1,
		Ep:			ci,
		Wd:			&sch.SchWatchDog{HaveDog:false,},
		Flag:		sch.SchCreatedGo,
		DieCb:		nil,
		UserDa:		nil,
	}

	eno, ptn := conMgr.sdl.SchCreateTask(&td)
	if eno != sch.SchEnoNone || ptn == nil {
		log.LogCallerFileLine("acceptInd: SchCreateTask failed, eno: %d", eno)
		return eno
	}

	ci.ptnMe = ptn
	po := sch.SchMessage{}
	sdl.SchMakeMessage(&po, conMgr.ptnMe, ci.ptnMe, sch.EvSchPoweron, nil)
	sdl.SchSendMessage(&po)

	hs := sch.SchMessage{}
	hsreq := sch.MsgDhtConInstHandshakeReq {
		DurHs: conMgr.cfg.hsTimeout,
	}
	sdl.SchMakeMessage(&hs, conMgr.ptnMe, ci.ptnMe, sch.EvDhtConInstHandshakeReq, &hsreq)
	sdl.SchSendMessage(&hs)

	return sch.SchEnoNone
}

//
// Handshake response handler
//
func (conMgr *ConMgr)handshakeRsp(msg *sch.MsgDhtConInstHandshakeRsp) sch.SchErrno {

	//
	// for inbound connection instances, we still not map them into "conMgr.ciTab",
	// we need to do this if handshake-ok reported, since we can obtain the peer
	// information in this case here.
	//

	cid := conInstIdentity {
		nid:	msg.Peer.ID,
		dir:	conInstDir(msg.Dir),
	}

	var ci *ConInst = nil

	if msg.Eno != DhtEnoNone {

		//
		// notice: if handshake failed, connection instances should have done themself,
		// so here we need not to request them to be closed, we just remove them from
		// the map table(for outbounds).
		//

		if msg.Dir == conInstDirOutbound {
			delete(conMgr.ciTab, cid)
		}

		cid := conInstIdentity{
			nid: msg.Peer.ID,
			dir: conInstDirOutbound,
		}

		if li, ok := conMgr.txQueTab[cid]; ok {
			for li.Len() > 0 {
				li.Remove(li.Front())
			}
			delete(conMgr.txQueTab, cid)
		}

		return sch.SchEnoNone
	}

	if msg.Dir == conInstDirInbound {

		ci = msg.Inst.(*ConInst)
		conMgr.ciTab[cid] = ci

	} else {

		if ci = conMgr.lookupOutboundConInst(&msg.Peer.ID); ci == nil {
			log.LogCallerFileLine("handshakeRsp: not found, id: %x", msg.Peer.ID)
			return sch.SchEnoUserTask
		}

		//
		// submit the pending package for outbound instance if any
		//

		cid := conInstIdentity{
			nid: msg.Peer.ID,
			dir: conInstDirOutbound,
		}

		if li, ok := conMgr.txQueTab[cid]; ok {

			for li.Len() > 0 {

				el := li.Front()
				tx := el.Value.(*sch.MsgDhtConMgrSendReq)

				pkg := conInstTxPkg {
					task:		tx.Task,
					submitTime:	time.Now(),
					payload:	tx.Data,
				}

				ci.txPutPending(&pkg)
			}

			delete(conMgr.txQueTab, cid)
		}
	}

	//
	// we need to update the route manager
	//

	update := sch.MsgDhtRutMgrUpdateReq {
		Seens:	[]config.Node {
			*msg.Peer,
		},
		Duras:	[]time.Duration {
			msg.Dur,
		},
	}

	schMsg := sch.SchMessage{}
	conMgr.sdl.SchMakeMessage(&schMsg, conMgr.ptnMe, conMgr.ptnRutMgr, sch.EvDhtRutMgrUpdateReq, &update)
	conMgr.sdl.SchSendMessage(&schMsg)

	return sch.SchEnoNone
}

//
// Listener manager status indication handler
//
func (conMgr *ConMgr)lsnMgrStatusInd(msg *sch.MsgDhtLsnMgrStatusInd) sch.SchErrno {
	log.LogCallerFileLine("lsnMgrStatusInd: listener manager status reported: %d", msg.Status)
	if msg.Status == lmsNull || msg.Status == lmsStopped {
		schMsg := sch.SchMessage{}
		conMgr.sdl.SchMakeMessage(&schMsg, conMgr.ptnMe, conMgr.ptnLsnMgr, sch.EvDhtLsnMgrStartReq, nil)
		return conMgr.sdl.SchSendMessage(&schMsg)
	}
	return sch.SchEnoNone
}

//
// Connect-request handler
//
func (conMgr *ConMgr)connctReq(msg *sch.MsgDhtConMgrConnectReq) sch.SchErrno {

	var rsp = sch.MsgDhtConMgrConnectRsp {
		Peer:	msg.Peer,
		Eno:	DhtEnoNone,
	}

	var sender = msg.Task
	var sdl = conMgr.sdl

	var rsp2Sender = func(eno DhtErrno) sch.SchErrno {
		msg := sch.SchMessage{}
		rsp.Eno = int(eno)
		sdl.SchMakeMessage(&msg, conMgr.ptnMe, sender, sch.EvDhtConMgrConnectRsp, &rsp)
		return sdl.SchSendMessage(&msg)
	}

	if conMgr.lookupOutboundConInst(&msg.Peer.ID) != nil {
		log.LogCallerFileLine("connctReq: outbound instance duplicated, id: %x", msg.Peer.ID)
		return rsp2Sender(DhtErrno(DhtEnoDuplicated))
	}

	ci := newConInst(fmt.Sprintf("%d", conMgr.ciSeq))
	conMgr.setupOutboundInst(ci, msg.Task, msg.Peer)
	conMgr.ciSeq++

	td := sch.SchTaskDescription{
		Name:		ci.name,
		MbSize:		-1,
		Ep:			ci,
		Wd:			&sch.SchWatchDog{HaveDog:false,},
		Flag:		sch.SchCreatedGo,
		DieCb:		nil,
		UserDa:		nil,
	}

	eno, ptn := conMgr.sdl.SchCreateTask(&td)
	if eno != sch.SchEnoNone || ptn == nil {
		log.LogCallerFileLine("")
		return rsp2Sender(DhtErrno(DhtEnoScheduler))
	}

	ci.ptnMe = ptn
	po := sch.SchMessage{}
	sdl.SchMakeMessage(&po, conMgr.ptnMe, ci.ptnMe, sch.EvSchPoweron, nil)
	sdl.SchSendMessage(&po)

	hs := sch.SchMessage{}
	hsreq := sch.MsgDhtConInstHandshakeReq {
		DurHs: conMgr.cfg.hsTimeout,
	}
	sdl.SchMakeMessage(&hs, conMgr.ptnMe, ci.ptnMe, sch.EvDhtConInstHandshakeReq, &hsreq)
	sdl.SchSendMessage(&hs)

	cid := conInstIdentity{
		nid:	msg.Peer.ID,
		dir:	conInstDirOutbound,
	}
	conMgr.ciTab[cid] = ci

	return rsp2Sender(DhtErrno(DhtEnoNone))
}

//
// Close-instance-request handler
//
func (conMgr *ConMgr)closeReq(msg *sch.MsgDhtConMgrCloseReq) sch.SchErrno {

	cid := conInstIdentity {
		nid:	msg.Peer.ID,
		dir:	conInstDir(msg.Dir),
	}

	schMsg := sch.SchMessage{}
	sender := msg.Task
	sdl := conMgr.sdl

	rsp2Sender := func(eno DhtErrno) sch.SchErrno{
		rsp := sch.MsgDhtConMgrCloseRsp{
			Eno:	int(eno),
			Peer:	msg.Peer,
			Dir:	msg.Dir,
		}
		sdl.SchMakeMessage(&schMsg, conMgr.ptnMe, sender, sch.EvDhtConMgrCloseRsp, &rsp)
		return sdl.SchSendMessage(&schMsg)
	}

	req2Inst := func(inst *ConInst) sch.SchErrno {
		req := sch.MsgDhtConInstCloseReq{
			Peer:	&msg.Peer.ID,
			Why:	sch.EvDhtConMgrCloseReq,
		}
		sdl.SchMakeMessage(&schMsg, conMgr.ptnMe, inst.ptnMe, sch.EvDhtConInstCloseReq, &req)
		return sdl.SchSendMessage(&schMsg)
	}

	found := false
	err := false

	cis := conMgr.lookupConInst(&cid)
	for _, ci := range cis {
		if ci != nil {
			found = true
			if req2Inst(ci) != sch.SchEnoNone {
				err = true
			}
		}
	}

	if !found {
		return rsp2Sender(DhtEnoNotFound)
	}

	if err {
		return rsp2Sender(DhtEnoScheduler)
	}

	return sch.SchEnoNone
}

//
// Send-data-request handler
//
func (conMgr *ConMgr)sendReq(msg *sch.MsgDhtConMgrSendReq) sch.SchErrno {

	//
	// notice: when requested to send, the connection instance might not be exist,
	// we need to establesh the connection and backup the data if it is the case;
	//
	// notice: it must be a outbound instance to bear the data requested to be sent,
	// even we have an instance of inbound with the same node identity, we do not
	// send on it.
	//

	ci := conMgr.lookupOutboundConInst(&msg.Peer.ID)

	if ci != nil {

		pkg := conInstTxPkg {
			task:		msg.Task,
			submitTime:	time.Now(),
			payload:	msg.Data,
		}

		if eno := ci.txPutPending(&pkg); eno != DhtEnoNone {
			log.LogCallerFileLine("sendReq: txPutPending failed, eno: %d", eno)
			return sch.SchEnoUserTask
		}

		return sch.SchEnoNone
	}

	schMsg := sch.SchMessage{}
	req := sch.MsgDhtConMgrConnectReq {
		Task:	msg.Task,
		Peer:	msg.Peer,
	}
	conMgr.sdl.SchMakeMessage(&schMsg, msg.Task, conMgr.ptnMe, sch.EvDhtConMgrConnectReq, &req)
	conMgr.sdl.SchSendMessage(&schMsg)

	cid := conInstIdentity {
		nid: msg.Peer.ID,
		dir: conInstDirOutbound,
	}

	li, ok := conMgr.txQueTab[cid]
	if ok {
		li.PushBack(msg)
	} else {
		li = list.New()
		conMgr.txQueTab[cid] = li
		li.PushBack(msg)
	}

	return sch.SchEnoNone
}

//
// Instance status indication handler
//
func (conMgr *ConMgr)instStatusInd(msg *sch.MsgDhtConInstStatusInd) sch.SchErrno {

	log.LogCallerFileLine("instStatusInd: " +
		"instance status reported: %d, id: %x",
		msg.Status, msg.Peer)

	switch msg.Status {
	case cisNull:
	case cisConnected:
	case cisInHandshaking:
	case cisHandshaked:
	case cisInService:
	case cisClosed:
	default:
	}

	return sch.SchEnoNone
}

//
// Close-instance-request handler
//
func (conMgr *ConMgr)instCloseRsp(msg *sch.MsgDhtConInstCloseRsp) sch.SchErrno {

	cid := conInstIdentity {
		nid:	*msg.Peer,
		dir:	conInstDir(msg.Dir),
	}

	schMsg := sch.SchMessage{}
	sdl := conMgr.sdl

	rsp2Sender := func(eno DhtErrno, peer *config.Node, task interface{}) sch.SchErrno{
		rsp := sch.MsgDhtConMgrCloseRsp{
			Eno:	int(eno),
			Peer:	peer,
			Dir:	msg.Dir,
		}
		sdl.SchMakeMessage(&schMsg, conMgr.ptnMe, task, sch.EvDhtConMgrCloseRsp, &rsp)
		return sdl.SchSendMessage(&schMsg)
	}

	found := false
	err := false

	cis := conMgr.lookupConInst(&cid)
	for _, ci := range cis {
		if ci != nil {
			found = true
			if rsp2Sender(DhtEnoNone, &ci.hsInfo.peer, ci.ptnSrcTsk) != sch.SchEnoNone {
				err = true
			} else {
				delete(conMgr.ciTab, cid)
			}
		}
	}

	if !found {
		log.LogCallerFileLine("instCloseRsp: not found, id: %x", msg.Peer)
	}

	if err {
		log.LogCallerFileLine("instCloseRsp: seems some errors")
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

//
// Peer removed from route indication handler
//
func (conMgr *ConMgr)rutPeerRemoveInd(msg *sch.MsgDhtRutPeerRemovedInd) sch.SchErrno {

	//
	// when peer is removed from route table, we close all bound connection
	// instances if any.
	//

	cid := conInstIdentity {
		nid:	msg.Peer,
		dir:	conInstDirAllbound,
	}

	schMsg := sch.SchMessage{}
	sdl := conMgr.sdl

	req2Inst := func(inst *ConInst) sch.SchErrno {
		req := sch.MsgDhtConInstCloseReq{
			Peer:	&msg.Peer,
			Why:	sch.EvDhtRutPeerRemovedInd,
		}
		sdl.SchMakeMessage(&schMsg, conMgr.ptnMe, inst.ptnMe, sch.EvDhtConInstCloseReq, &req)
		return sdl.SchSendMessage(&schMsg)
	}

	found := false
	err := false

	cis := conMgr.lookupConInst(&cid)
	for _, ci := range cis {
		if ci != nil {
			found = true
			if req2Inst(ci) != sch.SchEnoNone {
				err = true
			}
		}
	}

	if !found {
		log.LogCallerFileLine("rutPeerRemoveInd: not found, id: %x", msg.Peer)
	}

	if err {
		log.LogCallerFileLine("rutPeerRemoveInd: seems some errors")
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

//
// Get configuration for connection mananger
//
func (conMgr *ConMgr)getConfig() DhtErrno {
	cfg := config.P2pConfig4DhtConManager(conMgr.sdl.SchGetP2pCfgName())
	conMgr.cfg.maxCon = cfg.MaxCon
	conMgr.cfg.hsTimeout = cfg.HsTimeout
	return DhtEnoNone
}

//
// Lookup connection instance by instance identity
//
func (conMgr *ConMgr)lookupConInst(cid *conInstIdentity) []*ConInst {

	if cid == nil {
		return []*ConInst{nil}
	}

	if cid.dir == conInstDirInbound || cid.dir == conInstDirOutbound {

		return []*ConInst{conMgr.ciTab[*cid]}

	} else if cid.dir == conInstDirAllbound {

		inCid := *cid
		inCid.dir = conInstDirInbound
		outCid := *cid
		outCid.dir = conInstDirOutbound

		return []*ConInst {
			conMgr.ciTab[inCid],
			conMgr.ciTab[outCid],
		}
	}

	return []*ConInst{nil}
}

//
// Lookup outbound connection instance by node identity
//
func (conMgr *ConMgr)lookupOutboundConInst(nid *config.NodeID) *ConInst {
	ci := &conInstIdentity {
		nid:	*nid,
		dir:	conInstDirOutbound,
	}
	return conMgr.lookupConInst(ci)[0]
}

//
// Lookup outbound connection instance by node identity
//
func (conMgr *ConMgr)lookupInboundConInst(nid *config.NodeID) *ConInst {
	ci := &conInstIdentity {
		nid:	*nid,
		dir:	conInstDirInbound,
	}
	return conMgr.lookupConInst(ci)[0]
}

//
// Setup outbound connection instance
//
func (conMgr *ConMgr)setupOutboundInst(ci *ConInst, srcTask interface{}, peer *config.Node) DhtErrno {

	ci.sdl = conMgr.sdl
	ci.ptnConMgr = conMgr.ptnMe
	ci.ptnSrcTsk = srcTask

	if peer != nil {
		ci.hsInfo.peer = *peer
		ci.dir = conInstDirOutbound
	} else {
		ci.dir = conInstDirInbound
	}

	ci.cid = conInstIdentity{
		nid: peer.ID,
		dir: conInstDirInbound,
	}

	return DhtEnoNone
}
